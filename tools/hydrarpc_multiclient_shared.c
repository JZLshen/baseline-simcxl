#define _GNU_SOURCE

#include <cpuid.h>
#include <errno.h>
#include <gem5/m5ops.h>
#include <inttypes.h>
#include <linux/mempolicy.h>
#include <sched.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <x86intrin.h>

#define CACHELINE_SIZE 64u
#define MAX_TOURNAMENT_DEPTH 64u
#define DEFAULT_REQUEST_PAYLOAD_SIZE 64u
#define DEFAULT_RESPONSE_PAYLOAD_SIZE 64u

#define NOTIFY_LEN_BITS 14u
#define NOTIFY_CLIENT_BITS 5u
#define NOTIFY_OFFSET_BITS (64u - 1u - NOTIFY_CLIENT_BITS - NOTIFY_LEN_BITS)
#define NOTIFY_LEN_MAX (1u << NOTIFY_LEN_BITS)
#define NOTIFY_OFFSET_MASK ((1ull << NOTIFY_OFFSET_BITS) - 1ull)
#define NOTIFY_LEN_MASK ((1ull << NOTIFY_LEN_BITS) - 1ull)
#define NOTIFY_CLIENT_MASK ((1ull << NOTIFY_CLIENT_BITS) - 1ull)
#define NOTIFY_LEN_SHIFT NOTIFY_OFFSET_BITS
#define NOTIFY_CLIENT_SHIFT (NOTIFY_LEN_SHIFT + NOTIFY_LEN_BITS)
#define NOTIFY_VALID_SHIFT (NOTIFY_CLIENT_SHIFT + NOTIFY_CLIENT_BITS)
#define NOTIFY_VALID_MASK (1ull << NOTIFY_VALID_SHIFT)

struct NotifyQueueLine {
    volatile uint64_t word;
    uint8_t padding[CACHELINE_SIZE - sizeof(uint64_t)];
} __attribute__((aligned(CACHELINE_SIZE)));

struct PetersonNode {
    volatile uint64_t flag[2];
    volatile uint64_t victim;
    uint8_t padding[CACHELINE_SIZE - (3 * sizeof(uint64_t))];
} __attribute__((aligned(CACHELINE_SIZE)));

struct RequestPublishState {
    volatile uint64_t produce_seq;
    volatile uint64_t data_tail;
    uint8_t padding[CACHELINE_SIZE - (2 * sizeof(uint64_t))];
} __attribute__((aligned(CACHELINE_SIZE)));

struct Connection {
    volatile struct NotifyQueueLine *request_queue;
    volatile struct NotifyQueueLine *response_queue;
    uint8_t *request_data_area;
    uint8_t *response_data_area;
    struct PetersonNode *request_lock_nodes;
    struct PetersonNode *response_lock_nodes;
    volatile struct RequestPublishState *request_publish_state;
    volatile uint64_t *response_consume_seq;
    uint64_t lock_leaf_count;
    size_t request_data_size;
    size_t response_data_size;
};

struct SharedControl {
    volatile uint64_t ready_count;
    volatile uint64_t start_flag;
    volatile int rc;
};

struct RequestTimingData {
    uint64_t *client_req_start_ts_ns;
    uint64_t *client_resp_done_ts_ns;
    uint64_t *server_req_observe_ts_ns;
    uint64_t *server_exec_done_ts_ns;
    uint64_t *server_resp_done_ts_ns;
    uint64_t *server_loop_start_ts_ns;
};

enum SendMode {
    SEND_GREEDY = 0,
    SEND_UNIFORM,
    SEND_STAGGERED,
    SEND_UNEVEN,
};

_Static_assert(sizeof(struct NotifyQueueLine) == CACHELINE_SIZE,
               "NotifyQueueLine must occupy exactly one cacheline");
_Static_assert(sizeof(struct PetersonNode) == CACHELINE_SIZE,
               "PetersonNode must occupy exactly one cacheline");
_Static_assert(sizeof(struct RequestPublishState) == CACHELINE_SIZE,
               "RequestPublishState must occupy exactly one cacheline");

static inline uint64_t
round_up_u64(uint64_t value, uint64_t align)
{
    return ((value + align - 1u) / align) * align;
}

static inline uint64_t
payload_span_u64(uint64_t payload_len)
{
    return round_up_u64(payload_len, CACHELINE_SIZE);
}

static uint64_t
next_pow2_u64(uint64_t value)
{
    uint64_t result = 1;

    while (result < value)
        result <<= 1;

    return result;
}

static uint64_t
page_size_u64(void)
{
    long page_size = sysconf(_SC_PAGESIZE);

    if (page_size <= 0)
        return 4096u;

    return (uint64_t)page_size;
}

static inline uint64_t
m5_rpns_ns(void)
{
    return m5_rpns();
}

static inline void
clflushopt_line(const void *ptr)
{
    asm volatile("clflushopt (%0)" : : "r"(ptr) : "memory");
}

static int
cpu_has_clflushopt(void)
{
    unsigned int eax = 0;
    unsigned int ebx = 0;
    unsigned int ecx = 0;
    unsigned int edx = 0;

    if (!__get_cpuid_count(7, 0, &eax, &ebx, &ecx, &edx))
        return 0;

    return ((ebx >> 23) & 0x1u) ? 1 : 0;
}

static int
pin_to_cpu(int cpu_id)
{
    cpu_set_t cpu_set;

    CPU_ZERO(&cpu_set);
    CPU_SET(cpu_id, &cpu_set);
    return sched_setaffinity(0, sizeof(cpu_set), &cpu_set);
}

static uint64_t
parse_u64(const char *name, const char *value)
{
    char *end = NULL;
    unsigned long long parsed = strtoull(value, &end, 10);

    if (!value[0] || (end && *end != '\0')) {
        fprintf(stderr, "invalid %s: %s\n", name, value);
        exit(2);
    }

    return (uint64_t)parsed;
}

static const char *
send_mode_string(enum SendMode mode)
{
    switch (mode) {
      case SEND_GREEDY:
        return "greedy";
      case SEND_UNIFORM:
        return "uniform";
      case SEND_STAGGERED:
        return "staggered";
      case SEND_UNEVEN:
        return "uneven";
      default:
        return "unknown";
    }
}

static enum SendMode
parse_send_mode(const char *value)
{
    if (strcmp(value, "greedy") == 0)
        return SEND_GREEDY;
    if (strcmp(value, "uniform") == 0)
        return SEND_UNIFORM;
    if (strcmp(value, "staggered") == 0)
        return SEND_STAGGERED;
    if (strcmp(value, "uneven") == 0)
        return SEND_UNEVEN;

    fprintf(stderr, "invalid send-mode: %s\n", value);
    exit(2);
}

static size_t
shared_mapping_size(size_t size)
{
    return (size_t)round_up_u64((uint64_t)size, page_size_u64());
}

static void *
alloc_shared_mapping(size_t size)
{
    void *ptr = mmap(NULL,
                     shared_mapping_size(size),
                     PROT_READ | PROT_WRITE,
                     MAP_SHARED | MAP_ANONYMOUS,
                     -1,
                     0);

    if (ptr == MAP_FAILED) {
        fprintf(stderr, "shared allocation failed for %zu bytes: %s\n",
                size,
                strerror(errno));
        exit(1);
    }

    return ptr;
}

static void *
alloc_shared_zeroed(size_t size)
{
    void *ptr = alloc_shared_mapping(size);

    memset(ptr, 0, shared_mapping_size(size));
    return ptr;
}

static void
bind_mapping_to_node_or_die(void *ptr, size_t size, int node)
{
    unsigned long nodemask = 0;
    long rc = 0;

    if (node < 0 || node >= (int)(sizeof(nodemask) * 8u)) {
        fprintf(stderr, "invalid numa node: %d\n", node);
        exit(2);
    }

    nodemask = 1ul << node;
    rc = syscall(SYS_mbind,
                 ptr,
                 shared_mapping_size(size),
                 MPOL_BIND,
                 &nodemask,
                 sizeof(nodemask) * 8u,
                 MPOL_MF_MOVE | MPOL_MF_STRICT);
    if (rc != 0) {
        fprintf(stderr, "mbind to node %d failed: %s\n", node, strerror(errno));
        exit(1);
    }
}

static void *
alloc_shared_cxl(size_t size, int cxl_node)
{
    void *ptr = alloc_shared_mapping(size);

    bind_mapping_to_node_or_die(ptr, size, cxl_node);
    return ptr;
}

static inline uint64_t
notify_make(uint64_t offset, uint64_t client_id, uint64_t len)
{
    return NOTIFY_VALID_MASK |
           ((client_id & NOTIFY_CLIENT_MASK) << NOTIFY_CLIENT_SHIFT) |
           (((len - 1u) & NOTIFY_LEN_MASK) << NOTIFY_LEN_SHIFT) |
           (offset & NOTIFY_OFFSET_MASK);
}

static inline int
notify_valid(uint64_t word)
{
    return (word & NOTIFY_VALID_MASK) != 0;
}

static inline uint64_t
notify_client_id(uint64_t word)
{
    return (word >> NOTIFY_CLIENT_SHIFT) & NOTIFY_CLIENT_MASK;
}

static inline uint64_t
notify_len(uint64_t word)
{
    return ((word >> NOTIFY_LEN_SHIFT) & NOTIFY_LEN_MASK) + 1u;
}

static inline uint64_t
notify_offset(uint64_t word)
{
    return word & NOTIFY_OFFSET_MASK;
}

static inline volatile struct NotifyQueueLine *
notify_queue_line_at(volatile struct NotifyQueueLine *queue, uint64_t slot)
{
    return &queue[slot];
}

static inline uint8_t *
payload_at_offset(uint8_t *area, uint64_t offset)
{
    return area + offset;
}

__attribute__((noinline)) static void
copy_bytes_to_remote(void *dst, const void *src, size_t len)
{
    memcpy(dst, src, len);
}

__attribute__((noinline)) static void
copy_bytes_from_remote(void *dst, const void *src, size_t len)
{
    volatile const uint8_t *src_bytes =
        (volatile const uint8_t *)(const void *)src;
    uint8_t *dst_bytes = (uint8_t *)dst;

    for (size_t i = 0; i < len; i++)
        dst_bytes[i] = src_bytes[i];
}

static inline void
flush_remote_range(const void *ptr, uint64_t len)
{
    uintptr_t begin = (uintptr_t)ptr & ~((uintptr_t)CACHELINE_SIZE - 1u);
    uintptr_t end = round_up_u64((uint64_t)((uintptr_t)ptr + len),
                                 CACHELINE_SIZE);

    for (uintptr_t cursor = begin; cursor < end; cursor += CACHELINE_SIZE)
        clflushopt_line((const void *)cursor);
}

static inline uint64_t
read_remote_u64(volatile uint64_t *ptr)
{
    clflushopt_line((const void *)ptr);
    _mm_mfence();
    return __atomic_load_n((const uint64_t *)ptr, __ATOMIC_RELAXED);
}

static inline void
write_remote_u64(volatile uint64_t *ptr, uint64_t value)
{
    __atomic_store_n((uint64_t *)ptr, value, __ATOMIC_RELAXED);
    clflushopt_line((const void *)ptr);
    _mm_sfence();
}

static inline void
store_payload_remote(void *dst, const void *src, uint64_t payload_len)
{
    copy_bytes_to_remote(dst, src, payload_len);
    flush_remote_range(dst, payload_len);
    _mm_sfence();
}

static inline void
load_payload_remote(void *dst, const void *src, uint64_t payload_len)
{
    flush_remote_range(src, payload_len);
    _mm_mfence();
    copy_bytes_from_remote(dst, src, payload_len);
}

static inline uint64_t
load_notify_word_remote(volatile struct NotifyQueueLine *queue_line)
{
    clflushopt_line((const void *)queue_line);
    _mm_mfence();
    return __atomic_load_n((const uint64_t *)&queue_line->word,
                           __ATOMIC_RELAXED);
}

static inline void
write_notify_word_remote(volatile struct NotifyQueueLine *queue_line,
                         uint64_t notify_word)
{
    __atomic_store_n((uint64_t *)&queue_line->word, notify_word, __ATOMIC_RELAXED);
    clflushopt_line((const void *)queue_line);
    _mm_sfence();
}

static inline void
clear_notify_word_remote(volatile struct NotifyQueueLine *queue_line)
{
    __atomic_store_n((uint64_t *)&queue_line->word, 0, __ATOMIC_RELAXED);
    clflushopt_line((const void *)queue_line);
    _mm_sfence();
}

static inline void
load_request_publish_state_remote(struct RequestPublishState *dst,
                                  volatile struct RequestPublishState *src)
{
    clflushopt_line((const void *)src);
    _mm_mfence();
    copy_bytes_from_remote(dst, (const void *)src, sizeof(*dst));
}

static inline void
store_request_publish_state_remote(volatile struct RequestPublishState *dst,
                                   const struct RequestPublishState *src)
{
    copy_bytes_to_remote((void *)dst, src, sizeof(*src));
    clflushopt_line((const void *)dst);
    _mm_sfence();
}

static void
init_notify_ring(volatile struct NotifyQueueLine *queue, uint64_t slot_count)
{
    for (uint64_t slot = 0; slot < slot_count; slot++)
        clear_notify_word_remote(notify_queue_line_at(queue, slot));
}

static void
init_peterson_nodes(struct PetersonNode *nodes, uint64_t node_count)
{
    struct PetersonNode zero_node = {0};

    for (uint64_t node_id = 0; node_id < node_count; node_id++) {
        copy_bytes_to_remote(&nodes[node_id], &zero_node, sizeof(zero_node));
        clflushopt_line((const void *)&nodes[node_id]);
        _mm_sfence();
    }
}

static inline size_t
request_result_index(uint64_t client_id, uint64_t req_id, uint64_t request_count)
{
    return (size_t)(client_id * request_count + req_id);
}

static inline int
client_is_slow(uint64_t client_id, uint64_t slow_client_count)
{
    return client_id < slow_client_count;
}

static inline uint64_t
client_request_limit(uint64_t client_id,
                     uint64_t request_count,
                     uint64_t slow_client_count,
                     uint64_t slow_request_count)
{
    if (client_is_slow(client_id, slow_client_count))
        return slow_request_count;

    return request_count;
}

static uint64_t
total_request_limit(uint64_t client_count,
                    uint64_t request_count,
                    uint64_t slow_client_count,
                    uint64_t slow_request_count)
{
    uint64_t total = 0;

    for (uint64_t client_id = 0; client_id < client_count; client_id++) {
        total += client_request_limit(client_id,
                                      request_count,
                                      slow_client_count,
                                      slow_request_count);
    }

    return total;
}

static inline int
send_is_due(enum SendMode send_mode,
            uint64_t send_gap_ns,
            uint64_t client_count,
            uint64_t client_id,
            uint64_t sent,
            uint64_t schedule_base_ns)
{
    uint64_t deadline_ns = schedule_base_ns;
    uint64_t effective_gap_ns = send_gap_ns;

    if (send_mode == SEND_GREEDY || send_gap_ns == 0)
        return 1;

    if (send_mode == SEND_STAGGERED)
        deadline_ns += client_id * send_gap_ns;

    if (send_mode == SEND_UNEVEN) {
        uint64_t weight_num = 2 * (client_count - client_id);
        uint64_t weight_den = client_count + 1;

        effective_gap_ns = (uint64_t)((((__uint128_t)send_gap_ns * weight_den) +
                                       weight_num - 1) /
                                      weight_num);
    }

    deadline_ns += sent * effective_gap_ns;
    return m5_rpns_ns() >= deadline_ns;
}

static inline uint64_t
min_u64(uint64_t lhs, uint64_t rhs)
{
    return lhs < rhs ? lhs : rhs;
}

static uint64_t
next_rand(uint64_t *state)
{
    *state = (*state * 6364136223846793005ull) + 1442695040888963407ull;
    return *state;
}

static void
wait_for_start(volatile uint64_t *start_flag)
{
    while (__atomic_load_n((const uint64_t *)start_flag, __ATOMIC_ACQUIRE) ==
           0) {
    }
}

static void
set_control_error(struct SharedControl *control)
{
    __atomic_store_n(&control->rc, 1, __ATOMIC_RELAXED);
}

static int
control_has_error(struct SharedControl *control)
{
    return __atomic_load_n(&control->rc, __ATOMIC_RELAXED);
}

static inline void
store_shared_u64(uint64_t *ptr, uint64_t value)
{
    __atomic_store_n(ptr, value, __ATOMIC_RELAXED);
}

static inline uint8_t *
request_shadow_at(uint8_t *buffer, uint64_t seq, uint64_t request_payload_size)
{
    return buffer + (size_t)(seq * request_payload_size);
}

static void
fill_random_payload_bytes(uint8_t *dst, uint64_t payload_len, uint64_t *rng_state)
{
    uint64_t offset = 0;

    while (offset < payload_len) {
        uint64_t word = next_rand(rng_state);
        uint64_t chunk_len = min_u64(payload_len - offset, sizeof(word));

        memcpy(dst + offset, &word, (size_t)chunk_len);
        offset += chunk_len;
    }
}

static void
build_response_payload_bytes(uint8_t *dst,
                             uint64_t response_payload_size,
                             const uint8_t *request_bytes,
                             uint64_t request_payload_size)
{
    uint64_t copy_len = min_u64(response_payload_size, request_payload_size);

    memcpy(dst, request_bytes, (size_t)copy_len);
    if (response_payload_size > copy_len) {
        memset(dst + copy_len,
               0,
               (size_t)(response_payload_size - copy_len));
    }
}

static int
response_matches_request(const uint8_t *response_bytes,
                         uint64_t response_payload_size,
                         const uint8_t *request_bytes,
                         uint64_t request_payload_size)
{
    uint64_t copy_len = min_u64(response_payload_size, request_payload_size);

    if (memcmp(response_bytes, request_bytes, (size_t)copy_len) != 0)
        return 0;

    for (uint64_t i = copy_len; i < response_payload_size; i++) {
        if (response_bytes[i] != 0)
            return 0;
    }

    return 1;
}

static void
signal_ready_and_wait(struct SharedControl *control)
{
    __atomic_fetch_add(&control->ready_count, 1, __ATOMIC_RELEASE);
    wait_for_start(&control->start_flag);
}

static inline struct PetersonNode *
peterson_node_at(struct PetersonNode *nodes, uint64_t tree_index)
{
    return &nodes[tree_index - 1];
}

static void
peterson_publish_remote(struct PetersonNode *node, uint64_t side)
{
    node->flag[side] = 1;
    node->victim = side;
    clflushopt_line((const void *)node);
    _mm_sfence();
}

static void
peterson_clear_remote(struct PetersonNode *node, uint64_t side)
{
    node->flag[side] = 0;
    clflushopt_line((const void *)node);
    _mm_sfence();
}

static void
peterson_load_remote(struct PetersonNode *dst, const struct PetersonNode *src)
{
    clflushopt_line((const void *)src);
    _mm_mfence();
    copy_bytes_from_remote(dst, src, sizeof(*dst));
}

static uint64_t
tournament_lock_acquire(struct PetersonNode *nodes,
                        uint64_t leaf_count,
                        uint64_t client_id,
                        uint64_t *path_nodes,
                        uint8_t *path_sides)
{
    uint64_t idx = 0;
    uint64_t depth = 0;

    if (nodes == NULL || leaf_count == 0)
        return 0;

    idx = leaf_count + client_id;
    while (idx > 1) {
        uint64_t parent = idx >> 1;
        uint64_t side = (idx & 1u) ? 1u : 0u;
        uint64_t other_side = side ^ 1u;
        struct PetersonNode *node = peterson_node_at(nodes, parent);
        struct PetersonNode snapshot;

        path_nodes[depth] = parent;
        path_sides[depth] = (uint8_t)side;
        depth++;

        peterson_publish_remote(node, side);

        for (;;) {
            peterson_load_remote(&snapshot, node);
            if (snapshot.flag[other_side] == 0 || snapshot.victim != side)
                break;
        }

        idx = parent;
    }

    return depth;
}

static void
tournament_lock_release(struct PetersonNode *nodes,
                        const uint64_t *path_nodes,
                        const uint8_t *path_sides,
                        uint64_t depth)
{
    while (depth > 0) {
        uint64_t parent = path_nodes[depth - 1];
        uint64_t side = path_sides[depth - 1];
        struct PetersonNode *node = peterson_node_at(nodes, parent);

        peterson_clear_remote(node, side);
        depth--;
    }
}

static int
run_server_shared(struct Connection *connection,
                  uint64_t client_count,
                  uint64_t slot_count,
                  uint64_t request_count,
                  uint64_t slow_client_count,
                  uint64_t slow_request_count,
                  uint64_t total_request_target,
                  uint64_t request_payload_size,
                  uint64_t response_payload_size,
                  struct RequestTimingData *timing,
                  int server_cpu,
                  struct SharedControl *control)
{
    uint64_t request_consume_seq = 0;
    uint64_t response_produce_seq = 0;
    uint64_t response_data_tail = 0;
    uint64_t completed_total = 0;
    uint64_t *next_req_seq_per_client =
        calloc((size_t)client_count, sizeof(*next_req_seq_per_client));
    uint8_t *request_payload_local = malloc((size_t)request_payload_size);
    uint8_t *response_staging = malloc((size_t)response_payload_size);

    if (next_req_seq_per_client == NULL ||
        request_payload_local == NULL ||
        response_staging == NULL) {
        fprintf(stderr, "server tracking allocation failed\n");
        set_control_error(control);
        free(next_req_seq_per_client);
        free(request_payload_local);
        free(response_staging);
        return 1;
    }

    if (pin_to_cpu(server_cpu) != 0) {
        fprintf(stderr, "failed to pin server to cpu %d\n", server_cpu);
        set_control_error(control);
        free(next_req_seq_per_client);
        free(request_payload_local);
        free(response_staging);
        return 1;
    }

    signal_ready_and_wait(control);
    store_shared_u64(timing->server_loop_start_ts_ns, m5_rpns_ns());

    while (completed_total < total_request_target &&
           !control_has_error(control)) {
        uint64_t request_slot = request_consume_seq % slot_count;
        volatile struct NotifyQueueLine *request_queue_line =
            notify_queue_line_at(connection->request_queue, request_slot);
        uint64_t request_notify = load_notify_word_remote(request_queue_line);

        if (!notify_valid(request_notify))
            continue;

        {
            uint64_t request_client_id = notify_client_id(request_notify);
            uint64_t request_req_id = next_req_seq_per_client[request_client_id];
            uint64_t client_request_count = client_request_limit(
                request_client_id,
                request_count,
                slow_client_count,
                slow_request_count
            );
            uint64_t request_len = notify_len(request_notify);
            uint64_t request_offset = notify_offset(request_notify);
            uint64_t response_offset = response_data_tail;
            uint64_t response_notify = 0;
            uint64_t response_slot = 0;
            uint64_t server_req_observe_ts = 0;
            uint64_t server_exec_done_ts = 0;
            uint64_t server_resp_done_ts = 0;
            size_t index = 0;

            if (request_req_id >= client_request_count) {
                fprintf(stderr, "server observed unexpected request ordinal\n");
                set_control_error(control);
                break;
            }
            index = request_result_index(request_client_id,
                                         request_req_id,
                                         request_count);

            if (request_len != request_payload_size) {
                fprintf(stderr, "server observed unexpected request length\n");
                set_control_error(control);
                break;
            }

            server_req_observe_ts = m5_rpns_ns();
            load_payload_remote(request_payload_local,
                                payload_at_offset(connection->request_data_area,
                                                  request_offset),
                                request_len);
            server_exec_done_ts = m5_rpns_ns();
            clear_notify_word_remote(request_queue_line);

            build_response_payload_bytes(response_staging,
                                         response_payload_size,
                                         request_payload_local,
                                         request_payload_size);
            store_payload_remote(payload_at_offset(connection->response_data_area,
                                                   response_offset),
                                 response_staging,
                                 response_payload_size);
            response_data_tail += payload_span_u64(response_payload_size);

            response_notify =
                notify_make(response_offset,
                            request_client_id,
                            response_payload_size);

            for (;;) {
                response_slot = response_produce_seq % slot_count;
                {
                    volatile struct NotifyQueueLine *response_queue_line =
                        notify_queue_line_at(connection->response_queue,
                                             response_slot);

                    if (!notify_valid(load_notify_word_remote(response_queue_line))) {
                        write_notify_word_remote(response_queue_line, response_notify);
                        response_produce_seq++;
                        server_resp_done_ts = m5_rpns_ns();
                        break;
                    }
                }

                if (control_has_error(control))
                    break;
            }
            if (control_has_error(control))
                break;

            store_shared_u64(&timing->server_req_observe_ts_ns[index],
                             server_req_observe_ts);
            store_shared_u64(&timing->server_exec_done_ts_ns[index],
                             server_exec_done_ts);
            store_shared_u64(&timing->server_resp_done_ts_ns[index],
                             server_resp_done_ts);
            next_req_seq_per_client[request_client_id]++;
            request_consume_seq++;
            completed_total++;
        }
    }

    free(next_req_seq_per_client);
    free(request_payload_local);
    free(response_staging);
    return control_has_error(control) ? 1 : 0;
}

static int
run_client_shared(struct Connection *connection,
                  uint64_t client_count,
                  uint64_t client_id,
                  uint64_t window_size,
                  uint64_t slot_count,
                  uint64_t request_count,
                  uint64_t slow_client_count,
                  uint64_t slow_request_count,
                  uint64_t request_payload_size,
                  uint64_t response_payload_size,
                  enum SendMode send_mode,
                  uint64_t send_gap_ns,
                  uint64_t slow_send_gap_ns,
                  struct RequestTimingData *timing,
                  struct SharedControl *control)
{
    size_t base = (size_t)(client_id * request_count);
    uint64_t client_request_count = client_request_limit(client_id,
                                                         request_count,
                                                         slow_client_count,
                                                         slow_request_count);
    uint64_t rng_state = client_id + 1;
    uint64_t sent = 0;
    uint64_t completed = 0;
    uint64_t pending_start = 0;
    uint64_t send_schedule_base_ns = 0;
    uint64_t active_window_size = 1;
    uint64_t steady_schedule_started = 0;
    enum SendMode effective_send_mode = send_mode;
    uint64_t effective_send_gap_ns = send_gap_ns;
    uint8_t *request_shadow_local = calloc((size_t)client_request_count,
                                           (size_t)request_payload_size);
    uint8_t *response_local = malloc((size_t)response_payload_size);
    int pending_valid = 0;
    uint64_t request_lock_path_nodes[MAX_TOURNAMENT_DEPTH];
    uint8_t request_lock_path_sides[MAX_TOURNAMENT_DEPTH];
    uint64_t response_lock_path_nodes[MAX_TOURNAMENT_DEPTH];
    uint8_t response_lock_path_sides[MAX_TOURNAMENT_DEPTH];

    if (request_shadow_local == NULL || response_local == NULL) {
        fprintf(stderr,
                "client %" PRIu64 " local tracking allocation failed\n",
                client_id);
        free(request_shadow_local);
        free(response_local);
        set_control_error(control);
        return 1;
    }

    if (pin_to_cpu((int)client_id) != 0) {
        fprintf(stderr, "failed to pin client %" PRIu64 " to cpu %" PRIu64 "\n",
                client_id,
                client_id);
        set_control_error(control);
        free(request_shadow_local);
        free(response_local);
        return 1;
    }

    if (client_is_slow(client_id, slow_client_count)) {
        effective_send_mode = SEND_UNIFORM;
        effective_send_gap_ns = slow_send_gap_ns;
    }

    signal_ready_and_wait(control);
    send_schedule_base_ns = m5_rpns_ns();

    while (completed < client_request_count && !control_has_error(control)) {
        active_window_size = (completed == 0) ? 1 : window_size;

        if (completed > 0 && !steady_schedule_started) {
            send_schedule_base_ns = m5_rpns_ns();
            steady_schedule_started = 1;
        }

        if (!pending_valid &&
            sent < client_request_count &&
            (sent - completed) < active_window_size &&
            send_is_due(effective_send_mode,
                        effective_send_gap_ns,
                        client_count,
                        client_id,
                        sent,
                        send_schedule_base_ns)) {
            uint8_t *request_shadow =
                request_shadow_at(request_shadow_local, sent, request_payload_size);

            pending_start = m5_rpns_ns();
            fill_random_payload_bytes(request_shadow, request_payload_size, &rng_state);
            pending_valid = 1;
        }

        if (pending_valid && !control_has_error(control)) {
            uint64_t lock_depth = tournament_lock_acquire(connection->request_lock_nodes,
                                                          connection->lock_leaf_count,
                                                          client_id,
                                                          request_lock_path_nodes,
                                                          request_lock_path_sides);

            if (!control_has_error(control)) {
                struct RequestPublishState publish_state;
                uint64_t request_seq = 0;
                uint64_t request_slot = request_seq % slot_count;
                volatile struct NotifyQueueLine *request_queue_line =
                    notify_queue_line_at(connection->request_queue, request_slot);

                load_request_publish_state_remote(&publish_state,
                                                  connection->request_publish_state);
                request_seq = publish_state.produce_seq;
                request_slot = request_seq % slot_count;
                request_queue_line =
                    notify_queue_line_at(connection->request_queue, request_slot);

                if (!notify_valid(load_notify_word_remote(request_queue_line))) {
                    uint64_t request_offset = publish_state.data_tail;
                    size_t index = base + sent;
                    uint8_t *request_shadow =
                        request_shadow_at(request_shadow_local,
                                          sent,
                                          request_payload_size);

                    store_shared_u64(&timing->client_req_start_ts_ns[index],
                                     pending_start);
                    store_payload_remote(
                        payload_at_offset(connection->request_data_area,
                                          request_offset),
                        request_shadow,
                        request_payload_size
                    );
                    write_notify_word_remote(
                        request_queue_line,
                        notify_make(request_offset,
                                    client_id,
                                    request_payload_size)
                    );
                    publish_state.data_tail =
                        request_offset + payload_span_u64(request_payload_size);
                    publish_state.produce_seq = request_seq + 1;
                    store_request_publish_state_remote(
                        connection->request_publish_state,
                        &publish_state
                    );

                    pending_valid = 0;
                    sent++;
                }
            }

            tournament_lock_release(connection->request_lock_nodes,
                                    request_lock_path_nodes,
                                    request_lock_path_sides,
                                    lock_depth);
        }

        if (completed < sent && !control_has_error(control)) {
            uint64_t lock_depth = tournament_lock_acquire(connection->response_lock_nodes,
                                                          connection->lock_leaf_count,
                                                          client_id,
                                                          response_lock_path_nodes,
                                                          response_lock_path_sides);

            if (!control_has_error(control)) {
                uint64_t response_seq = read_remote_u64(connection->response_consume_seq);
                uint64_t response_slot = response_seq % slot_count;
                volatile struct NotifyQueueLine *response_queue_line =
                    notify_queue_line_at(connection->response_queue, response_slot);
                uint64_t response_notify = load_notify_word_remote(response_queue_line);

                if (notify_valid(response_notify) &&
                    notify_client_id(response_notify) == client_id) {
                    uint64_t response_len = notify_len(response_notify);
                    uint64_t response_offset = notify_offset(response_notify);

                    if (response_len != response_payload_size) {
                        fprintf(stderr,
                                "client %" PRIu64 " observed unexpected response length\n",
                                client_id);
                        set_control_error(control);
                    } else {
                        uint8_t *request_shadow =
                            request_shadow_at(request_shadow_local,
                                              completed,
                                              request_payload_size);
                        uint64_t end = 0;
                        int fail = 0;

                        load_payload_remote(response_local,
                                            payload_at_offset(connection->response_data_area,
                                                              response_offset),
                                            response_len);
                        fail = !response_matches_request(response_local,
                                                         response_payload_size,
                                                         request_shadow,
                                                         request_payload_size);
                        end = m5_rpns_ns();

                        store_shared_u64(
                            &timing->client_resp_done_ts_ns[base + completed],
                            end
                        );

                        if (fail) {
                            fprintf(stderr,
                                    "client response payload mismatch client=%" PRIu64
                                    " req=%" PRIu64 "\n",
                                    client_id,
                                    completed);
                            set_control_error(control);
                        } else {
                            clear_notify_word_remote(response_queue_line);
                            write_remote_u64(connection->response_consume_seq,
                                             response_seq + 1);
                            completed++;
                        }
                    }
                }
            }

            tournament_lock_release(connection->response_lock_nodes,
                                    response_lock_path_nodes,
                                    response_lock_path_sides,
                                    lock_depth);
        }

    }

    free(request_shadow_local);
    free(response_local);
    return control_has_error(control) ? 1 : 0;
}

static void
kill_children(pid_t *child_pids, uint64_t child_count)
{
    for (uint64_t i = 0; i < child_count; i++) {
        if (child_pids[i] > 0)
            kill(child_pids[i], SIGKILL);
    }
}

static void
mark_child_reaped(pid_t *child_pids, uint64_t child_count, pid_t pid)
{
    for (uint64_t i = 0; i < child_count; i++) {
        if (child_pids[i] == pid) {
            child_pids[i] = 0;
            return;
        }
    }
}

static void
write_results(FILE *stream,
              uint64_t client_count,
              uint64_t request_count,
              uint64_t slow_client_count,
              uint64_t slow_request_count,
              struct RequestTimingData *timing)
{
    fprintf(stream,
            "server_loop_start_ts_ns=%" PRIu64 "\n",
            *timing->server_loop_start_ts_ns);
    for (uint64_t client_id = 0; client_id < client_count; client_id++) {
        uint64_t client_request_count = client_request_limit(client_id,
                                                             request_count,
                                                             slow_client_count,
                                                             slow_request_count);

        for (uint64_t req_id = 0; req_id < client_request_count; req_id++) {
            size_t index = (size_t)(client_id * request_count + req_id);

            fprintf(stream,
                    "client_%" PRIu64 "_req_%" PRIu64
                    "_client_req_start_ts_ns=%" PRIu64 "\n",
                    client_id,
                    req_id,
                    timing->client_req_start_ts_ns[index]);
            fprintf(stream,
                    "client_%" PRIu64 "_req_%" PRIu64
                    "_client_resp_done_ts_ns=%" PRIu64 "\n",
                    client_id,
                    req_id,
                    timing->client_resp_done_ts_ns[index]);
            fprintf(stream,
                    "client_%" PRIu64 "_req_%" PRIu64
                    "_server_req_observe_ts_ns=%" PRIu64 "\n",
                    client_id,
                    req_id,
                    timing->server_req_observe_ts_ns[index]);
            fprintf(stream,
                    "client_%" PRIu64 "_req_%" PRIu64
                    "_server_exec_done_ts_ns=%" PRIu64 "\n",
                    client_id,
                    req_id,
                    timing->server_exec_done_ts_ns[index]);
            fprintf(stream,
                    "client_%" PRIu64 "_req_%" PRIu64
                    "_server_resp_done_ts_ns=%" PRIu64 "\n",
                    client_id,
                    req_id,
                    timing->server_resp_done_ts_ns[index]);
        }
    }
}

static void
print_results_with_rc(FILE *stream,
                      int rc,
                      uint64_t client_count,
                      uint64_t request_count,
                      uint64_t slow_client_count,
                      uint64_t slow_request_count,
                      struct RequestTimingData *timing)
{
    fprintf(stream, "benchmark_rc=%d\n", rc);
    write_results(stream,
                  client_count,
                  request_count,
                  slow_client_count,
                  slow_request_count,
                  timing);
}

int
main(int argc, char **argv)
{
    uint64_t client_count = 1;
    uint64_t request_count = 30;
    uint64_t window_size = 16;
    uint64_t slot_count = 0;
    uint64_t slow_client_count = 0;
    uint64_t slow_request_count = 0;
    uint64_t request_payload_size = DEFAULT_REQUEST_PAYLOAD_SIZE;
    uint64_t response_payload_size = DEFAULT_RESPONSE_PAYLOAD_SIZE;
    int cxl_node = 1;
    int server_cpu = -1;
    uint64_t send_gap_ns = 0;
    uint64_t slow_send_gap_ns = 0;
    enum SendMode send_mode = SEND_GREEDY;
    long online_cpus = sysconf(_SC_NPROCESSORS_ONLN);
    uint64_t total_requests = 0;
    uint64_t total_request_target = 0;
    uint64_t request_span = 0;
    uint64_t response_span = 0;
    size_t request_queue_size = 0;
    size_t response_queue_size = 0;
    size_t request_data_size = 0;
    size_t response_data_size = 0;
    size_t request_lock_nodes_size = 0;
    size_t response_lock_nodes_size = 0;
    size_t request_publish_state_size = 0;
    size_t response_consume_seq_size = 0;
    struct Connection connection = {0};
    struct SharedControl *control = NULL;
    struct RequestTimingData timing = {0};
    pid_t *child_pids = NULL;
    uint64_t created_children = 0;
    int rc = 0;

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--client-count") == 0 && i + 1 < argc) {
            client_count = parse_u64("client-count", argv[++i]);
        } else if (strcmp(argv[i], "--count-per-client") == 0 &&
                   i + 1 < argc) {
            request_count = parse_u64("count-per-client", argv[++i]);
        } else if (strcmp(argv[i], "--window-size") == 0 && i + 1 < argc) {
            window_size = parse_u64("window-size", argv[++i]);
        } else if (strcmp(argv[i], "--slot-count") == 0 && i + 1 < argc) {
            slot_count = parse_u64("slot-count", argv[++i]);
        } else if (strcmp(argv[i], "--req-bytes") == 0 && i + 1 < argc) {
            request_payload_size = parse_u64("req-bytes", argv[++i]);
        } else if (strcmp(argv[i], "--resp-bytes") == 0 && i + 1 < argc) {
            response_payload_size = parse_u64("resp-bytes", argv[++i]);
        } else if (strcmp(argv[i], "--slow-client-count") == 0 &&
                   i + 1 < argc) {
            slow_client_count = parse_u64("slow-client-count", argv[++i]);
        } else if (strcmp(argv[i], "--slow-count-per-client") == 0 &&
                   i + 1 < argc) {
            slow_request_count = parse_u64("slow-count-per-client", argv[++i]);
        } else if (strcmp(argv[i], "--cxl-node") == 0 && i + 1 < argc) {
            cxl_node = (int)parse_u64("cxl-node", argv[++i]);
        } else if (strcmp(argv[i], "--server-cpu") == 0 && i + 1 < argc) {
            server_cpu = (int)parse_u64("server-cpu", argv[++i]);
        } else if (strcmp(argv[i], "--send-mode") == 0 && i + 1 < argc) {
            send_mode = parse_send_mode(argv[++i]);
        } else if (strcmp(argv[i], "--send-gap-ns") == 0 && i + 1 < argc) {
            send_gap_ns = parse_u64("send-gap-ns", argv[++i]);
        } else if (strcmp(argv[i], "--slow-send-gap-ns") == 0 &&
                   i + 1 < argc) {
            slow_send_gap_ns = parse_u64("slow-send-gap-ns", argv[++i]);
        } else if (strcmp(argv[i], "--help") == 0 ||
                   strcmp(argv[i], "-h") == 0) {
            printf("Usage: %s [--client-count N] [--count-per-client N] "
                   "[--window-size N] [--slot-count N] "
                   "[--req-bytes N] [--resp-bytes N] "
                   "[--slow-client-count N] [--slow-count-per-client N] "
                   "[--slow-send-gap-ns N] [--cxl-node N] "
                   "[--send-mode greedy|uniform|staggered|uneven] "
                   "[--send-gap-ns N] [--server-cpu N]\n"
                   "  slot-count is shared request/response notify-ring depth; "
                   "default is client-count * min(window-size, count-per-client)\n"
                   "  req-bytes/resp-bytes set the request/response payload sizes; "
                   "defaults are %" PRIu64 "/%" PRIu64 " bytes\n"
                   "  payload areas are shared append-only regions; responses "
                   "copy the request prefix and zero-fill any extra bytes\n"
                   "  the first RPC from each client stays fully serialized "
                   "before the steady-state window opens\n"
                   "  slow-client-count marks the first N client ids as slow; "
                   "slow clients send slow-count-per-client requests using "
                   "uniform pacing with slow-send-gap-ns\n"
                   "  send-mode=uniform keeps a fixed inter-request gap per "
                   "client; staggered adds a client_id*send-gap-ns start "
                   "offset; uneven skews per-client rates while preserving "
                   "aggregate offered load\n",
                   argv[0],
                   (uint64_t)DEFAULT_REQUEST_PAYLOAD_SIZE,
                   (uint64_t)DEFAULT_RESPONSE_PAYLOAD_SIZE);
            return 0;
        } else {
            fprintf(stderr, "unknown argument: %s\n", argv[i]);
            return 2;
        }
    }

    if (!cpu_has_clflushopt()) {
        fprintf(stderr,
                "clflushopt is required for hydrarpc_multiclient_shared\n");
        return 2;
    }

    if (client_count == 0) {
        fprintf(stderr, "client-count must be positive\n");
        rc = 2;
        goto cleanup;
    }

    if (client_count > (NOTIFY_CLIENT_MASK + 1u)) {
        fprintf(stderr, "client-count must be <= %u for 5-bit client_id\n",
                (unsigned)(NOTIFY_CLIENT_MASK + 1u));
        rc = 2;
        goto cleanup;
    }

    if (request_count == 0) {
        fprintf(stderr, "count-per-client must be positive\n");
        rc = 2;
        goto cleanup;
    }

    if (request_payload_size == 0 || request_payload_size > NOTIFY_LEN_MAX) {
        fprintf(stderr,
                "req-bytes must be in [1, %u]\n",
                (unsigned)NOTIFY_LEN_MAX);
        rc = 2;
        goto cleanup;
    }

    if (response_payload_size == 0 || response_payload_size > NOTIFY_LEN_MAX) {
        fprintf(stderr,
                "resp-bytes must be in [1, %u]\n",
                (unsigned)NOTIFY_LEN_MAX);
        rc = 2;
        goto cleanup;
    }

    if (window_size == 0) {
        fprintf(stderr, "window-size must be positive\n");
        rc = 2;
        goto cleanup;
    }

    if (slow_client_count > client_count) {
        fprintf(stderr,
                "slow-client-count (%" PRIu64 ") must be <= client-count (%" PRIu64 ")\n",
                slow_client_count,
                client_count);
        rc = 2;
        goto cleanup;
    }

    if (slow_client_count > 0) {
        if (slow_request_count == 0) {
            fprintf(stderr,
                    "slow-count-per-client must be positive when slow-client-count > 0\n");
            rc = 2;
            goto cleanup;
        }
        if (slow_request_count > request_count) {
            fprintf(stderr,
                    "slow-count-per-client (%" PRIu64
                    ") must be <= count-per-client (%" PRIu64 ")\n",
                    slow_request_count,
                    request_count);
            rc = 2;
            goto cleanup;
        }
        if (slow_send_gap_ns == 0) {
            fprintf(stderr,
                    "slow-send-gap-ns must be positive when slow-client-count > 0\n");
            rc = 2;
            goto cleanup;
        }
    }

    if (send_mode != SEND_GREEDY && send_gap_ns == 0) {
        fprintf(stderr,
                "send-gap-ns must be positive when send-mode is %s\n",
                send_mode_string(send_mode));
        rc = 2;
        goto cleanup;
    }

    if (server_cpu < 0)
        server_cpu = (int)client_count;

    if (slot_count == 0) {
        uint64_t default_depth =
            window_size < request_count ? window_size : request_count;
        slot_count = client_count * default_depth;
    }

    if (slot_count == 0) {
        fprintf(stderr, "slot-count must be positive after defaulting\n");
        rc = 2;
        goto cleanup;
    }

    if (server_cpu < 0 || server_cpu < (int)client_count) {
        fprintf(stderr,
                "server-cpu (%d) must not overlap client cpus [0, %" PRIu64
                ")\n",
                server_cpu,
                client_count);
        rc = 2;
        goto cleanup;
    }

    if (online_cpus <= server_cpu || online_cpus < (long)client_count) {
        fprintf(stderr,
                "need online cpus > max(server-cpu, client-count-1), got %ld\n",
                online_cpus);
        rc = 2;
        goto cleanup;
    }

    request_span = payload_span_u64(request_payload_size);
    response_span = payload_span_u64(response_payload_size);
    total_requests = client_count * request_count;
    total_request_target = total_request_limit(client_count,
                                               request_count,
                                               slow_client_count,
                                               slow_request_count);
    request_queue_size = sizeof(*connection.request_queue) * slot_count;
    response_queue_size = sizeof(*connection.response_queue) * slot_count;
    request_data_size = (size_t)(total_request_target * request_span);
    response_data_size = (size_t)(total_request_target * response_span);
    request_publish_state_size = sizeof(*connection.request_publish_state);
    response_consume_seq_size = sizeof(*connection.response_consume_seq);

    if ((uint64_t)request_data_size > NOTIFY_OFFSET_MASK + 1u ||
        (uint64_t)response_data_size > NOTIFY_OFFSET_MASK + 1u) {
        fprintf(stderr,
                "shared payload area exceeds %u-bit notify offset range\n",
                (unsigned)NOTIFY_OFFSET_BITS);
        rc = 2;
        goto cleanup;
    }

    connection.lock_leaf_count =
        client_count > 1 ? next_pow2_u64(client_count) : 0;
    connection.request_data_size = request_data_size;
    connection.response_data_size = response_data_size;
    request_lock_nodes_size =
        sizeof(*connection.request_lock_nodes) *
        (connection.lock_leaf_count > 0
             ? (connection.lock_leaf_count - 1)
             : 0);
    response_lock_nodes_size =
        sizeof(*connection.response_lock_nodes) *
        (connection.lock_leaf_count > 0
             ? (connection.lock_leaf_count - 1)
             : 0);

    connection.request_queue = alloc_shared_cxl(request_queue_size, cxl_node);
    connection.response_queue = alloc_shared_cxl(response_queue_size, cxl_node);
    connection.request_data_area = alloc_shared_cxl(request_data_size, cxl_node);
    connection.response_data_area = alloc_shared_cxl(response_data_size, cxl_node);
    if (request_lock_nodes_size > 0) {
        connection.request_lock_nodes =
            alloc_shared_cxl(request_lock_nodes_size, cxl_node);
    }
    if (response_lock_nodes_size > 0) {
        connection.response_lock_nodes =
            alloc_shared_cxl(response_lock_nodes_size, cxl_node);
    }
    connection.request_publish_state =
        alloc_shared_cxl(request_publish_state_size, cxl_node);
    connection.response_consume_seq =
        alloc_shared_cxl(response_consume_seq_size, cxl_node);

    init_notify_ring(connection.request_queue, slot_count);
    init_notify_ring(connection.response_queue, slot_count);
    init_peterson_nodes(connection.request_lock_nodes,
                        connection.lock_leaf_count > 0
                            ? (connection.lock_leaf_count - 1)
                            : 0);
    init_peterson_nodes(connection.response_lock_nodes,
                        connection.lock_leaf_count > 0
                            ? (connection.lock_leaf_count - 1)
                            : 0);
    {
        struct RequestPublishState zero_publish_state = {0};

        store_request_publish_state_remote(connection.request_publish_state,
                                           &zero_publish_state);
    }
    write_remote_u64(connection.response_consume_seq, 0);

    child_pids = calloc((size_t)(client_count + 1), sizeof(*child_pids));
    control = alloc_shared_zeroed(sizeof(*control));
    timing.client_req_start_ts_ns = alloc_shared_zeroed(
        sizeof(*timing.client_req_start_ts_ns) * total_requests
    );
    timing.client_resp_done_ts_ns = alloc_shared_zeroed(
        sizeof(*timing.client_resp_done_ts_ns) * total_requests
    );
    timing.server_req_observe_ts_ns = alloc_shared_zeroed(
        sizeof(*timing.server_req_observe_ts_ns) * total_requests
    );
    timing.server_exec_done_ts_ns = alloc_shared_zeroed(
        sizeof(*timing.server_exec_done_ts_ns) * total_requests
    );
    timing.server_resp_done_ts_ns = alloc_shared_zeroed(
        sizeof(*timing.server_resp_done_ts_ns) * total_requests
    );
    timing.server_loop_start_ts_ns = alloc_shared_zeroed(
        sizeof(*timing.server_loop_start_ts_ns)
    );

    if (child_pids == NULL) {
        fprintf(stderr, "allocation failed for child pids\n");
        rc = 1;
        goto cleanup;
    }

    {
        pid_t pid = fork();

        if (pid < 0) {
            fprintf(stderr, "fork server failed: %s\n", strerror(errno));
            rc = 1;
            goto cleanup;
        }
        if (pid == 0) {
            int child_rc = run_server_shared(&connection,
                                             client_count,
                                             slot_count,
                                             request_count,
                                             slow_client_count,
                                             slow_request_count,
                                             total_request_target,
                                             request_payload_size,
                                             response_payload_size,
                                             &timing,
                                             server_cpu,
                                             control);
            _exit(child_rc == 0 ? 0 : 1);
        }
        child_pids[created_children++] = pid;
    }

    for (uint64_t client_id = 0; client_id < client_count; client_id++) {
        pid_t pid = fork();

        if (pid < 0) {
            fprintf(stderr, "fork client %" PRIu64 " failed: %s\n",
                    client_id,
                    strerror(errno));
            rc = 1;
            set_control_error(control);
            goto wait_children;
        }
        if (pid == 0) {
            int child_rc = run_client_shared(&connection,
                                             client_count,
                                             client_id,
                                             window_size,
                                             slot_count,
                                             request_count,
                                             slow_client_count,
                                             slow_request_count,
                                             request_payload_size,
                                             response_payload_size,
                                             send_mode,
                                             send_gap_ns,
                                             slow_send_gap_ns,
                                             &timing,
                                             control);
            _exit(child_rc == 0 ? 0 : 1);
        }
        child_pids[created_children++] = pid;
    }

    while (!control_has_error(control) &&
           __atomic_load_n((const uint64_t *)&control->ready_count,
                           __ATOMIC_ACQUIRE) < client_count + 1) {
    }

    if (control_has_error(control)) {
        rc = 1;
        kill_children(child_pids, created_children);
        goto wait_children;
    }

    __atomic_store_n((uint64_t *)&control->start_flag, 1, __ATOMIC_RELEASE);

wait_children:
    for (uint64_t remaining = created_children; remaining > 0; remaining--) {
        int status = 0;
        pid_t pid = waitpid(-1, &status, 0);

        if (pid < 0) {
            fprintf(stderr, "waitpid failed: %s\n", strerror(errno));
            rc = 1;
            kill_children(child_pids, created_children);
            break;
        }

        mark_child_reaped(child_pids, created_children, pid);
        if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
            if (WIFSIGNALED(status)) {
                fprintf(stderr, "child %ld killed by signal %d\n",
                        (long)pid,
                        WTERMSIG(status));
            } else {
                fprintf(stderr, "child %ld exited with failure\n", (long)pid);
            }
            rc = 1;
            set_control_error(control);
            kill_children(child_pids, created_children);
        }
    }

    if (control_has_error(control))
        rc = 1;

    print_results_with_rc(stdout,
                          rc,
                          client_count,
                          request_count,
                          slow_client_count,
                          slow_request_count,
                          &timing);

cleanup:
    if (connection.request_queue != NULL) {
        munmap((void *)connection.request_queue,
               shared_mapping_size(request_queue_size));
    }
    if (connection.response_queue != NULL) {
        munmap((void *)connection.response_queue,
               shared_mapping_size(response_queue_size));
    }
    if (connection.request_data_area != NULL) {
        munmap((void *)connection.request_data_area,
               shared_mapping_size(request_data_size));
    }
    if (connection.response_data_area != NULL) {
        munmap((void *)connection.response_data_area,
               shared_mapping_size(response_data_size));
    }
    if (connection.request_lock_nodes != NULL) {
        munmap((void *)connection.request_lock_nodes,
               shared_mapping_size(request_lock_nodes_size));
    }
    if (connection.response_lock_nodes != NULL) {
        munmap((void *)connection.response_lock_nodes,
               shared_mapping_size(response_lock_nodes_size));
    }
    if (connection.request_publish_state != NULL) {
        munmap((void *)connection.request_publish_state,
               shared_mapping_size(request_publish_state_size));
    }
    if (connection.response_consume_seq != NULL) {
        munmap((void *)connection.response_consume_seq,
               shared_mapping_size(response_consume_seq_size));
    }

    if (control != NULL)
        munmap((void *)control, shared_mapping_size(sizeof(*control)));
    if (timing.client_req_start_ts_ns != NULL) {
        munmap((void *)timing.client_req_start_ts_ns,
               shared_mapping_size(sizeof(*timing.client_req_start_ts_ns) *
                                   total_requests));
    }
    if (timing.client_resp_done_ts_ns != NULL) {
        munmap((void *)timing.client_resp_done_ts_ns,
               shared_mapping_size(sizeof(*timing.client_resp_done_ts_ns) *
                                   total_requests));
    }
    if (timing.server_req_observe_ts_ns != NULL) {
        munmap((void *)timing.server_req_observe_ts_ns,
               shared_mapping_size(sizeof(*timing.server_req_observe_ts_ns) *
                                   total_requests));
    }
    if (timing.server_exec_done_ts_ns != NULL) {
        munmap((void *)timing.server_exec_done_ts_ns,
               shared_mapping_size(sizeof(*timing.server_exec_done_ts_ns) *
                                   total_requests));
    }
    if (timing.server_resp_done_ts_ns != NULL) {
        munmap((void *)timing.server_resp_done_ts_ns,
               shared_mapping_size(sizeof(*timing.server_resp_done_ts_ns) *
                                   total_requests));
    }
    if (timing.server_loop_start_ts_ns != NULL) {
        munmap((void *)timing.server_loop_start_ts_ns,
               shared_mapping_size(sizeof(*timing.server_loop_start_ts_ns)));
    }

    free(child_pids);
    return rc;
}
