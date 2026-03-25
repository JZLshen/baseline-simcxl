#define _GNU_SOURCE

#include <cpuid.h>
#include <errno.h>
#include <gem5/m5ops.h>
#include <inttypes.h>
#include <linux/mempolicy.h>
#include <sched.h>
#include <signal.h>
#include <stddef.h>
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
#define QUEUE_STRIDE_WORDS (CACHELINE_SIZE / sizeof(uint64_t))

struct Request {
    uint64_t client_id;
    uint64_t req_id;
    uint64_t a;
    uint64_t b;
};

struct Response {
    uint64_t client_id;
    uint64_t req_id;
    uint64_t sum;
};

struct RequestLine {
    struct Request request;
    uint8_t padding[CACHELINE_SIZE - sizeof(struct Request)];
} __attribute__((aligned(CACHELINE_SIZE)));

struct ResponseLine {
    struct Response response;
    uint8_t padding[CACHELINE_SIZE - sizeof(struct Response)];
} __attribute__((aligned(CACHELINE_SIZE)));

struct Connection {
    volatile uint64_t *request_queue;
    volatile uint64_t *response_queue;
    uint8_t *request_data_area;
    uint8_t *response_data_area;
};

struct SharedControl {
    volatile uint64_t ready_count;
    volatile uint64_t start_flag;
    volatile int rc;
};

struct BreakdownData {
    uint64_t *request_publish_ns;
    uint64_t *server_observe_ns;
    uint64_t *response_publish_ns;
    uint64_t *response_observe_ns;
};

enum SendMode {
    SEND_GREEDY = 0,
    SEND_UNIFORM,
    SEND_STAGGERED,
    SEND_UNEVEN,
};

enum RecordBreakdownMode {
    RECORD_BREAKDOWN_NONE = 0,
    RECORD_BREAKDOWN_BASIC,
};

_Static_assert(sizeof(struct Request) <= CACHELINE_SIZE,
               "Request must fit within one cacheline-backed slot");
_Static_assert(sizeof(struct Response) <= CACHELINE_SIZE,
               "Response must fit within one cacheline-backed slot");
_Static_assert(sizeof(struct RequestLine) == CACHELINE_SIZE,
               "RequestLine must occupy exactly one cacheline");
_Static_assert(sizeof(struct ResponseLine) == CACHELINE_SIZE,
               "ResponseLine must occupy exactly one cacheline");

static inline void cpu_relax(void);
static int control_has_error(struct SharedControl *control);

static inline uint64_t
round_up_u64(uint64_t value, uint64_t align)
{
    return (value + align - 1) / align * align;
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

static inline void
cpu_relax(void)
{
    asm volatile("pause" : : : "memory");
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

static enum RecordBreakdownMode
parse_record_breakdown_mode(const char *value)
{
    if (strcmp(value, "none") == 0)
        return RECORD_BREAKDOWN_NONE;
    if (strcmp(value, "basic") == 0)
        return RECORD_BREAKDOWN_BASIC;

    fprintf(stderr, "invalid record-breakdown: %s\n", value);
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
slot_offset(uint64_t slot)
{
    return slot * CACHELINE_SIZE;
}

static inline volatile uint64_t *
queue_entry_ptr(volatile uint64_t *queue, uint64_t slot)
{
    return &queue[slot * QUEUE_STRIDE_WORDS];
}

static inline uint64_t
make_queue_entry(uint64_t offset)
{
    return offset | 0x1u;
}

static inline int
queue_entry_is_valid(uint64_t entry)
{
    return (int)(entry & 0x1u);
}

static inline uint64_t
queue_entry_offset(uint64_t entry)
{
    return entry & ~0x1ull;
}

static int
queue_offset_to_slot(uint64_t offset, uint64_t slot_count, uint64_t *slot_out)
{
    uint64_t slot = 0;

    if ((offset % CACHELINE_SIZE) != 0)
        return 0;

    slot = offset / CACHELINE_SIZE;
    if (slot >= slot_count)
        return 0;

    *slot_out = slot;
    return 1;
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

    for (uint64_t client_id = 0; client_id < client_count; client_id++)
        total += client_request_limit(client_id,
                                      request_count,
                                      slow_client_count,
                                      slow_request_count);

    return total;
}

static inline struct RequestLine *
request_at_slot(struct Connection *connection, uint64_t slot)
{
    return (struct RequestLine *)(connection->request_data_area + slot_offset(slot));
}

static inline struct ResponseLine *
response_at_slot(struct Connection *connection, uint64_t slot)
{
    return (struct ResponseLine *)(connection->response_data_area + slot_offset(slot));
}

static inline struct RequestLine *
request_at_offset(struct Connection *connection, uint64_t offset)
{
    return request_at_slot(connection, offset / CACHELINE_SIZE);
}

static inline struct ResponseLine *
response_at_offset(struct Connection *connection, uint64_t offset)
{
    return response_at_slot(connection, offset / CACHELINE_SIZE);
}

__attribute__((noinline)) static void
copy_line_to_remote(void *dst, const void *src)
{
    memcpy(dst, src, CACHELINE_SIZE);
}

__attribute__((noinline)) static void
copy_line_from_remote(void *dst, const void *src)
{
    volatile const uint8_t *src_bytes =
        (volatile const uint8_t *)(const void *)src;
    uint8_t *dst_bytes = (uint8_t *)dst;

    for (size_t i = 0; i < CACHELINE_SIZE; i++)
        dst_bytes[i] = src_bytes[i];
}

static inline uint64_t
read_queue_token_remote(volatile uint64_t *queue_slot)
{
    clflushopt_line((const void *)queue_slot);
    _mm_mfence();
    return *queue_slot;
}

static inline void
write_queue_token_remote(volatile uint64_t *queue_slot, uint64_t token)
{
    *queue_slot = token;
    clflushopt_line((const void *)queue_slot);
    _mm_sfence();
}

static inline void
store_request_remote(struct RequestLine *dst,
                     uint64_t client_id,
                     uint64_t req_id,
                     uint64_t a,
                     uint64_t b)
{
    struct RequestLine line = {0};

    line.request.client_id = client_id;
    line.request.req_id = req_id;
    line.request.a = a;
    line.request.b = b;
    copy_line_to_remote(dst, &line);
    clflushopt_line((const void *)dst);
    _mm_sfence();
}

static inline void
load_request_remote(struct Request *dst, const struct RequestLine *src)
{
    struct RequestLine line;

    clflushopt_line((const void *)src);
    _mm_mfence();
    copy_line_from_remote(&line, src);
    *dst = line.request;
}

static inline void
store_response_remote(struct ResponseLine *dst,
                      uint64_t client_id,
                      uint64_t req_id,
                      uint64_t sum)
{
    struct ResponseLine line = {0};

    line.response.client_id = client_id;
    line.response.req_id = req_id;
    line.response.sum = sum;
    copy_line_to_remote(dst, &line);
    clflushopt_line((const void *)dst);
    _mm_sfence();
}

static inline void
load_response_remote(struct Response *dst, const struct ResponseLine *src)
{
    struct ResponseLine line;

    clflushopt_line((const void *)src);
    _mm_mfence();
    copy_line_from_remote(&line, src);
    *dst = line.response;
}

static void
init_queue_entries_empty(volatile uint64_t *queue, uint64_t slot_count)
{
    for (uint64_t slot = 0; slot < slot_count; slot++)
        write_queue_token_remote(queue_entry_ptr(queue, slot), 0);
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

        /*
         * Skew per-client request rates while preserving the same aggregate
         * offered load as uniform pacing.
         */
        effective_gap_ns = (uint64_t)((((__uint128_t)send_gap_ns * weight_den) +
                                       weight_num - 1) /
                                      weight_num);
    }

    deadline_ns += sent * effective_gap_ns;
    return m5_rpns_ns() >= deadline_ns;
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
        cpu_relax();
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

static void
signal_ready_and_wait(struct SharedControl *control)
{
    __atomic_fetch_add(&control->ready_count, 1, __ATOMIC_RELEASE);
    wait_for_start(&control->start_flag);
}

static int
run_server_dedicated(struct Connection *connections,
                     uint64_t client_count,
                     uint64_t slot_count,
                     uint64_t request_count,
                     uint64_t slow_client_count,
                     uint64_t slow_request_count,
                     uint64_t total_request_target,
                     struct BreakdownData *breakdown,
                     int server_cpu,
                     struct SharedControl *control)
{
    uint64_t *next_seq = calloc((size_t)client_count, sizeof(*next_seq));
    uint64_t completed_total = 0;
    uint64_t next_client_hint = 0;
    struct Request request_local;
    struct Response response_local;

    if (next_seq == NULL) {
        fprintf(stderr, "server tracking allocation failed\n");
        set_control_error(control);
        free(next_seq);
        return 1;
    }

    if (pin_to_cpu(server_cpu) != 0) {
        fprintf(stderr, "failed to pin server to cpu %d\n", server_cpu);
        set_control_error(control);
        free(next_seq);
        return 1;
    }

    signal_ready_and_wait(control);

    while (completed_total < total_request_target &&
           !control_has_error(control)) {
        int made_progress = 0;

        for (uint64_t scanned = 0;
             scanned < client_count &&
             completed_total < total_request_target;
             scanned++) {
            uint64_t client_id = (next_client_hint + scanned) % client_count;
            struct Connection *connection = &connections[client_id];
            uint64_t seq = next_seq[client_id];
            uint64_t client_request_count = client_request_limit(client_id,
                                                                 request_count,
                                                                 slow_client_count,
                                                                 slow_request_count);
            uint64_t slot = 0;
            uint64_t request_entry = 0;
            uint64_t response_entry = 0;
            uint64_t request_offset = 0;
            uint64_t response_offset = 0;
            uint64_t request_slot = 0;
            size_t index = 0;
            volatile uint64_t *request_queue_slot = NULL;
            volatile uint64_t *response_queue_slot = NULL;
            struct RequestLine *request = NULL;
            struct ResponseLine *response = NULL;

            if (seq >= client_request_count)
                continue;

            slot = seq % slot_count;
            index = request_result_index(client_id, seq, request_count);
            request_queue_slot =
                queue_entry_ptr(connection->request_queue, slot);
            response_queue_slot =
                queue_entry_ptr(connection->response_queue, slot);
            request_entry = read_queue_token_remote(request_queue_slot);
            if (!queue_entry_is_valid(request_entry))
                continue;
            if (breakdown != NULL &&
                breakdown->server_observe_ns != NULL &&
                breakdown->server_observe_ns[index] == 0) {
                store_shared_u64(&breakdown->server_observe_ns[index],
                                 m5_rpns_ns());
            }
            response_entry = read_queue_token_remote(response_queue_slot);
            if (queue_entry_is_valid(response_entry))
                continue;

            request_offset = queue_entry_offset(request_entry);
            if (!queue_offset_to_slot(request_offset, slot_count, &request_slot)) {
                fprintf(stderr,
                        "server request offset invalid client=%" PRIu64
                        " seq=%" PRIu64 " entry=0x%016" PRIx64 "\n",
                        client_id,
                        seq,
                        request_entry);
                set_control_error(control);
                break;
            }

            request = request_at_offset(connection, request_offset);
            load_request_remote(&request_local, request);
            if (request_local.client_id != client_id ||
                request_local.req_id != seq) {
                fprintf(stderr,
                        "server request metadata mismatch client=%" PRIu64
                        " seq=%" PRIu64 " payload_client=%" PRIu64
                        " payload_req=%" PRIu64 "\n",
                        client_id,
                        seq,
                        request_local.client_id,
                        request_local.req_id);
                set_control_error(control);
                break;
            }

            write_queue_token_remote(request_queue_slot, 0);

            response_offset = slot_offset(slot);
            response = response_at_offset(connection, response_offset);
            response_local.client_id = client_id;
            response_local.req_id = seq;
            response_local.sum = request_local.a + request_local.b;

            store_response_remote(response,
                                  response_local.client_id,
                                  response_local.req_id,
                                  response_local.sum);
            write_queue_token_remote(response_queue_slot,
                                     make_queue_entry(response_offset));
            if (breakdown != NULL) {
                if (breakdown->response_publish_ns != NULL) {
                    store_shared_u64(&breakdown->response_publish_ns[index],
                                     m5_rpns_ns());
                }
            }

            next_seq[client_id]++;
            completed_total++;
            next_client_hint = (client_id + 1) % client_count;
            made_progress = 1;
            break;
        }

        if (!made_progress) {
            cpu_relax();
        }
    }

    free(next_seq);
    return control_has_error(control) ? 1 : 0;
}


static int
run_client_dedicated(struct Connection *connection,
                     uint64_t client_count,
                     uint64_t client_id,
                     uint64_t slot_count,
                     uint64_t window_size,
                     uint64_t request_count,
                     uint64_t slow_client_count,
                     uint64_t slow_request_count,
                     enum SendMode send_mode,
                     uint64_t send_gap_ns,
                     uint64_t slow_send_gap_ns,
                     struct BreakdownData *breakdown,
                     uint64_t *start_ns,
                     uint64_t *end_ns,
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
    uint64_t send_schedule_base_ns = 0;
    uint64_t active_window_size = 1;
    uint64_t steady_schedule_started = 0;
    enum SendMode effective_send_mode = send_mode;
    uint64_t effective_send_gap_ns = send_gap_ns;
    uint64_t *expected_local = calloc((size_t)client_request_count, sizeof(*expected_local));

    if (expected_local == NULL) {
        fprintf(stderr, "client %" PRIu64 " local tracking allocation failed\n",
                client_id);
        set_control_error(control);
        free(expected_local);
        return 1;
    }

    if (pin_to_cpu((int)client_id) != 0) {
        fprintf(stderr, "failed to pin client %" PRIu64 " to cpu %" PRIu64 "\n",
                client_id,
                client_id);
        set_control_error(control);
        free(expected_local);
        return 1;
    }

    if (client_is_slow(client_id, slow_client_count)) {
        effective_send_mode = SEND_UNIFORM;
        effective_send_gap_ns = slow_send_gap_ns;
    }

    signal_ready_and_wait(control);
    send_schedule_base_ns = m5_rpns_ns();

    while (completed < client_request_count && !control_has_error(control)) {
        int made_progress = 0;

        /*
         * Keep each client's first RPC fully serialized, then reopen the
         * configured steady-state window for the remaining requests.
         */
        active_window_size = (completed == 0) ? 1 : window_size;

        if (completed > 0 && !steady_schedule_started) {
            /*
             * Non-greedy pacing for the remaining requests should start only
             * after the first end-to-end RPC has completed.
             */
            send_schedule_base_ns = m5_rpns_ns();
            steady_schedule_started = 1;
        }

        if (sent < client_request_count && (sent - completed) < active_window_size) {
            uint64_t slot = sent % slot_count;
            uint64_t request_offset = slot_offset(slot);
            volatile uint64_t *request_queue_slot = queue_entry_ptr(
                connection->request_queue, slot
            );
            struct RequestLine *request = request_at_offset(connection, request_offset);

            if (send_is_due(effective_send_mode,
                            effective_send_gap_ns,
                            client_count,
                            client_id,
                            sent,
                            send_schedule_base_ns)) {
                if (!queue_entry_is_valid(read_queue_token_remote(request_queue_slot))) {
                    uint64_t a = next_rand(&rng_state);
                    uint64_t b = next_rand(&rng_state);
                    uint64_t expected = a + b;
                    uint64_t start = m5_rpns_ns();
                    size_t index = base + sent;

                    store_shared_u64(&start_ns[index], start);
                    expected_local[sent] = expected;
                    store_request_remote(request, client_id, sent, a, b);
                    write_queue_token_remote(request_queue_slot,
                                             make_queue_entry(request_offset));
                    if (breakdown != NULL) {
                        if (breakdown->request_publish_ns != NULL) {
                            store_shared_u64(&breakdown->request_publish_ns[index],
                                             m5_rpns_ns());
                        }
                    }

                    sent++;
                    made_progress = 1;
                }
            }
        }

        if (completed < sent) {
            uint64_t slot = completed % slot_count;
            uint64_t response_entry = 0;
            uint64_t response_offset = 0;
            uint64_t response_slot = 0;
            volatile uint64_t *response_queue_slot = queue_entry_ptr(
                connection->response_queue, slot
            );

            response_entry = read_queue_token_remote(response_queue_slot);
            if (queue_entry_is_valid(response_entry)) {
                struct ResponseLine *response = NULL;
                struct Response response_local;
                uint64_t expected = expected_local[completed];
                uint64_t sum = 0;
                uint64_t response_observe = 0;
                uint64_t end = 0;

                response_offset = queue_entry_offset(response_entry);
                if (!queue_offset_to_slot(response_offset, slot_count, &response_slot)) {
                    fprintf(stderr,
                            "client response offset invalid client=%" PRIu64
                            " req=%" PRIu64 " entry=0x%016" PRIx64 "\n",
                            client_id,
                            completed,
                            response_entry);
                    set_control_error(control);
                    break;
                }
                response = response_at_offset(connection, response_offset);

                response_observe = m5_rpns_ns();
                load_response_remote(&response_local, response);
                if (response_local.client_id != client_id ||
                    response_local.req_id != completed) {
                    fprintf(stderr,
                            "client response metadata mismatch client=%" PRIu64
                            " completed=%" PRIu64 " payload_client=%" PRIu64
                            " payload_req=%" PRIu64 "\n",
                            client_id,
                            completed,
                            response_local.client_id,
                            response_local.req_id);
                    set_control_error(control);
                    break;
                }
                sum = response_local.sum;
                if (sum != expected) {
                    fprintf(stderr,
                            "client response sum mismatch client=%" PRIu64
                            " req=%" PRIu64 " expected=%" PRIu64
                            " actual=%" PRIu64 "\n",
                            client_id,
                            completed,
                            expected,
                            sum);
                    set_control_error(control);
                    break;
                }
                end = m5_rpns_ns();

                if (breakdown != NULL) {
                    if (breakdown->response_observe_ns != NULL) {
                        store_shared_u64(&breakdown->response_observe_ns[base + completed],
                                         response_observe);
                    }
                }
                store_shared_u64(&end_ns[base + completed], end);
                write_queue_token_remote(response_queue_slot, 0);

                completed++;
                made_progress = 1;
            }
        }

        if (!made_progress)
            cpu_relax();
    }

    free(expected_local);
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
              struct BreakdownData *breakdown,
              uint64_t *start_ns,
              uint64_t *end_ns)
{
    for (uint64_t client_id = 0; client_id < client_count; client_id++) {
        uint64_t client_request_count = client_request_limit(client_id,
                                                             request_count,
                                                             slow_client_count,
                                                             slow_request_count);

        for (uint64_t req_id = 0; req_id < client_request_count; req_id++) {
            size_t index = (size_t)(client_id * request_count + req_id);

            fprintf(stream,
                    "client_%" PRIu64 "_req_%" PRIu64 "_start_ns=%" PRIu64 "\n",
                    client_id,
                    req_id,
                    start_ns[index]);
            fprintf(stream,
                    "client_%" PRIu64 "_req_%" PRIu64 "_end_ns=%" PRIu64 "\n",
                    client_id,
                    req_id,
                    end_ns[index]);
            if (breakdown != NULL) {
                if (breakdown->request_publish_ns != NULL) {
                    fprintf(stream,
                            "client_%" PRIu64 "_req_%" PRIu64
                            "_request_publish_ns=%" PRIu64 "\n",
                            client_id,
                            req_id,
                            breakdown->request_publish_ns[index]);
                }
                if (breakdown->server_observe_ns != NULL) {
                    fprintf(stream,
                            "client_%" PRIu64 "_req_%" PRIu64
                            "_server_observe_ns=%" PRIu64 "\n",
                            client_id,
                            req_id,
                            breakdown->server_observe_ns[index]);
                }
                if (breakdown->response_publish_ns != NULL) {
                    fprintf(stream,
                            "client_%" PRIu64 "_req_%" PRIu64
                            "_response_publish_ns=%" PRIu64 "\n",
                            client_id,
                            req_id,
                            breakdown->response_publish_ns[index]);
                }
                if (breakdown->response_observe_ns != NULL) {
                    fprintf(stream,
                            "client_%" PRIu64 "_req_%" PRIu64
                            "_response_observe_ns=%" PRIu64 "\n",
                            client_id,
                            req_id,
                            breakdown->response_observe_ns[index]);
                }
            }
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
                      struct BreakdownData *breakdown,
                      uint64_t *start_ns,
                      uint64_t *end_ns)
{
    fprintf(stream, "benchmark_rc=%d\n", rc);
    write_results(stream,
                  client_count,
                  request_count,
                  slow_client_count,
                  slow_request_count,
                  breakdown,
                  start_ns,
                  end_ns);
}

int
main(int argc, char **argv)
{
    uint64_t client_count = 1;
    uint64_t request_count = 8;
    uint64_t window_size = 16;
    uint64_t slot_count = 0;
    uint64_t slow_client_count = 0;
    uint64_t slow_request_count = 0;
    int cxl_node = 1;
    int server_cpu = -1;
    uint64_t send_gap_ns = 0;
    uint64_t slow_send_gap_ns = 0;
    enum SendMode send_mode = SEND_GREEDY;
    enum RecordBreakdownMode record_breakdown = RECORD_BREAKDOWN_NONE;
    long online_cpus = sysconf(_SC_NPROCESSORS_ONLN);
    uint64_t total_requests = 0;
    uint64_t total_request_target = 0;
    size_t request_queue_size = 0;
    size_t response_queue_size = 0;
    size_t request_data_size = 0;
    size_t response_data_size = 0;
    struct Connection *connections = NULL;
    struct SharedControl *control = NULL;
    struct BreakdownData breakdown = {0};
    uint64_t *start_ns = NULL;
    uint64_t *end_ns = NULL;
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
        } else if (strcmp(argv[i], "--record-breakdown") == 0 &&
                   i + 1 < argc) {
            record_breakdown = parse_record_breakdown_mode(argv[++i]);
        } else if (strcmp(argv[i], "--help") == 0 ||
                   strcmp(argv[i], "-h") == 0) {
            printf("Usage: %s [--client-count N] [--count-per-client N] "
                   "[--window-size N] [--slot-count N] "
                   "[--slow-client-count N] [--slow-count-per-client N] "
                   "[--slow-send-gap-ns N] [--cxl-node N] "
                   "[--send-mode greedy|uniform|staggered|uneven] "
                   "[--send-gap-ns N] "
                   "[--record-breakdown none|basic] "
                   "[--server-cpu N] "
                   "\n"
                   "  window-size is max outstanding requests per client "
                   "after each client's first RPC completes; default is 16\n"
                   "  slot-count is per-client ring depth; default is "
                   "min(window-size, count-per-client)\n"
                   "  slow-client-count marks the first N client ids as slow;"
                   " slow clients send slow-count-per-client requests using "
                   "uniform pacing with slow-send-gap-ns\n"
                   "  send-mode=uniform keeps a fixed inter-request gap per "
                   "client; staggered adds a client_id*send-gap-ns start "
                   "offset; uneven skews per-client rates while preserving "
                   "aggregate offered load\n"
                   "  record-breakdown controls extra stage timestamps; "
                   "start/end timestamps are always recorded\n",
                   argv[0]);
            return 0;
        } else {
            fprintf(stderr, "unknown argument: %s\n", argv[i]);
            return 2;
        }
    }

    if (!cpu_has_clflushopt()) {
        fprintf(stderr,
                "clflushopt is required for hydrarpc_multiclient_dedicated_send1_poll1\n");
        return 2;
    }

    if (client_count == 0) {
        fprintf(stderr, "client-count must be positive\n");
        rc = 2;
        goto cleanup;
    }

    if (request_count == 0) {
        fprintf(stderr, "count-per-client must be positive\n");
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

    if (slot_count == 0)
        slot_count = window_size < request_count ? window_size : request_count;

    if (slot_count == 0) {
        fprintf(stderr,
                "slot-count must be positive after defaulting\n");
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

    total_requests = client_count * request_count;
    total_request_target = total_request_limit(client_count,
                                               request_count,
                                               slow_client_count,
                                               slow_request_count);
    request_queue_size = sizeof(*connections->request_queue) * slot_count *
                         QUEUE_STRIDE_WORDS;
    response_queue_size = sizeof(*connections->response_queue) * slot_count *
                          QUEUE_STRIDE_WORDS;
    request_data_size = (size_t)(CACHELINE_SIZE * slot_count);
    response_data_size = (size_t)(CACHELINE_SIZE * slot_count);

    connections = calloc((size_t)client_count, sizeof(*connections));
    child_pids = calloc((size_t)(client_count + 1), sizeof(*child_pids));
    control = alloc_shared_zeroed(sizeof(*control));
    start_ns = alloc_shared_zeroed(sizeof(*start_ns) * total_requests);
    end_ns = alloc_shared_zeroed(sizeof(*end_ns) * total_requests);
    if (record_breakdown >= RECORD_BREAKDOWN_BASIC) {
        breakdown.request_publish_ns = alloc_shared_zeroed(
            sizeof(*breakdown.request_publish_ns) * total_requests
        );
        breakdown.server_observe_ns = alloc_shared_zeroed(
            sizeof(*breakdown.server_observe_ns) * total_requests
        );
        breakdown.response_publish_ns = alloc_shared_zeroed(
            sizeof(*breakdown.response_publish_ns) * total_requests
        );
        breakdown.response_observe_ns = alloc_shared_zeroed(
            sizeof(*breakdown.response_observe_ns) * total_requests
        );
    }

    if (connections == NULL || child_pids == NULL) {
        fprintf(stderr, "allocation failed for connections/child pids\n");
        rc = 1;
        goto cleanup;
    }

    for (uint64_t client_id = 0; client_id < client_count; client_id++) {
        connections[client_id].request_queue =
            alloc_shared_cxl(request_queue_size, cxl_node);
        connections[client_id].response_queue =
            alloc_shared_cxl(response_queue_size, cxl_node);
        connections[client_id].request_data_area =
            alloc_shared_cxl(request_data_size, cxl_node);
        connections[client_id].response_data_area =
            alloc_shared_cxl(response_data_size, cxl_node);
        init_queue_entries_empty(connections[client_id].request_queue, slot_count);
        init_queue_entries_empty(connections[client_id].response_queue, slot_count);
    }

    {
        pid_t pid = fork();

        if (pid < 0) {
            fprintf(stderr, "fork server failed: %s\n", strerror(errno));
            rc = 1;
            goto cleanup;
        }
        if (pid == 0) {
            int child_rc = run_server_dedicated(connections,
                                                client_count,
                                                slot_count,
                                                request_count,
                                                slow_client_count,
                                                slow_request_count,
                                                total_request_target,
                                                &breakdown,
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
            int child_rc = run_client_dedicated(&connections[client_id],
                                                client_count,
                                                client_id,
                                                slot_count,
                                                window_size,
                                                request_count,
                                                slow_client_count,
                                                slow_request_count,
                                                send_mode,
                                                send_gap_ns,
                                                slow_send_gap_ns,
                                                &breakdown,
                                                start_ns,
                                                end_ns,
                                                control);
            _exit(child_rc == 0 ? 0 : 1);
        }
        child_pids[created_children++] = pid;
    }

    while (!control_has_error(control) &&
           __atomic_load_n((const uint64_t *)&control->ready_count,
                           __ATOMIC_ACQUIRE) < client_count + 1) {
        cpu_relax();
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
                          &breakdown,
                          start_ns,
                          end_ns);

cleanup:
    if (connections != NULL) {
        for (uint64_t client_id = 0; client_id < client_count; client_id++) {
            if (connections[client_id].request_queue != NULL) {
                munmap((void *)connections[client_id].request_queue,
                       shared_mapping_size(request_queue_size));
            }
            if (connections[client_id].response_queue != NULL) {
                munmap((void *)connections[client_id].response_queue,
                       shared_mapping_size(response_queue_size));
            }
            if (connections[client_id].request_data_area != NULL) {
                munmap((void *)connections[client_id].request_data_area,
                       shared_mapping_size(request_data_size));
            }
            if (connections[client_id].response_data_area != NULL) {
                munmap((void *)connections[client_id].response_data_area,
                       shared_mapping_size(response_data_size));
            }
        }
    }

    if (control != NULL)
        munmap((void *)control, shared_mapping_size(sizeof(*control)));
    if (start_ns != NULL)
        munmap((void *)start_ns, shared_mapping_size(sizeof(*start_ns) * total_requests));
    if (end_ns != NULL)
        munmap((void *)end_ns, shared_mapping_size(sizeof(*end_ns) * total_requests));
    if (breakdown.request_publish_ns != NULL) {
        munmap((void *)breakdown.request_publish_ns,
               shared_mapping_size(sizeof(*breakdown.request_publish_ns) * total_requests));
    }
    if (breakdown.server_observe_ns != NULL) {
        munmap((void *)breakdown.server_observe_ns,
               shared_mapping_size(sizeof(*breakdown.server_observe_ns) * total_requests));
    }
    if (breakdown.response_publish_ns != NULL) {
        munmap((void *)breakdown.response_publish_ns,
               shared_mapping_size(sizeof(*breakdown.response_publish_ns) * total_requests));
    }
    if (breakdown.response_observe_ns != NULL) {
        munmap((void *)breakdown.response_observe_ns,
               shared_mapping_size(sizeof(*breakdown.response_observe_ns) * total_requests));
    }

    free(connections);
    free(child_pids);
    return rc;
}
