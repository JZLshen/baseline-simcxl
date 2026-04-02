#if !defined(HYDRARPC_VARIANT_NAME_STR)
#define HYDRARPC_VARIANT_NAME_STR "hydrarpc_shared"
#endif

#if !defined(HYDRARPC_APP_VARIANT)
#define HYDRARPC_APP_VARIANT 0
#endif

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

#if HYDRARPC_APP_VARIANT
#include "hydrarpc_app_runtime.h"
#endif

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

struct CachelineWord {
    volatile uint64_t value;
    uint8_t padding[CACHELINE_SIZE - sizeof(uint64_t)];
} __attribute__((aligned(CACHELINE_SIZE)));

struct PetersonNode {
    /*
     * In non-coherent CXL memory, writeback is cacheline-granular. If flag[0],
     * flag[1], and victim share one line, two contenders can flush stale copies
     * of sibling fields and lose each other's updates. Keep each writer-owned
     * field on its own line.
     */
    struct CachelineWord flag[2];
    struct CachelineWord victim;
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
    volatile uint64_t fail_code;
    volatile uint64_t fail_client_id;
    volatile uint64_t fail_req_id;
    volatile uint64_t fail_aux0;
    volatile uint64_t fail_aux1;
    volatile uint64_t fail_aux2;
    volatile uint64_t fail_aux3;
};

enum SharedFailCode {
    FAIL_NONE = 0,
    FAIL_SERVER_ALLOC = 1,
    FAIL_SERVER_PIN = 2,
    FAIL_SERVER_REQ_ORDINAL = 3,
    FAIL_SERVER_REQ_LEN = 4,
    FAIL_CLIENT_ALLOC = 5,
    FAIL_CLIENT_PIN = 6,
    FAIL_CLIENT_RESP_LEN = 7,
    FAIL_CLIENT_RESP_MISMATCH = 8,
    FAIL_FORK_CLIENT = 9,
    FAIL_WAIT_CHILD = 10,
    FAIL_TRACE_STALL = 11,
};

struct RequestTimingData {
    uint64_t *client_req_publish_seq;
    uint64_t *client_req_publish_offset;
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

static int g_trace_requests = 0;
#define TRACE_ROLE_BYTES 8u
#define TRACE_KIND_BYTES 8u
#define TRACE_STAGE_BYTES 24u
#define TRACE_STALL_TIMEOUT_NS (100ull * 1000ull * 1000ull)

struct TraceContext {
    uint64_t sent;
    uint64_t completed;
    uint64_t window_size;
    uint64_t pending_start_ts_ns;
    uint64_t raw_word;
    uint8_t pending_valid;
};

struct TraceEvent {
    volatile uint64_t committed;
    uint64_t ts_ns;
    uint64_t actor_id;
    uint64_t client_id;
    uint64_t req_id;
    uint64_t seq;
    uint64_t slot;
    uint64_t offset;
    uint64_t len;
    uint64_t aux0;
    uint64_t aux1;
    uint64_t aux2;
    uint64_t sent;
    uint64_t completed;
    uint64_t window_size;
    uint64_t pending_start_ts_ns;
    uint64_t raw_word;
    uint8_t pending_valid;
    uint8_t padding[7];
    char role[TRACE_ROLE_BYTES];
    char kind[TRACE_KIND_BYTES];
    char stage[TRACE_STAGE_BYTES];
};

struct TraceBuffer {
    volatile uint64_t write_index;
    volatile uint64_t dropped_events;
    uint64_t capacity;
    uint8_t padding[CACHELINE_SIZE - (3 * sizeof(uint64_t))];
    struct TraceEvent events[];
} __attribute__((aligned(CACHELINE_SIZE)));

static struct TraceBuffer *g_trace_buffer = NULL;

_Static_assert(sizeof(struct NotifyQueueLine) == CACHELINE_SIZE,
               "NotifyQueueLine must occupy exactly one cacheline");
_Static_assert(sizeof(struct CachelineWord) == CACHELINE_SIZE,
               "CachelineWord must occupy exactly one cacheline");
_Static_assert(sizeof(struct PetersonNode) == 3 * CACHELINE_SIZE,
               "PetersonNode must occupy exactly three cachelines");
_Static_assert(sizeof(struct RequestPublishState) == CACHELINE_SIZE,
               "RequestPublishState must occupy exactly one cacheline");
_Static_assert(sizeof(((struct TraceEvent *)0)->role) == TRACE_ROLE_BYTES,
               "Trace role size mismatch");
_Static_assert(sizeof(((struct TraceEvent *)0)->kind) == TRACE_KIND_BYTES,
               "Trace kind size mismatch");
_Static_assert(sizeof(((struct TraceEvent *)0)->stage) == TRACE_STAGE_BYTES,
               "Trace stage size mismatch");

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

static inline void
copy_trace_text(char *dst, size_t dst_size, const char *src)
{
    if (dst_size == 0)
        return;

    memset(dst, 0, dst_size);
    if (src == NULL)
        return;

    strncpy(dst, src, dst_size - 1u);
}

static uint64_t
trace_event_capacity(uint64_t total_request_target, uint64_t client_count)
{
    uint64_t capacity =
        (total_request_target * 32u) + (client_count * 128u) + 4096u;

    if (capacity < 16384u)
        capacity = 16384u;

    return capacity;
}

static size_t
trace_buffer_size_bytes(uint64_t capacity)
{
    return sizeof(struct TraceBuffer) +
           (size_t)(capacity * sizeof(struct TraceEvent));
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
    unsigned long long parsed = strtoull(value, &end, 0);

    if (!value[0] || (end && *end != '\0')) {
        fprintf(stderr, "invalid %s: %s\n", name, value);
        exit(2);
    }

    return (uint64_t)parsed;
}

#if HYDRARPC_APP_VARIANT
static double
parse_double(const char *name, const char *value)
{
    char *end = NULL;
    double parsed = strtod(value, &end);

    if (!value[0] || (end && *end != '\0')) {
        fprintf(stderr, "invalid %s: %s\n", name, value);
        exit(2);
    }

    return parsed;
}
#endif

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
        flush_remote_range((const void *)&nodes[node_id], sizeof(zero_node));
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

struct PayloadLengthPlan {
    uint64_t min_size;
    uint64_t max_size;
};

static uint64_t
payload_length_plan_len(const struct PayloadLengthPlan *plan,
                        uint64_t client_id,
                        uint64_t req_id,
                        uint64_t client_request_count);

#if HYDRARPC_APP_VARIANT
struct AppClientWorkload {
    hydrarpc_app_operation_t *ops;
    uint64_t request_count;
};

struct AppRuntime {
    const char *profile_name;
    uint64_t record_count;
    uint64_t dataset_seed;
    uint64_t workload_seed;
    size_t key_size;
    size_t value_size;
    size_t max_key_size;
    size_t max_value_size;
    size_t max_request_size;
    size_t max_response_size;
    double read_ratio;
    double update_ratio;
    double rmw_ratio;
    hydrarpc_app_key_dist_t key_dist;
    double zipf_theta;
    int variable_layout;
    struct AppClientWorkload *client_workloads;
    hydrarpc_app_store_t store;
};

static const hydrarpc_app_operation_t *
app_operation_for_seq(const struct AppRuntime *runtime,
                      uint64_t client_id,
                      uint64_t req_id)
{
    if (!runtime || !runtime->client_workloads ||
        !runtime->client_workloads[client_id].ops ||
        req_id >= runtime->client_workloads[client_id].request_count) {
        return NULL;
    }

    return &runtime->client_workloads[client_id].ops[req_id];
}

static void
free_app_runtime(struct AppRuntime *runtime, uint64_t client_count)
{
    if (!runtime)
        return;

    if (runtime->client_workloads != NULL) {
        for (uint64_t client_id = 0; client_id < client_count; client_id++)
            free(runtime->client_workloads[client_id].ops);
    }
    free(runtime->client_workloads);
    hydrarpc_apprt_store_free(&runtime->store);
    memset(runtime, 0, sizeof(*runtime));
}

static int
init_app_runtime(struct AppRuntime *runtime,
                 uint64_t client_count,
                 uint64_t request_count,
                 uint64_t slow_client_count,
                 uint64_t slow_request_count,
                 const hydrarpc_app_profile_t *profile,
                 size_t key_size,
                 size_t value_size,
                 int variable_layout,
                 uint64_t record_count,
                 double read_ratio,
                 double update_ratio,
                 double rmw_ratio,
                 hydrarpc_app_key_dist_t key_dist,
                 double zipf_theta,
                 uint64_t dataset_seed,
                 uint64_t workload_seed)
{
    if (!runtime || !profile || client_count == 0)
        return -1;

    memset(runtime, 0, sizeof(*runtime));
    runtime->profile_name = profile->name;
    runtime->record_count = record_count;
    runtime->dataset_seed = dataset_seed;
    runtime->workload_seed = workload_seed;
    runtime->key_size = key_size;
    runtime->value_size = value_size;
    runtime->read_ratio = read_ratio;
    runtime->update_ratio = update_ratio;
    runtime->rmw_ratio = rmw_ratio;
    runtime->key_dist = key_dist;
    runtime->zipf_theta = zipf_theta;
    runtime->variable_layout = variable_layout;
    runtime->max_key_size =
        variable_layout ?
            hydrarpc_app_profile_max_key_size(profile->name, key_size) :
            key_size;
    runtime->max_value_size =
        variable_layout ?
            hydrarpc_app_profile_max_value_size(profile->name, value_size) :
            value_size;
    runtime->max_request_size =
        hydrarpc_app_request_wire_size(HYDRARPC_APP_OP_PUT,
                                       runtime->max_key_size,
                                       runtime->max_value_size);
    runtime->max_response_size =
        hydrarpc_app_response_wire_size(HYDRARPC_APP_STATUS_OK,
                                        HYDRARPC_APP_OP_GET,
                                        runtime->max_value_size);

    runtime->client_workloads =
        calloc((size_t)client_count, sizeof(*runtime->client_workloads));
    if (runtime->client_workloads == NULL)
        goto fail;

    for (uint64_t client_id = 0; client_id < client_count; client_id++) {
        uint64_t client_request_count = client_request_limit(client_id,
                                                             request_count,
                                                             slow_client_count,
                                                             slow_request_count);

        runtime->client_workloads[client_id].request_count = client_request_count;
        if (client_request_count == 0)
            continue;

        runtime->client_workloads[client_id].ops =
            calloc((size_t)client_request_count,
                   sizeof(*runtime->client_workloads[client_id].ops));
        if (runtime->client_workloads[client_id].ops == NULL)
            goto fail;

        if (hydrarpc_apprt_build_operations(
                runtime->client_workloads[client_id].ops,
                client_request_count,
                profile->name,
                record_count,
                key_size,
                value_size,
                runtime->max_key_size,
                read_ratio,
                update_ratio,
                rmw_ratio,
                key_dist,
                zipf_theta,
                dataset_seed,
                workload_seed ^ (client_id + 1u)) != 0) {
            goto fail;
        }
    }

    if (hydrarpc_apprt_store_init(&runtime->store,
                                  profile->name,
                                  (size_t)record_count,
                                  key_size,
                                  value_size,
                                  runtime->max_key_size,
                                  runtime->max_value_size,
                                  dataset_seed) != 0 ||
        hydrarpc_apprt_store_preload(&runtime->store) != 0) {
        goto fail;
    }

    return 0;

fail:
    free_app_runtime(runtime, client_count);
    return -1;
}

static inline uint64_t
request_len_for_seq(const struct PayloadLengthPlan *plan,
                    const struct AppRuntime *app_runtime,
                    uint64_t client_id,
                    uint64_t req_id,
                    uint64_t client_request_count)
{
    const hydrarpc_app_operation_t *op =
        app_operation_for_seq(app_runtime, client_id, req_id);

    (void)plan;
    (void)client_request_count;
    return op ? op->request_len : 0;
}

static inline uint64_t
response_len_for_seq(const struct PayloadLengthPlan *plan,
                     const struct AppRuntime *app_runtime,
                     uint64_t client_id,
                     uint64_t req_id,
                     uint64_t client_request_count)
{
    const hydrarpc_app_operation_t *op =
        app_operation_for_seq(app_runtime, client_id, req_id);

    (void)plan;
    (void)client_request_count;
    return op ? op->response_len : 0;
}
#else
static inline uint64_t
request_len_for_seq(const struct PayloadLengthPlan *plan,
                    uint64_t client_id,
                    uint64_t req_id,
                    uint64_t client_request_count)
{
    return payload_length_plan_len(plan, client_id, req_id, client_request_count);
}

static inline uint64_t
response_len_for_seq(const struct PayloadLengthPlan *plan,
                     uint64_t client_id,
                     uint64_t req_id,
                     uint64_t client_request_count)
{
    return payload_length_plan_len(plan, client_id, req_id, client_request_count);
}
#endif

static inline uint64_t
payload_length_plan_stride(const struct PayloadLengthPlan *plan)
{
    return plan->max_size;
}

static inline int
payload_length_plan_is_uniform(const struct PayloadLengthPlan *plan)
{
    return plan->min_size != plan->max_size;
}

static inline uint64_t
payload_length_plan_midpoint(const struct PayloadLengthPlan *plan)
{
    return plan->min_size + ((plan->max_size - plan->min_size) / 2u);
}

static inline uint64_t
payload_length_plan_slot_index(uint64_t client_id,
                               uint64_t req_id,
                               uint64_t client_request_count)
{
    if (client_request_count <= 1)
        return 0;

    return (req_id + (client_id % client_request_count)) % client_request_count;
}

static uint64_t
payload_length_plan_len(const struct PayloadLengthPlan *plan,
                        uint64_t client_id,
                        uint64_t req_id,
                        uint64_t client_request_count)
{
    uint64_t span = 0;
    uint64_t denom = 0;
    uint64_t slot_index = 0;

    if (!payload_length_plan_is_uniform(plan))
        return plan->min_size;

    if (client_request_count <= 1)
        return payload_length_plan_midpoint(plan);

    span = plan->max_size - plan->min_size;
    denom = client_request_count - 1;
    slot_index = payload_length_plan_slot_index(client_id,
                                                req_id,
                                                client_request_count);
    return plan->min_size +
           (uint64_t)((((__uint128_t)span * slot_index) + (denom / 2u)) / denom);
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
debug_mark(const char *role,
           uint64_t id,
           const char *stage,
           uint64_t aux0,
           uint64_t aux1)
{
    uint64_t index = 0;
    struct TraceEvent *event = NULL;

    if (!g_trace_requests || g_trace_buffer == NULL)
        return;

    index = __atomic_fetch_add(&g_trace_buffer->write_index,
                               1,
                               __ATOMIC_RELAXED);
    if (index >= g_trace_buffer->capacity) {
        __atomic_fetch_add(&g_trace_buffer->dropped_events, 1, __ATOMIC_RELAXED);
        return;
    }

    event = &g_trace_buffer->events[index];
    memset(event, 0, sizeof(*event));
    event->ts_ns = m5_rpns_ns();
    event->actor_id = id;
    event->aux0 = aux0;
    event->aux1 = aux1;
    copy_trace_text(event->role, sizeof(event->role), role);
    copy_trace_text(event->kind, sizeof(event->kind), "DBG");
    copy_trace_text(event->stage, sizeof(event->stage), stage);
    __atomic_store_n(&event->committed, 1, __ATOMIC_RELEASE);
}

static void
trace_event_log(const char *role,
                const char *kind,
                const char *stage,
                uint64_t actor_id,
                uint64_t client_id,
                uint64_t req_id,
                uint64_t seq,
                uint64_t slot,
                uint64_t offset,
                uint64_t len,
                uint64_t aux0,
                uint64_t aux1,
                uint64_t aux2,
                const struct TraceContext *context)
{
    uint64_t index = 0;
    struct TraceEvent *event = NULL;

    if (!g_trace_requests || g_trace_buffer == NULL)
        return;

    index = __atomic_fetch_add(&g_trace_buffer->write_index,
                               1,
                               __ATOMIC_RELAXED);
    if (index >= g_trace_buffer->capacity) {
        __atomic_fetch_add(&g_trace_buffer->dropped_events, 1, __ATOMIC_RELAXED);
        return;
    }

    event = &g_trace_buffer->events[index];
    memset(event, 0, sizeof(*event));
    event->ts_ns = m5_rpns_ns();
    event->actor_id = actor_id;
    event->client_id = client_id;
    event->req_id = req_id;
    event->seq = seq;
    event->slot = slot;
    event->offset = offset;
    event->len = len;
    event->aux0 = aux0;
    event->aux1 = aux1;
    event->aux2 = aux2;
    if (context != NULL) {
        event->sent = context->sent;
        event->completed = context->completed;
        event->window_size = context->window_size;
        event->pending_start_ts_ns = context->pending_start_ts_ns;
        event->raw_word = context->raw_word;
        event->pending_valid = context->pending_valid;
    }
    copy_trace_text(event->role, sizeof(event->role), role);
    copy_trace_text(event->kind, sizeof(event->kind), kind);
    copy_trace_text(event->stage, sizeof(event->stage), stage);
    __atomic_store_n(&event->committed, 1, __ATOMIC_RELEASE);
}

static void
debug_queue_state_event(const char *role,
                        uint64_t actor_id,
                        uint64_t client_id,
                        uint64_t req_id,
                        const char *stage,
                        uint64_t seq,
                        uint64_t slot,
                        uint64_t offset,
                        uint64_t len,
                        uint64_t aux0,
                        uint64_t aux1,
                        uint64_t aux2,
                        const struct TraceContext *context)
{
    trace_event_log(role,
                    "HEAD",
                    stage,
                    actor_id,
                    client_id,
                    req_id,
                    seq,
                    slot,
                    offset,
                    len,
                    aux0,
                    aux1,
                    aux2,
                    context);
}

static void
debug_request_state_event(const char *role,
                          uint64_t actor_id,
                          uint64_t client_id,
                          uint64_t req_id,
                          const char *stage,
                          uint64_t seq,
                          uint64_t slot,
                          uint64_t offset,
                          uint64_t len,
                          uint64_t aux0,
                          uint64_t aux1,
                          uint64_t aux2,
                          const struct TraceContext *context)
{
    trace_event_log(role,
                    "REQ",
                    stage,
                    actor_id,
                    client_id,
                    req_id,
                    seq,
                    slot,
                    offset,
                    len,
                    aux0,
                    aux1,
                    aux2,
                    context);
}

static void
debug_request_event(const char *role,
                    uint64_t actor_id,
                    uint64_t client_id,
                    uint64_t req_id,
                    const char *stage,
                    uint64_t aux0,
                    uint64_t aux1,
                    uint64_t aux2)
{
    debug_request_state_event(role,
                              actor_id,
                              client_id,
                              req_id,
                              stage,
                              0,
                              aux0,
                              aux1,
                              aux2,
                              0,
                              0,
                              0,
                              NULL);
}

static void
debug_response_head_state_event(uint64_t actor_id,
                                uint64_t completed,
                                const char *stage,
                                uint64_t response_seq,
                                uint64_t response_slot,
                                uint64_t owner_client_id,
                                uint64_t response_offset,
                                uint64_t response_len,
                                const struct TraceContext *context)
{
    trace_event_log("client",
                    "RSP",
                    stage,
                    actor_id,
                    owner_client_id,
                    completed,
                    response_seq,
                    response_slot,
                    response_offset,
                    response_len,
                    0,
                    0,
                    0,
                    context);
}

static void
record_control_error(struct SharedControl *control,
                     uint64_t fail_code,
                     uint64_t fail_client_id,
                     uint64_t fail_req_id,
                     uint64_t fail_aux0,
                     uint64_t fail_aux1,
                     uint64_t fail_aux2,
                     uint64_t fail_aux3)
{
    int expected = 0;

    if (__atomic_compare_exchange_n(&control->rc,
                                    &expected,
                                    1,
                                    0,
                                    __ATOMIC_RELAXED,
                                    __ATOMIC_RELAXED)) {
        __atomic_store_n((uint64_t *)&control->fail_code,
                         fail_code,
                         __ATOMIC_RELAXED);
        __atomic_store_n((uint64_t *)&control->fail_client_id,
                         fail_client_id,
                         __ATOMIC_RELAXED);
        __atomic_store_n((uint64_t *)&control->fail_req_id,
                         fail_req_id,
                         __ATOMIC_RELAXED);
        __atomic_store_n((uint64_t *)&control->fail_aux0,
                         fail_aux0,
                         __ATOMIC_RELAXED);
        __atomic_store_n((uint64_t *)&control->fail_aux1,
                         fail_aux1,
                         __ATOMIC_RELAXED);
        __atomic_store_n((uint64_t *)&control->fail_aux2,
                         fail_aux2,
                         __ATOMIC_RELAXED);
        __atomic_store_n((uint64_t *)&control->fail_aux3,
                         fail_aux3,
                         __ATOMIC_RELAXED);
    } else {
        __atomic_store_n(&control->rc, 1, __ATOMIC_RELAXED);
    }
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

#if HYDRARPC_APP_VARIANT
static int
build_client_request_payload(const struct AppRuntime *app_runtime,
                             uint64_t client_id,
                             uint64_t req_id,
                             uint8_t *remote_dst,
                             uint8_t *request_shadow,
                             uint64_t request_payload_size)
{
    const hydrarpc_app_operation_t *op =
        app_operation_for_seq(app_runtime, client_id, req_id);
    size_t built_len = 0;

    if (op == NULL)
        return -1;

    built_len = hydrarpc_apprt_encode_request(request_shadow,
                                              (size_t)request_payload_size,
                                              op,
                                              app_runtime->dataset_seed);
    if (built_len != request_payload_size)
        return -1;

    store_payload_remote(remote_dst, request_shadow, request_payload_size);
    return 0;
}

static int
build_server_response_payload(struct AppRuntime *app_runtime,
                              uint8_t *remote_dst,
                              uint8_t *response_staging,
                              uint64_t response_payload_stride,
                              const uint8_t *request_bytes,
                              uint64_t request_payload_size,
                              uint64_t *response_payload_size_out)
{
    size_t response_len = 0;

    if (!response_staging || !response_payload_size_out)
        return -1;

    if (hydrarpc_apprt_store_process_request(&app_runtime->store,
                                             request_bytes,
                                             (size_t)request_payload_size,
                                             response_staging,
                                             (size_t)response_payload_stride,
                                             &response_len) != 0) {
        return -1;
    }

    *response_payload_size_out = response_len;
    store_payload_remote(remote_dst, response_staging, response_len);
    return 0;
}

static int
client_response_matches(const struct AppRuntime *app_runtime,
                        uint64_t client_id,
                        uint64_t req_id,
                        const uint8_t *response_bytes,
                        uint64_t response_payload_size,
                        const uint8_t *request_bytes,
                        uint64_t request_payload_size)
{
    const hydrarpc_app_operation_t *op =
        app_operation_for_seq(app_runtime, client_id, req_id);

    (void)request_bytes;
    (void)request_payload_size;
    return (op != NULL &&
            hydrarpc_apprt_validate_response(response_bytes,
                                             (size_t)response_payload_size,
                                             op) == 0);
}
#else
static int
build_client_request_payload(uint8_t *remote_dst,
                             uint8_t *request_shadow,
                             uint64_t request_payload_size,
                             uint64_t *rng_state)
{
    fill_random_payload_bytes(request_shadow, request_payload_size, rng_state);
    store_payload_remote(remote_dst, request_shadow, request_payload_size);
    return 0;
}

static int
build_server_response_payload(uint8_t *remote_dst,
                              uint8_t *response_staging,
                              uint64_t response_payload_size,
                              const uint8_t *request_bytes,
                              uint64_t request_payload_size,
                              uint64_t *response_payload_size_out)
{
    build_response_payload_bytes(response_staging,
                                 response_payload_size,
                                 request_bytes,
                                 request_payload_size);
    store_payload_remote(remote_dst, response_staging, response_payload_size);
    if (response_payload_size_out != NULL)
        *response_payload_size_out = response_payload_size;
    return 0;
}

static int
client_response_matches(uint64_t client_id,
                        uint64_t req_id,
                        const uint8_t *response_bytes,
                        uint64_t response_payload_size,
                        const uint8_t *request_bytes,
                        uint64_t request_payload_size)
{
    (void)client_id;
    (void)req_id;
    return response_matches_request(response_bytes,
                                    response_payload_size,
                                    request_bytes,
                                    request_payload_size);
}
#endif

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
    node->flag[side].value = 1;
    clflushopt_line((const void *)&node->flag[side]);
    _mm_sfence();

    node->victim.value = side;
    clflushopt_line((const void *)&node->victim);
    _mm_sfence();
}

static void
peterson_clear_remote(struct PetersonNode *node, uint64_t side)
{
    node->flag[side].value = 0;
    clflushopt_line((const void *)&node->flag[side]);
    _mm_sfence();
}

static void
peterson_load_remote(const struct PetersonNode *src,
                     uint64_t other_side,
                     uint64_t *other_flag,
                     uint64_t *victim)
{
    clflushopt_line((const void *)&src->flag[other_side]);
    clflushopt_line((const void *)&src->victim);
    _mm_mfence();
    *other_flag = src->flag[other_side].value;
    *victim = src->victim.value;
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
        uint64_t other_flag = 0;
        uint64_t victim = 0;

        path_nodes[depth] = parent;
        path_sides[depth] = (uint8_t)side;
        depth++;

        peterson_publish_remote(node, side);

        for (;;) {
            peterson_load_remote(node, other_side, &other_flag, &victim);
            if (other_flag == 0 || victim != side)
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
                  const struct PayloadLengthPlan *request_length_plan,
                  const struct PayloadLengthPlan *response_length_plan,
#if HYDRARPC_APP_VARIANT
                  struct AppRuntime *app_runtime,
#endif
                  struct RequestTimingData *timing,
                  int server_cpu,
                  struct SharedControl *control)
{
    uint64_t request_payload_stride =
        payload_length_plan_stride(request_length_plan);
    uint64_t response_payload_stride =
        payload_length_plan_stride(response_length_plan);
    int first_req_marked = 0;
    int first_resp_marked = 0;
    uint64_t last_request_head_trace_seq = UINT64_MAX;
    uint64_t last_request_head_trace_notify = UINT64_MAX;
    uint64_t last_response_tail_trace_seq = UINT64_MAX;
    uint64_t last_response_tail_trace_notify = UINT64_MAX;
    uint64_t request_consume_seq = 0;
    uint64_t response_produce_seq = 0;
    uint64_t response_data_tail = 0;
    uint64_t completed_total = 0;
    uint64_t *next_req_seq_per_client =
        calloc((size_t)client_count, sizeof(*next_req_seq_per_client));
    uint8_t *request_payload_local = malloc((size_t)request_payload_stride);
    uint8_t *response_staging = malloc((size_t)response_payload_stride);

    if (next_req_seq_per_client == NULL ||
        request_payload_local == NULL ||
        response_staging == NULL) {
        fprintf(stderr, "server tracking allocation failed\n");
        record_control_error(control, FAIL_SERVER_ALLOC, 0, 0, 0, 0, 0, 0);
        free(next_req_seq_per_client);
        free(request_payload_local);
        free(response_staging);
        return 1;
    }

    if (pin_to_cpu(server_cpu) != 0) {
        fprintf(stderr, "failed to pin server to cpu %d\n", server_cpu);
        record_control_error(control, FAIL_SERVER_PIN, 0, 0, 0, 0, 0, 0);
        free(next_req_seq_per_client);
        free(request_payload_local);
        free(response_staging);
        return 1;
    }

    debug_mark("server", 0, "pinned", (uint64_t)server_cpu, total_request_target);
    debug_mark("server", 0, "ready_wait", 0, 0);
    signal_ready_and_wait(control);
    debug_mark("server", 0, "start", 0, 0);
    store_shared_u64(timing->server_loop_start_ts_ns, m5_rpns_ns());

    while (completed_total < total_request_target &&
           !control_has_error(control)) {
        uint64_t request_slot = request_consume_seq % slot_count;
        volatile struct NotifyQueueLine *request_queue_line =
            notify_queue_line_at(connection->request_queue, request_slot);
        uint64_t request_notify = load_notify_word_remote(request_queue_line);

        if (request_consume_seq != last_request_head_trace_seq ||
            request_notify != last_request_head_trace_notify) {
            const char *stage = "srv_req_head_invalid";
            uint64_t trace_client_id = UINT64_MAX;
            uint64_t trace_req_id = UINT64_MAX;
            uint64_t trace_len = 0;
            uint64_t trace_offset = 0;

            if (notify_valid(request_notify)) {
                trace_client_id = notify_client_id(request_notify);
                trace_req_id = next_req_seq_per_client[trace_client_id];
                trace_len = notify_len(request_notify);
                trace_offset = notify_offset(request_notify);
                stage = "srv_req_head_valid";
            }

            debug_queue_state_event("server",
                                    0,
                                    trace_client_id,
                                    trace_req_id,
                                    stage,
                                    request_consume_seq,
                                    request_slot,
                                    trace_offset,
                                    trace_len,
                                    request_notify,
                                    completed_total,
                                    total_request_target,
                                    NULL);
            last_request_head_trace_seq = request_consume_seq;
            last_request_head_trace_notify = request_notify;
        }

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
            uint64_t expected_request_len =
#if HYDRARPC_APP_VARIANT
                request_len_for_seq(request_length_plan,
                                    app_runtime,
                                    request_client_id,
                                    request_req_id,
                                    client_request_count);
            uint64_t expected_response_len =
                response_len_for_seq(response_length_plan,
                                     app_runtime,
                                     request_client_id,
                                     request_req_id,
                                     client_request_count);
#else
                request_len_for_seq(request_length_plan,
                                    request_client_id,
                                    request_req_id,
                                    client_request_count);
            uint64_t expected_response_len =
                response_len_for_seq(response_length_plan,
                                     request_client_id,
                                     request_req_id,
                                     client_request_count);
#endif
            uint64_t server_req_observe_ts = 0;
            uint64_t server_exec_done_ts = 0;
            uint64_t server_resp_done_ts = 0;
            size_t index = 0;

            if (request_req_id >= client_request_count) {
                fprintf(stderr, "server observed unexpected request ordinal\n");
                record_control_error(control,
                                     FAIL_SERVER_REQ_ORDINAL,
                                     request_client_id,
                                     request_req_id,
                                     client_request_count,
                                     0,
                                     0,
                                     0);
                break;
            }
            index = request_result_index(request_client_id,
                                         request_req_id,
                                         request_count);

            if (request_len != expected_request_len) {
                fprintf(stderr, "server observed unexpected request length\n");
                record_control_error(control,
                                     FAIL_SERVER_REQ_LEN,
                                     request_client_id,
                                     request_req_id,
                                     request_len,
                                     expected_request_len,
                                     0,
                                     0);
                break;
            }

            if (!first_req_marked) {
                debug_mark("server",
                           0,
                           "first_req",
                           request_client_id,
                           request_req_id);
                first_req_marked = 1;
            }

            server_req_observe_ts = m5_rpns_ns();
            debug_request_event("server",
                                0,
                                request_client_id,
                                request_req_id,
                                "consume",
                                request_slot,
                                request_offset,
                                request_len);
            load_payload_remote(request_payload_local,
                                payload_at_offset(connection->request_data_area,
                                                  request_offset),
                                request_len);
            server_exec_done_ts = m5_rpns_ns();
            clear_notify_word_remote(request_queue_line);

            if (
#if HYDRARPC_APP_VARIANT
                build_server_response_payload(app_runtime,
                                              payload_at_offset(connection->response_data_area,
                                                                response_offset),
                                              response_staging,
                                              response_payload_stride,
                                              request_payload_local,
                                              request_len,
                                              &expected_response_len) != 0
#else
                build_server_response_payload(
                    payload_at_offset(connection->response_data_area,
                                      response_offset),
                    response_staging,
                    expected_response_len,
                    request_payload_local,
                    request_len,
                    &expected_response_len) != 0
#endif
            ) {
                fprintf(stderr, "server failed to build response payload\n");
                record_control_error(control,
                                     FAIL_SERVER_REQ_LEN,
                                     request_client_id,
                                     request_req_id,
                                     request_len,
                                     expected_response_len,
                                     0,
                                     0);
                break;
            }
            response_data_tail += payload_span_u64(expected_response_len);

            response_notify =
                notify_make(response_offset,
                            request_client_id,
                            expected_response_len);

            for (;;) {
                response_slot = response_produce_seq % slot_count;
                {
                    volatile struct NotifyQueueLine *response_queue_line =
                        notify_queue_line_at(connection->response_queue,
                                             response_slot);
                    uint64_t response_slot_notify =
                        load_notify_word_remote(response_queue_line);
                    const char *stage = notify_valid(response_slot_notify)
                                            ? "srv_resp_tail_busy"
                                            : "srv_resp_tail_free";

                    if (response_produce_seq != last_response_tail_trace_seq ||
                        response_slot_notify != last_response_tail_trace_notify) {
                        debug_queue_state_event("server",
                                                0,
                                                request_client_id,
                                                request_req_id,
                                                stage,
                                                response_produce_seq,
                                                response_slot,
                                                response_offset,
                                                expected_response_len,
                                                response_slot_notify,
                                                completed_total,
                                                total_request_target,
                                                NULL);
                        last_response_tail_trace_seq = response_produce_seq;
                        last_response_tail_trace_notify = response_slot_notify;
                    }

                    if (!notify_valid(response_slot_notify)) {
                        write_notify_word_remote(response_queue_line, response_notify);
                        response_produce_seq++;
                        server_resp_done_ts = m5_rpns_ns();
                        debug_request_event("server",
                                            0,
                                            request_client_id,
                                            request_req_id,
                                            "respond",
                                            response_slot,
                                            response_offset,
                                            expected_response_len);
                        if (!first_resp_marked) {
                            debug_mark("server",
                                       0,
                                       "first_resp",
                                       response_slot,
                                       response_offset);
                            first_resp_marked = 1;
                        }
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
    debug_mark("server", 0, "exit", completed_total, control_has_error(control));
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
                  const struct PayloadLengthPlan *request_length_plan,
                  const struct PayloadLengthPlan *response_length_plan,
                  enum SendMode send_mode,
                  uint64_t send_gap_ns,
                  uint64_t slow_send_gap_ns,
#if HYDRARPC_APP_VARIANT
                  const struct AppRuntime *app_runtime,
#endif
                  struct RequestTimingData *timing,
                  struct SharedControl *control)
{
    int first_pending_marked = 0;
    int first_lock_marked = 0;
    int first_publish_marked = 0;
    int first_resp_marked = 0;
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
    uint64_t request_payload_stride =
        payload_length_plan_stride(request_length_plan);
    uint64_t response_payload_stride =
        payload_length_plan_stride(response_length_plan);
    uint8_t *request_shadow_local = calloc((size_t)client_request_count,
                                           (size_t)request_payload_stride);
    uint8_t *response_local = malloc((size_t)response_payload_stride);
    uint64_t last_response_trace_seq = UINT64_MAX;
    uint64_t last_response_trace_notify = UINT64_MAX;
    uint64_t last_request_head_trace_seq = UINT64_MAX;
    uint64_t last_request_head_trace_tail = UINT64_MAX;
    uint64_t last_request_head_trace_notify = UINT64_MAX;
    int pending_valid = 0;
    uint64_t request_lock_path_nodes[MAX_TOURNAMENT_DEPTH];
    uint8_t request_lock_path_sides[MAX_TOURNAMENT_DEPTH];

    if (request_shadow_local == NULL || response_local == NULL) {
        fprintf(stderr,
                "client %" PRIu64 " local tracking allocation failed\n",
                client_id);
        free(request_shadow_local);
        free(response_local);
        record_control_error(control, FAIL_CLIENT_ALLOC, client_id, 0, 0, 0, 0, 0);
        return 1;
    }

    if (pin_to_cpu((int)client_id) != 0) {
        fprintf(stderr, "failed to pin client %" PRIu64 " to cpu %" PRIu64 "\n",
                client_id,
                client_id);
        record_control_error(control, FAIL_CLIENT_PIN, client_id, 0, 0, 0, 0, 0);
        free(request_shadow_local);
        free(response_local);
        return 1;
    }

    debug_mark("client", client_id, "pinned", client_request_count, window_size);

    if (client_is_slow(client_id, slow_client_count)) {
        effective_send_mode = SEND_UNIFORM;
        effective_send_gap_ns = slow_send_gap_ns;
    }

    debug_mark("client", client_id, "ready_wait", 0, 0);
    signal_ready_and_wait(control);
    debug_mark("client", client_id, "start", 0, 0);
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
            uint64_t request_payload_len =
                request_len_for_seq(
                    request_length_plan,
#if HYDRARPC_APP_VARIANT
                    app_runtime,
#endif
                    client_id,
                    sent,
                    client_request_count);
            uint8_t *request_shadow =
                request_shadow_at(request_shadow_local,
                                  sent,
                                  request_payload_stride);

            pending_start = m5_rpns_ns();
#if HYDRARPC_APP_VARIANT
            (void)request_shadow;
            (void)request_payload_len;
#else
            fill_random_payload_bytes(request_shadow,
                                      request_payload_len,
                                      &rng_state);
#endif
            pending_valid = 1;
            if (!first_pending_marked) {
                debug_mark("client", client_id, "first_pending", sent, pending_start);
                first_pending_marked = 1;
            }
        }

        if (pending_valid && !control_has_error(control)) {
            uint64_t lock_depth = tournament_lock_acquire(connection->request_lock_nodes,
                                                          connection->lock_leaf_count,
                                                          client_id,
                                                          request_lock_path_nodes,
                                                          request_lock_path_sides);

            if (!first_lock_marked) {
                debug_mark("client", client_id, "first_lock", sent, completed);
                first_lock_marked = 1;
            }

            if (!control_has_error(control)) {
                struct RequestPublishState publish_state;
                uint64_t request_seq = 0;
                uint64_t request_slot = request_seq % slot_count;
                uint64_t request_notify = 0;
                volatile struct NotifyQueueLine *request_queue_line =
                    notify_queue_line_at(connection->request_queue, request_slot);

                load_request_publish_state_remote(&publish_state,
                                                  connection->request_publish_state);
                request_seq = publish_state.produce_seq;
                request_slot = request_seq % slot_count;
                request_queue_line =
                    notify_queue_line_at(connection->request_queue, request_slot);
                request_notify = load_notify_word_remote(request_queue_line);

                if (request_seq != last_request_head_trace_seq ||
                    publish_state.data_tail != last_request_head_trace_tail ||
                    request_notify != last_request_head_trace_notify) {
                    uint64_t request_payload_len =
                        request_len_for_seq(
                            request_length_plan,
#if HYDRARPC_APP_VARIANT
                            app_runtime,
#endif
                            client_id,
                            sent,
                            client_request_count);
                    struct TraceContext context = {
                        .sent = sent,
                        .completed = completed,
                        .window_size = active_window_size,
                        .pending_start_ts_ns = pending_start,
                        .raw_word = request_notify,
                        .pending_valid = (uint8_t)pending_valid,
                    };
                    const char *stage = notify_valid(request_notify)
                                            ? "req_head_busy"
                                            : "req_head_free";

                    debug_queue_state_event("client",
                                            client_id,
                                            client_id,
                                            sent,
                                            stage,
                                            request_seq,
                                            request_slot,
                                            publish_state.data_tail,
                                            request_payload_len,
                                            request_notify,
                                            0,
                                            0,
                                            &context);
                    last_request_head_trace_seq = request_seq;
                    last_request_head_trace_tail = publish_state.data_tail;
                    last_request_head_trace_notify = request_notify;
                }

                if (!notify_valid(request_notify)) {
                    uint64_t request_offset = publish_state.data_tail;
                    uint64_t request_payload_len =
                        request_len_for_seq(
                            request_length_plan,
#if HYDRARPC_APP_VARIANT
                            app_runtime,
#endif
                            client_id,
                            sent,
                            client_request_count);
                    size_t index = base + sent;
                    uint8_t *request_shadow =
                        request_shadow_at(request_shadow_local,
                                          sent,
                                          request_payload_stride);
                    struct TraceContext context = {
                        .sent = sent,
                        .completed = completed,
                        .window_size = active_window_size,
                        .pending_start_ts_ns = pending_start,
                        .raw_word = request_notify,
                        .pending_valid = (uint8_t)pending_valid,
                    };

                    store_shared_u64(&timing->client_req_publish_seq[index],
                                     request_seq);
                    store_shared_u64(&timing->client_req_publish_offset[index],
                                     request_offset);
                    store_shared_u64(&timing->client_req_start_ts_ns[index],
                                     pending_start);
                    if (
#if HYDRARPC_APP_VARIANT
                        build_client_request_payload(
                            app_runtime,
                            client_id,
                            sent,
                            payload_at_offset(connection->request_data_area,
                                              request_offset),
                            request_shadow,
                            request_payload_len
                        ) != 0
#else
                        build_client_request_payload(
                            payload_at_offset(connection->request_data_area,
                                              request_offset),
                            request_shadow,
                            request_payload_len,
                            &rng_state
                        ) != 0
#endif
                    ) {
                        record_control_error(control,
                                             FAIL_CLIENT_RESP_MISMATCH,
                                             client_id,
                                             sent,
                                             request_payload_len,
                                             0,
                                             0,
                                             0);
                        tournament_lock_release(connection->request_lock_nodes,
                                                request_lock_path_nodes,
                                                request_lock_path_sides,
                                                lock_depth);
                        free(request_shadow_local);
                        free(response_local);
                        return 1;
                    }
                    write_notify_word_remote(
                        request_queue_line,
                        notify_make(request_offset,
                                    client_id,
                                    request_payload_len)
                    );
                    publish_state.data_tail =
                        request_offset + payload_span_u64(request_payload_len);
                    publish_state.produce_seq = request_seq + 1;
                    store_request_publish_state_remote(
                        connection->request_publish_state,
                        &publish_state
                    );
                    debug_request_state_event("client",
                                              client_id,
                                              client_id,
                                              sent,
                                              "publish",
                                              request_seq,
                                              request_slot,
                                              request_offset,
                                              request_payload_len,
                                              0,
                                              0,
                                              0,
                                              &context);

                    if (!first_publish_marked) {
                        debug_mark("client",
                                   client_id,
                                   "first_publish",
                                   request_seq,
                                   request_offset);
                        first_publish_marked = 1;
                    }

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
            uint64_t response_seq = read_remote_u64(connection->response_consume_seq);
            uint64_t response_slot = response_seq % slot_count;
            volatile struct NotifyQueueLine *response_queue_line =
                notify_queue_line_at(connection->response_queue, response_slot);
            uint64_t response_notify = load_notify_word_remote(response_queue_line);
            uint64_t response_owner = UINT64_MAX;
            uint64_t response_len = 0;
            uint64_t response_offset = 0;

            if (notify_valid(response_notify)) {
                response_owner = notify_client_id(response_notify);
                response_len = notify_len(response_notify);
                response_offset = notify_offset(response_notify);
            }

            if (response_seq != last_response_trace_seq ||
                response_notify != last_response_trace_notify) {
                const char *stage = "resp_head_invalid";
                struct TraceContext context = {
                    .sent = sent,
                    .completed = completed,
                    .window_size = active_window_size,
                    .pending_start_ts_ns = pending_start,
                    .raw_word = response_notify,
                    .pending_valid = (uint8_t)pending_valid,
                };

                if (notify_valid(response_notify)) {
                    if (response_owner == client_id)
                        stage = "resp_head_self";
                    else
                        stage = "resp_head_other";
                }

                debug_response_head_state_event(client_id,
                                                completed,
                                                stage,
                                                response_seq,
                                                response_slot,
                                                response_owner,
                                                response_offset,
                                                response_len,
                                                &context);
                last_response_trace_seq = response_seq;
                last_response_trace_notify = response_notify;
            }

            if (notify_valid(response_notify) &&
                response_owner == client_id) {
                struct TraceContext context = {
                    .sent = sent,
                    .completed = completed,
                    .window_size = active_window_size,
                    .pending_start_ts_ns = pending_start,
                    .raw_word = response_notify,
                    .pending_valid = (uint8_t)pending_valid,
                };
                uint64_t response_seq_reread =
                    read_remote_u64(connection->response_consume_seq);

                if (response_seq_reread != response_seq) {
                    debug_response_head_state_event(client_id,
                                                    completed,
                                                    "resp_head_raced",
                                                    response_seq,
                                                    response_slot,
                                                    response_owner,
                                                    response_offset,
                                                    response_len,
                                                    &context);
                    continue;
                }

                debug_request_state_event("client",
                                          client_id,
                                          client_id,
                                          completed,
                                          "resp_seen",
                                          response_seq,
                                          response_slot,
                                          response_offset,
                                          response_len,
                                          0,
                                          0,
                                          0,
                                          &context);

                if (!first_resp_marked) {
                    debug_mark("client",
                               client_id,
                               "first_resp_seen",
                               response_seq,
                               response_offset);
                    first_resp_marked = 1;
                }

                {
                    uint64_t expected_response_len =
                        response_len_for_seq(
                            response_length_plan,
#if HYDRARPC_APP_VARIANT
                            app_runtime,
#endif
                            client_id,
                            completed,
                            client_request_count);
                    if (response_len != expected_response_len) {
                    fprintf(stderr,
                            "client %" PRIu64 " observed unexpected response length\n",
                            client_id);
                    record_control_error(control,
                                         FAIL_CLIENT_RESP_LEN,
                                         client_id,
                                         completed,
                                         response_len,
                                         expected_response_len,
                                         0,
                                         0);
                    } else {
                        uint64_t expected_request_len =
                            request_len_for_seq(
                                request_length_plan,
#if HYDRARPC_APP_VARIANT
                                app_runtime,
#endif
                                client_id,
                                completed,
                                client_request_count);
                        uint8_t *request_shadow =
                            request_shadow_at(request_shadow_local,
                                              completed,
                                              request_payload_stride);
                        uint64_t expected_head = 0;
                        uint64_t observed_head = 0;
                        uint64_t end = 0;
                        int fail = 0;

                    clear_notify_word_remote(response_queue_line);
                    write_remote_u64(connection->response_consume_seq,
                                     response_seq + 1);

                    load_payload_remote(response_local,
                                        payload_at_offset(connection->response_data_area,
                                                          response_offset),
                                        response_len);
                        fail =
#if HYDRARPC_APP_VARIANT
                            !client_response_matches(app_runtime,
                                                     client_id,
                                                     completed,
                                                     response_local,
                                                     response_len,
                                                     request_shadow,
                                                     expected_request_len);
#else
                            !client_response_matches(client_id,
                                                     completed,
                                                     response_local,
                                                     response_len,
                                                     request_shadow,
                                                     expected_request_len);
#endif
                        memcpy(&expected_head,
                               request_shadow,
                               sizeof(expected_head));
                        memcpy(&observed_head,
                               response_local,
                               sizeof(observed_head));
                        end = m5_rpns_ns();

                    store_shared_u64(
                        &timing->client_resp_done_ts_ns[base + completed],
                        end
                    );

                    if (fail) {
                        debug_request_state_event("client",
                                                  client_id,
                                                  client_id,
                                                  completed,
                                                  "resp_mismatch",
                                                  response_seq,
                                                  response_slot,
                                                  response_offset,
                                                  response_len,
                                                  expected_head,
                                                  observed_head,
                                                  0,
                                                  &context);
                        fprintf(stderr,
                                "client response payload mismatch client=%" PRIu64
                                " req=%" PRIu64
                                " response_seq=%" PRIu64
                                " notify_offset=%" PRIu64 "\n",
                                client_id,
                                completed,
                                response_seq,
                                response_offset);
                        record_control_error(control,
                                             FAIL_CLIENT_RESP_MISMATCH,
                                             client_id,
                                             completed,
                                             response_seq,
                                             response_offset,
                                             expected_head,
                                             observed_head);
                    } else {
                        debug_request_state_event("client",
                                                  client_id,
                                                  client_id,
                                                  completed,
                                                  "resp_done",
                                                  response_seq,
                                                  response_slot,
                                                  response_offset,
                                                  response_len,
                                                  0,
                                                  0,
                                                  0,
                                                  &context);
                        completed++;
                    }
                    }
                }
            }
        }

    }

    free(request_shadow_local);
    free(response_local);
    debug_mark("client", client_id, "exit", sent, completed);
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
                    "_client_req_publish_seq=%" PRIu64 "\n",
                    client_id,
                    req_id,
                    timing->client_req_publish_seq[index]);
            fprintf(stream,
                    "client_%" PRIu64 "_req_%" PRIu64
                    "_client_req_publish_offset=%" PRIu64 "\n",
                    client_id,
                    req_id,
                    timing->client_req_publish_offset[index]);
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
                      struct SharedControl *control,
                      uint64_t client_count,
                      uint64_t request_count,
                      uint64_t slow_client_count,
                      uint64_t slow_request_count,
                      struct RequestTimingData *timing)
{
    fprintf(stream, "benchmark_rc=%d\n", rc);
    fprintf(stream, "guest_command_rc=%d\n", rc);
    fprintf(stream, "fail_code=%" PRIu64 "\n",
            __atomic_load_n((const uint64_t *)&control->fail_code,
                            __ATOMIC_RELAXED));
    fprintf(stream, "fail_client_id=%" PRIu64 "\n",
            __atomic_load_n((const uint64_t *)&control->fail_client_id,
                            __ATOMIC_RELAXED));
    fprintf(stream, "fail_req_id=%" PRIu64 "\n",
            __atomic_load_n((const uint64_t *)&control->fail_req_id,
                            __ATOMIC_RELAXED));
    fprintf(stream, "fail_aux0=%" PRIu64 "\n",
            __atomic_load_n((const uint64_t *)&control->fail_aux0,
                            __ATOMIC_RELAXED));
    fprintf(stream, "fail_aux1=%" PRIu64 "\n",
            __atomic_load_n((const uint64_t *)&control->fail_aux1,
                            __ATOMIC_RELAXED));
    fprintf(stream, "fail_aux2=%" PRIu64 "\n",
            __atomic_load_n((const uint64_t *)&control->fail_aux2,
                            __ATOMIC_RELAXED));
    fprintf(stream, "fail_aux3=%" PRIu64 "\n",
            __atomic_load_n((const uint64_t *)&control->fail_aux3,
                            __ATOMIC_RELAXED));
    write_results(stream,
                  client_count,
                  request_count,
                  slow_client_count,
                  slow_request_count,
                  timing);
}

static int
write_host_buffer(const char *host_filename, const char *buffer, size_t buffer_len)
{
    enum { WRITEFILE_CHUNK_BYTES = 256 * 1024 };
    size_t offset = 0;
    int failed = 0;

    while (offset < buffer_len) {
        size_t chunk_len = buffer_len - offset;

        if (chunk_len > WRITEFILE_CHUNK_BYTES)
            chunk_len = WRITEFILE_CHUNK_BYTES;

        if (m5_write_file((void *)(buffer + offset),
                          chunk_len,
                          offset,
                          host_filename) != chunk_len) {
            fprintf(stderr,
                    "m5_write_file short write at offset=%zu chunk=%zu\n",
                    offset,
                    chunk_len);
            failed = 1;
            break;
        }

        offset += chunk_len;
    }

    return failed;
}

static void
write_trace_log(FILE *stream, const struct TraceBuffer *trace_buffer)
{
    uint64_t write_index = 0;
    uint64_t capacity = 0;
    uint64_t limit = 0;

    if (trace_buffer == NULL)
        return;

    write_index = __atomic_load_n(&trace_buffer->write_index, __ATOMIC_ACQUIRE);
    capacity = trace_buffer->capacity;
    limit = write_index < capacity ? write_index : capacity;

    fprintf(stream, "trace_capacity=%" PRIu64 "\n", capacity);
    fprintf(stream, "trace_write_index=%" PRIu64 "\n", write_index);
    fprintf(stream,
            "trace_dropped_events=%" PRIu64 "\n",
            __atomic_load_n(&trace_buffer->dropped_events, __ATOMIC_ACQUIRE));

    for (uint64_t i = 0; i < limit; i++) {
        const struct TraceEvent *event = &trace_buffer->events[i];

        if (__atomic_load_n(&event->committed, __ATOMIC_ACQUIRE) == 0)
            continue;

        fprintf(stream,
                "TRACE idx=%" PRIu64
                " kind=%s role=%s stage=%s"
                " ts=%" PRIu64
                " actor=%" PRIu64
                " client=%" PRIu64
                " req=%" PRIu64
                " seq=%" PRIu64
                " slot=%" PRIu64
                " offset=%" PRIu64
                " len=%" PRIu64
                " aux0=%" PRIu64
                " aux1=%" PRIu64
                " aux2=%" PRIu64
                " sent=%" PRIu64
                " completed=%" PRIu64
                " window=%" PRIu64
                " pending_valid=%u"
                " pending_start_ts_ns=%" PRIu64
                " raw=0x%016" PRIx64 "\n",
                i,
                event->kind,
                event->role,
                event->stage,
                event->ts_ns,
                event->actor_id,
                event->client_id,
                event->req_id,
                event->seq,
                event->slot,
                event->offset,
                event->len,
                event->aux0,
                event->aux1,
                event->aux2,
                event->sent,
                event->completed,
                event->window_size,
                (unsigned)event->pending_valid,
                event->pending_start_ts_ns,
                event->raw_word);
    }
}

static int
write_host_result_log(const char *host_filename,
                      int rc,
                      struct SharedControl *control,
                      uint64_t client_count,
                      uint64_t request_count,
                      uint64_t slow_client_count,
                      uint64_t slow_request_count,
                      struct RequestTimingData *timing)
{
    char *buffer = NULL;
    size_t buffer_len = 0;
    FILE *stream = open_memstream(&buffer, &buffer_len);
    int failed = 0;

    if (stream == NULL) {
        fprintf(stderr, "open_memstream failed: %s\n", strerror(errno));
        return 1;
    }

    print_results_with_rc(stream,
                          rc,
                          control,
                          client_count,
                          request_count,
                          slow_client_count,
                          slow_request_count,
                          timing);

    if (fclose(stream) != 0) {
        fprintf(stderr, "closing result memstream failed: %s\n", strerror(errno));
        free(buffer);
        return 1;
    }

    failed = write_host_buffer(host_filename, buffer, buffer_len);

    free(buffer);
    return failed;
}

static int
write_host_trace_log(const char *host_filename, const struct TraceBuffer *trace_buffer)
{
    char *buffer = NULL;
    size_t buffer_len = 0;
    FILE *stream = open_memstream(&buffer, &buffer_len);
    int failed = 0;

    if (stream == NULL) {
        fprintf(stderr, "open_memstream failed: %s\n", strerror(errno));
        return 1;
    }

    write_trace_log(stream, trace_buffer);

    if (fclose(stream) != 0) {
        fprintf(stderr, "closing trace memstream failed: %s\n", strerror(errno));
        free(buffer);
        return 1;
    }

    failed = write_host_buffer(host_filename, buffer, buffer_len);
    free(buffer);
    return failed;
}

int
main(int argc, char **argv)
{
    static const char result_log_name[] = HYDRARPC_VARIANT_NAME_STR ".result.log";
    static const char trace_log_name[] = HYDRARPC_VARIANT_NAME_STR ".trace.log";
    uint64_t client_count = 1;
    uint64_t request_count = 30;
    uint64_t window_size = 16;
    uint64_t slot_count = 0;
    uint64_t slow_client_count = 0;
    uint64_t slow_request_count = 0;
    uint64_t request_payload_size = DEFAULT_REQUEST_PAYLOAD_SIZE;
    uint64_t response_payload_size = DEFAULT_RESPONSE_PAYLOAD_SIZE;
    uint64_t request_payload_min_size = 0;
    uint64_t request_payload_max_size = 0;
    uint64_t response_payload_min_size = 0;
    uint64_t response_payload_max_size = 0;
    int request_payload_min_set = 0;
    int request_payload_max_set = 0;
    int response_payload_min_set = 0;
    int response_payload_max_set = 0;
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
    size_t request_publish_state_size = 0;
    size_t response_consume_seq_size = 0;
    size_t trace_buffer_size = 0;
    uint64_t trace_capacity = 0;
    struct PayloadLengthPlan request_length_plan = {0};
    struct PayloadLengthPlan response_length_plan = {0};
#if HYDRARPC_APP_VARIANT
    hydrarpc_app_profile_t app_profile;
    struct AppRuntime app_runtime = {0};
    uint64_t app_record_count = HYDRARPC_APP_DEFAULT_RECORD_COUNT;
    uint64_t app_dataset_seed = HYDRARPC_APP_DEFAULT_DATASET_SEED;
    uint64_t app_workload_seed = HYDRARPC_APP_DEFAULT_WORKLOAD_SEED;
    size_t app_key_size = 0;
    size_t app_value_size = 0;
    double app_read_ratio = 0.0;
    double app_update_ratio = 0.0;
    double app_rmw_ratio = 0.0;
    hydrarpc_app_key_dist_t app_key_dist = HYDRARPC_APP_KEY_DIST_ZIPF;
    double app_zipf_theta = 0.0;
    int app_key_size_overridden = 0;
    int app_value_size_overridden = 0;
    int app_variable_layout = 0;
#endif
    struct Connection connection = {0};
    struct SharedControl *control = NULL;
    struct RequestTimingData timing = {0};
    struct TraceBuffer *trace_buffer = NULL;
    pid_t *child_pids = NULL;
    uint64_t created_children = 0;
    int rc = 0;

#if HYDRARPC_APP_VARIANT
    if (hydrarpc_app_lookup_profile(HYDRARPC_APP_PROFILE_YCSB_C_1K,
                                    &app_profile) != 0) {
        fprintf(stderr, "failed to load default application profile\n");
        return 1;
    }
    app_key_size = app_profile.key_size;
    app_value_size = app_profile.value_size;
    app_read_ratio = app_profile.read_ratio;
    app_update_ratio = app_profile.update_ratio;
    app_rmw_ratio = app_profile.rmw_ratio;
    app_key_dist = app_profile.key_dist;
    app_zipf_theta = app_profile.zipf_theta;
#endif

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
        } else if (strcmp(argv[i], "--req-min-bytes") == 0 && i + 1 < argc) {
            request_payload_min_size = parse_u64("req-min-bytes", argv[++i]);
            request_payload_min_set = 1;
        } else if (strcmp(argv[i], "--req-max-bytes") == 0 && i + 1 < argc) {
            request_payload_max_size = parse_u64("req-max-bytes", argv[++i]);
            request_payload_max_set = 1;
        } else if (strcmp(argv[i], "--resp-min-bytes") == 0 && i + 1 < argc) {
            response_payload_min_size = parse_u64("resp-min-bytes", argv[++i]);
            response_payload_min_set = 1;
        } else if (strcmp(argv[i], "--resp-max-bytes") == 0 && i + 1 < argc) {
            response_payload_max_size = parse_u64("resp-max-bytes", argv[++i]);
            response_payload_max_set = 1;
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
        } else if (strcmp(argv[i], "--trace-requests") == 0) {
            g_trace_requests = 1;
#if HYDRARPC_APP_VARIANT
        } else if (strcmp(argv[i], "--profile") == 0 && i + 1 < argc) {
            if (hydrarpc_app_lookup_profile(argv[++i], &app_profile) != 0) {
                fprintf(stderr, "invalid profile: %s\n", argv[i]);
                return 2;
            }
            app_key_size = app_profile.key_size;
            app_value_size = app_profile.value_size;
            app_read_ratio = app_profile.read_ratio;
            app_update_ratio = app_profile.update_ratio;
            app_rmw_ratio = app_profile.rmw_ratio;
            app_key_dist = app_profile.key_dist;
            app_zipf_theta = app_profile.zipf_theta;
        } else if (strcmp(argv[i], "--record-count") == 0 && i + 1 < argc) {
            app_record_count = parse_u64("record-count", argv[++i]);
        } else if (strcmp(argv[i], "--dataset-seed") == 0 && i + 1 < argc) {
            app_dataset_seed = parse_u64("dataset-seed", argv[++i]);
        } else if (strcmp(argv[i], "--workload-seed") == 0 && i + 1 < argc) {
            app_workload_seed = parse_u64("workload-seed", argv[++i]);
        } else if (strcmp(argv[i], "--key-size") == 0 && i + 1 < argc) {
            app_key_size = (size_t)parse_u64("key-size", argv[++i]);
            app_key_size_overridden = 1;
        } else if (strcmp(argv[i], "--value-size") == 0 && i + 1 < argc) {
            app_value_size = (size_t)parse_u64("value-size", argv[++i]);
            app_value_size_overridden = 1;
        } else if (strcmp(argv[i], "--read-ratio") == 0 && i + 1 < argc) {
            app_read_ratio = parse_double("read-ratio", argv[++i]);
        } else if (strcmp(argv[i], "--update-ratio") == 0 && i + 1 < argc) {
            app_update_ratio = parse_double("update-ratio", argv[++i]);
        } else if (strcmp(argv[i], "--rmw-ratio") == 0 && i + 1 < argc) {
            app_rmw_ratio = parse_double("rmw-ratio", argv[++i]);
        } else if (strcmp(argv[i], "--zipf-theta") == 0 && i + 1 < argc) {
            app_zipf_theta = parse_double("zipf-theta", argv[++i]);
#endif
        } else if (strcmp(argv[i], "--help") == 0 ||
                   strcmp(argv[i], "-h") == 0) {
            printf("Usage: %s [--client-count N] [--count-per-client N] "
                   "[--window-size N] [--slot-count N] "
                   "[--req-bytes N] [--resp-bytes N] "
                   "[--req-min-bytes N] [--req-max-bytes N] "
                   "[--resp-min-bytes N] [--resp-max-bytes N] "
                   "[--slow-client-count N] [--slow-count-per-client N] "
                   "[--slow-send-gap-ns N] [--cxl-node N] "
                   "[--send-mode greedy|uniform|staggered|uneven] "
                   "[--send-gap-ns N] [--server-cpu N] "
                   "[--trace-requests]\n"
                   "  slot-count is shared request/response notify-ring depth; "
                   "default is client-count * min(window-size, count-per-client)\n"
                   "  req-bytes/resp-bytes set the request/response payload sizes; "
                   "defaults are %" PRIu64 "/%" PRIu64 " bytes\n"
                   "  req/resp-min-bytes and req/resp-max-bytes optionally enable "
                   "deterministic per-request uniform bins across each client's "
                   "request sequence\n"
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
                "clflushopt is required for hydrarpc_shared\n");
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

#if HYDRARPC_APP_VARIANT
    if (app_key_size == 0 || app_key_size > HYDRARPC_APP_MAX_KEY_SIZE) {
        fprintf(stderr, "key-size must be in [1, %u]\n",
                (unsigned)HYDRARPC_APP_MAX_KEY_SIZE);
        rc = 2;
        goto cleanup;
    }
    if (app_value_size == 0 || app_value_size > NOTIFY_LEN_MAX) {
        fprintf(stderr, "value-size must be in [1, %u]\n",
                (unsigned)NOTIFY_LEN_MAX);
        rc = 2;
        goto cleanup;
    }
    if (app_record_count == 0 || app_dataset_seed == 0 || app_workload_seed == 0) {
        fprintf(stderr, "record-count/dataset-seed/workload-seed must be positive\n");
        rc = 2;
        goto cleanup;
    }
    if (app_read_ratio < 0.0 || app_update_ratio < 0.0 ||
        app_rmw_ratio < 0.0 ||
        (app_key_dist == HYDRARPC_APP_KEY_DIST_ZIPF &&
         app_zipf_theta < 0.01) ||
        fabs((app_read_ratio + app_update_ratio + app_rmw_ratio) - 1.0) > 1e-6) {
        fprintf(stderr,
                "read-ratio + update-ratio + rmw-ratio must equal 1.0 and zipf-theta must be >= 0.01 for zipf workloads\n");
        rc = 2;
        goto cleanup;
    }

    app_variable_layout =
        hydrarpc_app_profile_has_variable_layout(app_profile.name) &&
        !app_key_size_overridden &&
        !app_value_size_overridden;
    if (init_app_runtime(&app_runtime,
                         client_count,
                         request_count,
                         slow_client_count,
                         slow_request_count,
                         &app_profile,
                         app_key_size,
                         app_value_size,
                         app_variable_layout,
                         app_record_count,
                         app_read_ratio,
                         app_update_ratio,
                         app_rmw_ratio,
                         app_key_dist,
                         app_zipf_theta,
                         app_dataset_seed,
                         app_workload_seed) != 0) {
        fprintf(stderr, "failed to initialize application runtime\n");
        rc = 1;
        goto cleanup;
    }

    request_length_plan.min_size = app_runtime.max_request_size;
    request_length_plan.max_size = app_runtime.max_request_size;
    response_length_plan.min_size = app_runtime.max_response_size;
    response_length_plan.max_size = app_runtime.max_response_size;
    if (request_length_plan.max_size > NOTIFY_LEN_MAX ||
        response_length_plan.max_size > NOTIFY_LEN_MAX) {
        fprintf(stderr,
                "application request/response payload exceeds notify length limit (%u)\n",
                (unsigned)NOTIFY_LEN_MAX);
        rc = 2;
        goto cleanup;
    }
#else
    if (!request_payload_min_set)
        request_payload_min_size = request_payload_size;
    if (!request_payload_max_set)
        request_payload_max_size = request_payload_size;
    if (!response_payload_min_set)
        response_payload_min_size = response_payload_size;
    if (!response_payload_max_set)
        response_payload_max_size = response_payload_size;

    request_length_plan.min_size = request_payload_min_size;
    request_length_plan.max_size = request_payload_max_size;
    response_length_plan.min_size = response_payload_min_size;
    response_length_plan.max_size = response_payload_max_size;

    if (request_length_plan.min_size == 0 ||
        request_length_plan.min_size > NOTIFY_LEN_MAX ||
        request_length_plan.max_size == 0 ||
        request_length_plan.max_size > NOTIFY_LEN_MAX) {
        fprintf(stderr,
                "request payload sizes must be in [1, %u]\n",
                (unsigned)NOTIFY_LEN_MAX);
        rc = 2;
        goto cleanup;
    }

    if (request_length_plan.min_size > request_length_plan.max_size) {
        fprintf(stderr,
                "req-min-bytes must be <= req-max-bytes\n");
        rc = 2;
        goto cleanup;
    }

    if (response_length_plan.min_size == 0 ||
        response_length_plan.min_size > NOTIFY_LEN_MAX ||
        response_length_plan.max_size == 0 ||
        response_length_plan.max_size > NOTIFY_LEN_MAX) {
        fprintf(stderr,
                "response payload sizes must be in [1, %u]\n",
                (unsigned)NOTIFY_LEN_MAX);
        rc = 2;
        goto cleanup;
    }

    if (response_length_plan.min_size > response_length_plan.max_size) {
        fprintf(stderr,
                "resp-min-bytes must be <= resp-max-bytes\n");
        rc = 2;
        goto cleanup;
    }
#endif

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

    request_span = payload_span_u64(payload_length_plan_stride(&request_length_plan));
    response_span = payload_span_u64(payload_length_plan_stride(&response_length_plan));
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

    connection.request_queue = alloc_shared_cxl(request_queue_size, cxl_node);
    connection.response_queue = alloc_shared_cxl(response_queue_size, cxl_node);
    connection.request_data_area = alloc_shared_cxl(request_data_size, cxl_node);
    connection.response_data_area = alloc_shared_cxl(response_data_size, cxl_node);
    if (request_lock_nodes_size > 0) {
        connection.request_lock_nodes =
            alloc_shared_cxl(request_lock_nodes_size, cxl_node);
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
    {
        struct RequestPublishState zero_publish_state = {0};

        store_request_publish_state_remote(connection.request_publish_state,
                                           &zero_publish_state);
    }
    write_remote_u64(connection.response_consume_seq, 0);

    child_pids = calloc((size_t)(client_count + 1), sizeof(*child_pids));
    control = alloc_shared_zeroed(sizeof(*control));
    timing.client_req_publish_seq = alloc_shared_zeroed(
        sizeof(*timing.client_req_publish_seq) * total_requests
    );
    timing.client_req_publish_offset = alloc_shared_zeroed(
        sizeof(*timing.client_req_publish_offset) * total_requests
    );
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
    if (g_trace_requests) {
        trace_capacity = trace_event_capacity(total_request_target, client_count);
        trace_buffer_size = trace_buffer_size_bytes(trace_capacity);
        trace_buffer = alloc_shared_zeroed(trace_buffer_size);
        trace_buffer->capacity = trace_capacity;
        g_trace_buffer = trace_buffer;
    }

    if (child_pids == NULL) {
        fprintf(stderr, "allocation failed for child pids\n");
        rc = 1;
        goto cleanup;
    }

    debug_mark("parent", 0, "main_enter", client_count, request_count);

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
                                             &request_length_plan,
                                             &response_length_plan,
#if HYDRARPC_APP_VARIANT
                                             &app_runtime,
#endif
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
            record_control_error(control,
                                 FAIL_FORK_CLIENT,
                                 client_id,
                                 0,
                                 0,
                                 0,
                                 0,
                                 0);
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
                                             &request_length_plan,
                                             &response_length_plan,
                                             send_mode,
                                             send_gap_ns,
                                             slow_send_gap_ns,
#if HYDRARPC_APP_VARIANT
                                             &app_runtime,
#endif
                                             &timing,
                                             control);
            _exit(child_rc == 0 ? 0 : 1);
        }
        child_pids[created_children++] = pid;
    }

    debug_mark("parent", 0, "forks_done", created_children, client_count + 1);
    {
        uint64_t last_ready_count = UINT64_MAX;

        while (!control_has_error(control) &&
               __atomic_load_n((const uint64_t *)&control->ready_count,
                               __ATOMIC_ACQUIRE) < client_count + 1) {
            uint64_t ready_count =
                __atomic_load_n((const uint64_t *)&control->ready_count,
                                __ATOMIC_ACQUIRE);

            if (ready_count != last_ready_count) {
                debug_mark("parent",
                           0,
                           "ready_progress",
                           ready_count,
                           client_count + 1);
                last_ready_count = ready_count;
            }
        }
    }

    if (control_has_error(control)) {
        debug_mark("parent", 0, "ready_error", control->fail_code, control->rc);
        rc = 1;
        kill_children(child_pids, created_children);
        goto wait_children;
    }

    __atomic_store_n((uint64_t *)&control->start_flag, 1, __ATOMIC_RELEASE);
    debug_mark("parent", 0, "start_release", created_children, 0);

wait_children:
    {
        uint64_t remaining = created_children;
        int wait_nonblock = g_trace_requests && g_trace_buffer != NULL;
        int wait_killed = 0;
        uint64_t last_trace_index =
            wait_nonblock
                ? __atomic_load_n(&g_trace_buffer->write_index, __ATOMIC_ACQUIRE)
                : 0;
        uint64_t last_progress_ts_ns = m5_rpns_ns();

        while (remaining > 0) {
            int status = 0;
            pid_t pid = waitpid(-1, &status, wait_nonblock ? WNOHANG : 0);

            if (pid == 0) {
                uint64_t trace_index = 0;
                uint64_t now_ns = m5_rpns_ns();

                if (control_has_error(control) && !wait_killed) {
                    debug_mark("parent",
                               0,
                               "wait_error_kill",
                               control->fail_code,
                               control->fail_client_id);
                    rc = 1;
                    wait_killed = 1;
                    kill_children(child_pids, created_children);
                    last_progress_ts_ns = now_ns;
                    continue;
                }

                trace_index =
                    __atomic_load_n(&g_trace_buffer->write_index, __ATOMIC_ACQUIRE);
                if (trace_index != last_trace_index) {
                    last_trace_index = trace_index;
                    last_progress_ts_ns = now_ns;
                } else if (!wait_killed &&
                           now_ns >= last_progress_ts_ns &&
                           (now_ns - last_progress_ts_ns) >=
                               TRACE_STALL_TIMEOUT_NS) {
                    debug_mark("parent",
                               0,
                               "wait_stall",
                               remaining,
                               trace_index);
                    record_control_error(control,
                                         FAIL_TRACE_STALL,
                                         0,
                                         0,
                                         remaining,
                                         trace_index,
                                         now_ns - last_progress_ts_ns,
                                         0);
                    rc = 1;
                    wait_killed = 1;
                    kill_children(child_pids, created_children);
                }

                sched_yield();
                continue;
            }

            if (pid < 0) {
                if (errno == EINTR)
                    continue;

                fprintf(stderr, "waitpid failed: %s\n", strerror(errno));
                rc = 1;
                kill_children(child_pids, created_children);
                break;
            }

            remaining--;
            if (wait_nonblock) {
                last_trace_index =
                    __atomic_load_n(&g_trace_buffer->write_index, __ATOMIC_ACQUIRE);
                last_progress_ts_ns = m5_rpns_ns();
            }

            mark_child_reaped(child_pids, created_children, pid);
            debug_mark("parent", 0, "child_reaped", (uint64_t)pid, (uint64_t)status);
            if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
                if (WIFSIGNALED(status)) {
                    fprintf(stderr, "child %ld killed by signal %d\n",
                            (long)pid,
                            WTERMSIG(status));
                } else {
                    fprintf(stderr, "child %ld exited with failure\n", (long)pid);
                }
                rc = 1;
                record_control_error(control,
                                     FAIL_WAIT_CHILD,
                                     0,
                                     0,
                                     pid,
                                     status,
                                     0,
                                     0);
                kill_children(child_pids, created_children);
                wait_killed = 1;
            }
        }
    }

    if (control_has_error(control))
        rc = 1;

    if (g_trace_requests &&
        g_trace_buffer != NULL &&
        write_host_trace_log(trace_log_name, g_trace_buffer) != 0) {
        fprintf(stderr, "failed to write %s\n", trace_log_name);
        rc = 1;
    }

    if (write_host_result_log(result_log_name,
                              rc,
                              control,
                              client_count,
                              request_count,
                              slow_client_count,
                              slow_request_count,
                              &timing) != 0) {
        fprintf(stderr, "failed to write %s\n", result_log_name);
        rc = 1;
    }

cleanup:
#if HYDRARPC_APP_VARIANT
    free_app_runtime(&app_runtime, client_count);
#endif
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
    if (connection.request_publish_state != NULL) {
        munmap((void *)connection.request_publish_state,
               shared_mapping_size(request_publish_state_size));
    }
    if (connection.response_consume_seq != NULL) {
        munmap((void *)connection.response_consume_seq,
               shared_mapping_size(response_consume_seq_size));
    }
    if (trace_buffer != NULL) {
        munmap((void *)trace_buffer, shared_mapping_size(trace_buffer_size));
    }
    g_trace_buffer = NULL;

    if (control != NULL)
        munmap((void *)control, shared_mapping_size(sizeof(*control)));
    if (timing.client_req_publish_seq != NULL) {
        munmap((void *)timing.client_req_publish_seq,
               shared_mapping_size(sizeof(*timing.client_req_publish_seq) *
                                   total_requests));
    }
    if (timing.client_req_publish_offset != NULL) {
        munmap((void *)timing.client_req_publish_offset,
               shared_mapping_size(sizeof(*timing.client_req_publish_offset) *
                                   total_requests));
    }
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
