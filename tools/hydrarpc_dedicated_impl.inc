#if !defined(HYDRARPC_VARIANT_NAME)
#error "HYDRARPC_VARIANT_NAME must be defined before including hydrarpc_dedicated_impl.inc"
#endif

#if !defined(HYDRARPC_COHERENT)
#error "HYDRARPC_COHERENT must be defined before including hydrarpc_dedicated_impl.inc"
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
#define DEFAULT_REQUEST_PAYLOAD_SIZE 64u
#define DEFAULT_RESPONSE_PAYLOAD_SIZE 64u

#define NOTIFY_LEN_BITS 14u
#define NOTIFY_LEN_MAX (1u << NOTIFY_LEN_BITS)
#define NOTIFY_OFFSET_BITS (64u - 1u - NOTIFY_LEN_BITS)
#define NOTIFY_OFFSET_MASK ((1ull << NOTIFY_OFFSET_BITS) - 1ull)
#define NOTIFY_LEN_MASK ((1ull << NOTIFY_LEN_BITS) - 1ull)
#define NOTIFY_LEN_SHIFT NOTIFY_OFFSET_BITS
#define NOTIFY_VALID_SHIFT 63u
#define NOTIFY_VALID_MASK (1ull << NOTIFY_VALID_SHIFT)

struct NotifyQueueLine {
    volatile uint64_t word;
    uint8_t padding[CACHELINE_SIZE - sizeof(uint64_t)];
} __attribute__((aligned(CACHELINE_SIZE)));

struct Connection {
    volatile struct NotifyQueueLine *request_queue;
    volatile struct NotifyQueueLine *response_queue;
    uint8_t *request_data_area;
    uint8_t *response_data_area;
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

enum TransferMode {
    TRANSFER_STAGING = 0,
    TRANSFER_DIRECT,
};

_Static_assert(sizeof(struct NotifyQueueLine) == CACHELINE_SIZE,
               "NotifyQueueLine must occupy exactly one cacheline");

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

#if !HYDRARPC_COHERENT
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
#endif

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

static enum TransferMode
parse_transfer_mode(const char *value)
{
    if (strcmp(value, "staging") == 0)
        return TRANSFER_STAGING;
    if (strcmp(value, "direct") == 0)
        return TRANSFER_DIRECT;

    fprintf(stderr, "invalid transfer-mode: %s\n", value);
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
notify_make(uint64_t offset, uint64_t len)
{
    return NOTIFY_VALID_MASK |
           (((len - 1u) & NOTIFY_LEN_MASK) << NOTIFY_LEN_SHIFT) |
           (offset & NOTIFY_OFFSET_MASK);
}

static inline int
notify_valid(uint64_t word)
{
    return (word & NOTIFY_VALID_MASK) != 0;
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

#if HYDRARPC_COHERENT
static inline void
store_payload_remote(void *dst, const void *src, uint64_t payload_len)
{
    memcpy(dst, src, payload_len);
}

static inline void
load_payload_remote(void *dst, const void *src, uint64_t payload_len)
{
    memcpy(dst, src, payload_len);
}
#else
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

static inline void
flush_payload_remote_stores(const void *ptr, uint64_t payload_len)
{
    flush_remote_range(ptr, payload_len);
    _mm_sfence();
}

static inline void
store_payload_remote(void *dst, const void *src, uint64_t payload_len)
{
    copy_bytes_to_remote(dst, src, payload_len);
    flush_payload_remote_stores(dst, payload_len);
}

static inline void
load_payload_remote(void *dst, const void *src, uint64_t payload_len)
{
    flush_remote_range(src, payload_len);
    _mm_mfence();
    copy_bytes_from_remote(dst, src, payload_len);
}
#endif

static inline uint64_t
load_notify_word_remote(volatile struct NotifyQueueLine *queue_line)
{
#if HYDRARPC_COHERENT
    return __atomic_load_n((const uint64_t *)&queue_line->word,
                           __ATOMIC_ACQUIRE);
#else
    clflushopt_line((const void *)queue_line);
    _mm_mfence();
    return __atomic_load_n((const uint64_t *)&queue_line->word,
                           __ATOMIC_RELAXED);
#endif
}

static inline void
write_notify_word_remote(volatile struct NotifyQueueLine *queue_line,
                         uint64_t notify_word)
{
#if HYDRARPC_COHERENT
    __atomic_store_n((uint64_t *)&queue_line->word, notify_word, __ATOMIC_RELEASE);
#else
    __atomic_store_n((uint64_t *)&queue_line->word, notify_word, __ATOMIC_RELAXED);
    clflushopt_line((const void *)queue_line);
    _mm_sfence();
#endif
}

static inline void
clear_notify_word_remote(volatile struct NotifyQueueLine *queue_line)
{
#if HYDRARPC_COHERENT
    __atomic_store_n((uint64_t *)&queue_line->word, 0, __ATOMIC_RELEASE);
#else
    __atomic_store_n((uint64_t *)&queue_line->word, 0, __ATOMIC_RELAXED);
    clflushopt_line((const void *)queue_line);
    _mm_sfence();
#endif
}

static void
init_notify_ring(volatile struct NotifyQueueLine *queue, uint64_t slot_count)
{
    for (uint64_t slot = 0; slot < slot_count; slot++)
        clear_notify_word_remote(notify_queue_line_at(queue, slot));
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

    if (op == NULL)
        return 0;

    (void)plan;
    (void)client_request_count;
    return op->request_len;
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

    if (op == NULL)
        return 0;

    (void)plan;
    (void)client_request_count;
    return op->response_len;
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

static void
signal_ready_and_wait(struct SharedControl *control)
{
    __atomic_fetch_add(&control->ready_count, 1, __ATOMIC_RELEASE);
    wait_for_start(&control->start_flag);
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
fill_random_payload_remote_and_shadow(uint8_t *remote_dst,
                                      uint8_t *shadow_dst,
                                      uint64_t payload_len,
                                      uint64_t *rng_state)
{
    uint64_t offset = 0;

    while (offset < payload_len) {
        uint64_t word = next_rand(rng_state);
        uint64_t chunk_len = min_u64(payload_len - offset, sizeof(word));

        memcpy(remote_dst + offset, &word, (size_t)chunk_len);
        memcpy(shadow_dst + offset, &word, (size_t)chunk_len);
        offset += chunk_len;
    }

#if !HYDRARPC_COHERENT
    flush_payload_remote_stores(remote_dst, payload_len);
#endif
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
store_response_payload_direct(uint8_t *remote_dst,
                              uint64_t response_payload_size,
                              const uint8_t *request_bytes,
                              uint64_t request_payload_size)
{
    uint64_t copy_len = min_u64(response_payload_size, request_payload_size);

    memcpy(remote_dst, request_bytes, (size_t)copy_len);
    if (response_payload_size > copy_len) {
        memset(remote_dst + copy_len,
               0,
               (size_t)(response_payload_size - copy_len));
    }

#if !HYDRARPC_COHERENT
    flush_payload_remote_stores(remote_dst, response_payload_size);
#endif
}

static void
store_request_payload(uint8_t *remote_dst,
                      uint8_t *request_shadow,
                      uint64_t request_payload_size,
                      uint64_t *rng_state,
                      enum TransferMode transfer_mode)
{
    if (transfer_mode == TRANSFER_STAGING) {
        fill_random_payload_bytes(request_shadow, request_payload_size, rng_state);
        store_payload_remote(remote_dst, request_shadow, request_payload_size);
        return;
    }

    fill_random_payload_remote_and_shadow(remote_dst,
                                          request_shadow,
                                          request_payload_size,
                                          rng_state);
}

static void
store_response_payload(uint8_t *remote_dst,
                       uint64_t response_payload_size,
                       const uint8_t *request_bytes,
                       uint64_t request_payload_size,
                       enum TransferMode transfer_mode,
                       uint8_t *response_staging)
{
    if (transfer_mode == TRANSFER_STAGING) {
        build_response_payload_bytes(response_staging,
                                     response_payload_size,
                                     request_bytes,
                                     request_payload_size);
        store_payload_remote(remote_dst, response_staging, response_payload_size);
        return;
    }

    store_response_payload_direct(remote_dst,
                                  response_payload_size,
                                  request_bytes,
                                  request_payload_size);
}

#if HYDRARPC_APP_VARIANT
static int
build_client_request_payload(const struct AppRuntime *app_runtime,
                             uint64_t client_id,
                             uint64_t req_id,
                             uint8_t *remote_dst,
                             uint8_t *request_shadow,
                             uint64_t request_payload_size,
                             uint64_t *rng_state,
                             enum TransferMode transfer_mode)
{
    const hydrarpc_app_operation_t *op =
        app_operation_for_seq(app_runtime, client_id, req_id);
    size_t built_len = 0;

    (void)rng_state;
    if (transfer_mode != TRANSFER_STAGING || op == NULL)
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
                              uint64_t response_payload_stride,
                              const uint8_t *request_bytes,
                              uint64_t request_payload_size,
                              enum TransferMode transfer_mode,
                              uint8_t *response_staging,
                              uint64_t *response_payload_size_out)
{
    size_t response_len = 0;

    (void)remote_dst;
    if (transfer_mode != TRANSFER_STAGING || response_staging == NULL ||
        response_payload_size_out == NULL) {
        return -1;
    }

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
                             uint64_t *rng_state,
                             enum TransferMode transfer_mode)
{
    store_request_payload(remote_dst,
                          request_shadow,
                          request_payload_size,
                          rng_state,
                          transfer_mode);
    return 0;
}

static int
build_server_response_payload(uint8_t *remote_dst,
                              uint64_t response_payload_size,
                              const uint8_t *request_bytes,
                              uint64_t request_payload_size,
                              enum TransferMode transfer_mode,
                              uint8_t *response_staging,
                              uint64_t *response_payload_size_out)
{
    store_response_payload(remote_dst,
                           response_payload_size,
                           request_bytes,
                           request_payload_size,
                           transfer_mode,
                           response_staging);
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

static int
run_server_dedicated(struct Connection *connections,
                     uint64_t client_count,
                     uint64_t slot_count,
                     uint64_t request_count,
                     uint64_t slow_client_count,
                     uint64_t slow_request_count,
                     uint64_t total_request_target,
                     const struct PayloadLengthPlan *request_length_plan,
                     const struct PayloadLengthPlan *response_length_plan,
                     enum TransferMode response_transfer_mode,
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
    uint64_t *next_seq = calloc((size_t)client_count, sizeof(*next_seq));
    uint64_t *response_data_tail =
        calloc((size_t)client_count, sizeof(*response_data_tail));
    uint64_t completed_total = 0;
    uint64_t next_client_hint = 0;
    uint8_t *request_payload_local = malloc((size_t)request_payload_stride);
    uint8_t *response_staging = NULL;

    if (response_transfer_mode == TRANSFER_STAGING)
        response_staging = malloc((size_t)response_payload_stride);

    if (next_seq == NULL || response_data_tail == NULL ||
        request_payload_local == NULL ||
        (response_transfer_mode == TRANSFER_STAGING && response_staging == NULL)) {
        fprintf(stderr, "server tracking allocation failed\n");
        set_control_error(control);
        free(next_seq);
        free(response_data_tail);
        free(request_payload_local);
        free(response_staging);
        return 1;
    }

    if (pin_to_cpu(server_cpu) != 0) {
        fprintf(stderr, "failed to pin server to cpu %d\n", server_cpu);
        set_control_error(control);
        free(next_seq);
        free(response_data_tail);
        free(request_payload_local);
        free(response_staging);
        return 1;
    }

    signal_ready_and_wait(control);
    store_shared_u64(timing->server_loop_start_ts_ns, m5_rpns_ns());

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
            uint64_t request_notify = 0;
            uint64_t request_len = 0;
            uint64_t request_offset = 0;
            uint64_t response_offset = 0;
            uint64_t expected_request_len = 0;
            uint64_t expected_response_len = 0;
            uint64_t server_req_observe_ts = 0;
            uint64_t server_exec_done_ts = 0;
            uint64_t server_resp_done_ts = 0;
            size_t index = 0;
            volatile struct NotifyQueueLine *request_queue_line = NULL;
            volatile struct NotifyQueueLine *response_queue_line = NULL;

            if (seq >= client_request_count)
                continue;

            expected_request_len =
#if HYDRARPC_APP_VARIANT
                request_len_for_seq(request_length_plan,
                                    app_runtime,
                                    client_id,
                                    seq,
                                    client_request_count);
            expected_response_len =
                response_len_for_seq(response_length_plan,
                                     app_runtime,
                                     client_id,
                                     seq,
                                     client_request_count);
#else
                request_len_for_seq(request_length_plan,
                                    client_id,
                                    seq,
                                    client_request_count);
            expected_response_len =
                response_len_for_seq(response_length_plan,
                                     client_id,
                                     seq,
                                     client_request_count);
#endif

            slot = seq % slot_count;
            index = request_result_index(client_id, seq, request_count);
            request_queue_line =
                notify_queue_line_at(connection->request_queue, slot);
            response_queue_line =
                notify_queue_line_at(connection->response_queue, slot);
            request_notify = load_notify_word_remote(request_queue_line);
            if (!notify_valid(request_notify))
                continue;

            request_len = notify_len(request_notify);
            if (request_len != expected_request_len) {
                fprintf(stderr, "server observed unexpected request length\n");
                set_control_error(control);
                break;
            }

            server_req_observe_ts = m5_rpns_ns();
            request_offset = notify_offset(request_notify);
            load_payload_remote(request_payload_local,
                                payload_at_offset(connection->request_data_area,
                                                  request_offset),
                                request_len);
            server_exec_done_ts = m5_rpns_ns();

            clear_notify_word_remote(request_queue_line);

            response_offset = response_data_tail[client_id];
            if (
#if HYDRARPC_APP_VARIANT
                build_server_response_payload(
                    app_runtime,
                    payload_at_offset(connection->response_data_area,
                                      response_offset),
                    response_payload_stride,
                    request_payload_local,
                    request_len,
                    response_transfer_mode,
                    response_staging,
                    &expected_response_len
                ) != 0
#else
                build_server_response_payload(
                    payload_at_offset(connection->response_data_area,
                                      response_offset),
                    expected_response_len,
                    request_payload_local,
                    request_len,
                    response_transfer_mode,
                    response_staging,
                    &expected_response_len
                ) != 0
#endif
            ) {
                fprintf(stderr, "server failed to build response payload\n");
                set_control_error(control);
                break;
            }
            write_notify_word_remote(response_queue_line,
                                     notify_make(response_offset,
                                                 expected_response_len));
            server_resp_done_ts = m5_rpns_ns();
            store_shared_u64(&timing->server_req_observe_ts_ns[index],
                             server_req_observe_ts);
            store_shared_u64(&timing->server_exec_done_ts_ns[index],
                             server_exec_done_ts);
            store_shared_u64(&timing->server_resp_done_ts_ns[index],
                             server_resp_done_ts);

            response_data_tail[client_id] +=
                payload_span_u64(expected_response_len);
            next_seq[client_id]++;
            completed_total++;
            next_client_hint = (client_id + 1) % client_count;
            made_progress = 1;
            break;
        }

        if (!made_progress) {
        }
    }

    free(next_seq);
    free(response_data_tail);
    free(request_payload_local);
    free(response_staging);
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
                     const struct PayloadLengthPlan *request_length_plan,
                     const struct PayloadLengthPlan *response_length_plan,
                     enum SendMode send_mode,
                     uint64_t send_gap_ns,
                     uint64_t slow_send_gap_ns,
                     enum TransferMode request_transfer_mode,
#if HYDRARPC_APP_VARIANT
                     const struct AppRuntime *app_runtime,
#endif
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
    uint64_t request_data_tail = 0;
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

    if (request_shadow_local == NULL || response_local == NULL) {
        fprintf(stderr, "client %" PRIu64 " local tracking allocation failed\n",
                client_id);
        set_control_error(control);
        free(request_shadow_local);
        free(response_local);
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
        int made_progress = 0;

        active_window_size = (completed == 0) ? 1 : window_size;

        if (completed > 0 && !steady_schedule_started) {
            send_schedule_base_ns = m5_rpns_ns();
            steady_schedule_started = 1;
        }

        if (sent < client_request_count &&
            (sent - completed) < active_window_size) {
            uint64_t slot = sent % slot_count;
            volatile struct NotifyQueueLine *request_queue_line =
                notify_queue_line_at(connection->request_queue, slot);

            if (send_is_due(effective_send_mode,
                            effective_send_gap_ns,
                            client_count,
                            client_id,
                            sent,
                            send_schedule_base_ns)) {
                uint64_t start = 0;
                uint64_t request_offset = request_data_tail;
                uint64_t request_payload_len =
#if HYDRARPC_APP_VARIANT
                    request_len_for_seq(request_length_plan,
                                        app_runtime,
                                        client_id,
                                        sent,
                                        client_request_count);
#else
                    request_len_for_seq(request_length_plan,
                                        client_id,
                                        sent,
                                        client_request_count);
#endif
                size_t index = base + sent;
                uint8_t *request_shadow =
                    request_shadow_at(request_shadow_local,
                                      sent,
                                      request_payload_stride);

                start = m5_rpns_ns();

                store_shared_u64(&timing->client_req_start_ts_ns[index], start);
                if (
#if HYDRARPC_APP_VARIANT
                    build_client_request_payload(
                        app_runtime,
                        client_id,
                        sent,
                        payload_at_offset(connection->request_data_area,
                                          request_offset),
                        request_shadow,
                        request_payload_len,
                        &rng_state,
                        request_transfer_mode
                    ) != 0
#else
                    build_client_request_payload(
                        payload_at_offset(connection->request_data_area,
                                          request_offset),
                        request_shadow,
                        request_payload_len,
                        &rng_state,
                        request_transfer_mode
                    ) != 0
#endif
                ) {
                    fprintf(stderr,
                            "client %" PRIu64 " failed to build request payload\n",
                            client_id);
                        set_control_error(control);
                        break;
                }
                write_notify_word_remote(request_queue_line,
                                         notify_make(request_offset,
                                                     request_payload_len));

                request_data_tail += payload_span_u64(request_payload_len);
                sent++;
                made_progress = 1;
            }
        }

        if (completed < sent) {
            uint64_t slot = completed % slot_count;
            volatile struct NotifyQueueLine *response_queue_line =
                notify_queue_line_at(connection->response_queue, slot);
                uint64_t response_notify = load_notify_word_remote(response_queue_line);

                if (notify_valid(response_notify)) {
                    uint64_t response_len = notify_len(response_notify);
                    uint64_t response_offset = notify_offset(response_notify);
                    uint64_t expected_request_len =
#if HYDRARPC_APP_VARIANT
                        request_len_for_seq(request_length_plan,
                                            app_runtime,
                                            client_id,
                                            completed,
                                            client_request_count);
                    uint64_t expected_response_len =
                        response_len_for_seq(response_length_plan,
                                             app_runtime,
                                             client_id,
                                             completed,
                                             client_request_count);
#else
                        request_len_for_seq(request_length_plan,
                                            client_id,
                                            completed,
                                            client_request_count);
                    uint64_t expected_response_len =
                        response_len_for_seq(response_length_plan,
                                             client_id,
                                             completed,
                                             client_request_count);
#endif
                    uint8_t *request_shadow =
                        request_shadow_at(request_shadow_local,
                                          completed,
                                          request_payload_stride);
                    uint64_t end = 0;

                    if (response_len != expected_response_len) {
                        fprintf(stderr,
                                "client %" PRIu64 " observed unexpected response length\n",
                                client_id);
                    set_control_error(control);
                    break;
                }

                load_payload_remote(
                    response_local,
                    payload_at_offset(connection->response_data_area,
                                      response_offset),
                    response_len
                    );
                    if (
#if HYDRARPC_APP_VARIANT
                        !client_response_matches(app_runtime,
                                                 client_id,
                                                 completed,
                                                 response_local,
                                                 response_len,
                                                 request_shadow,
                                                 expected_request_len)
#else
                        !client_response_matches(client_id,
                                                 completed,
                                                 response_local,
                                                 response_len,
                                                 request_shadow,
                                                 expected_request_len)
#endif
                    ) {
                        fprintf(stderr,
                                "client response payload mismatch client=%" PRIu64
                                " req=%" PRIu64 "\n",
                            client_id,
                            completed);
                    set_control_error(control);
                    break;
                }

                end = m5_rpns_ns();
                store_shared_u64(&timing->client_resp_done_ts_ns[base + completed],
                                 end);
                clear_notify_word_remote(response_queue_line);

                completed++;
                made_progress = 1;
            }
        }

        if (!made_progress) {
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
    fprintf(stream, "guest_command_rc=%d\n", rc);
    write_results(stream,
                  client_count,
                  request_count,
                  slow_client_count,
                  slow_request_count,
                  timing);
}

static int
write_host_result_log(const char *host_filename,
                      int rc,
                      uint64_t client_count,
                      uint64_t request_count,
                      uint64_t slow_client_count,
                      uint64_t slow_request_count,
                      struct RequestTimingData *timing)
{
    enum { WRITEFILE_CHUNK_BYTES = 256 * 1024 };
    char *buffer = NULL;
    size_t buffer_len = 0;
    FILE *stream = open_memstream(&buffer, &buffer_len);
    size_t offset = 0;
    int failed = 0;

    if (stream == NULL) {
        fprintf(stderr, "open_memstream failed: %s\n", strerror(errno));
        return 1;
    }

    print_results_with_rc(stream,
                          rc,
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

    while (offset < buffer_len) {
        size_t chunk_len = buffer_len - offset;

        if (chunk_len > WRITEFILE_CHUNK_BYTES)
            chunk_len = WRITEFILE_CHUNK_BYTES;

        if (m5_write_file(buffer + offset,
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

    free(buffer);
    return failed;
}

int
main(int argc, char **argv)
{
    static const char result_log_name[] = HYDRARPC_VARIANT_NAME ".result.log";
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
    enum TransferMode request_transfer_mode = TRANSFER_STAGING;
    enum TransferMode response_transfer_mode = TRANSFER_STAGING;
    long online_cpus = sysconf(_SC_NPROCESSORS_ONLN);
    uint64_t total_requests = 0;
    uint64_t total_request_target = 0;
    uint64_t request_span = 0;
    uint64_t response_span = 0;
    size_t request_queue_size = 0;
    size_t response_queue_size = 0;
    size_t request_data_size = 0;
    size_t response_data_size = 0;
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
    struct Connection *connections = NULL;
    struct SharedControl *control = NULL;
    struct RequestTimingData timing = {0};
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
        } else if (strcmp(argv[i], "--request-transfer-mode") == 0 &&
                   i + 1 < argc) {
            request_transfer_mode = parse_transfer_mode(argv[++i]);
        } else if (strcmp(argv[i], "--response-transfer-mode") == 0 &&
                   i + 1 < argc) {
            response_transfer_mode = parse_transfer_mode(argv[++i]);
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
                   "[--send-gap-ns N] "
                   "[--request-transfer-mode staging|direct] "
                   "[--response-transfer-mode staging|direct] "
                   "[--server-cpu N]\n"
                   "  window-size is max outstanding requests per client "
                   "after each client's first RPC completes; default is 16\n"
                   "  slot-count is per-client request/response notify-ring depth; "
                   "default is min(window-size, count-per-client)\n"
                   "  req-bytes/resp-bytes set the request/response payload sizes; "
                   "defaults are %" PRIu64 "/%" PRIu64 " bytes\n"
                   "  req/resp-min-bytes and req/resp-max-bytes optionally enable "
                   "deterministic per-request uniform bins across each client's "
                   "request sequence\n"
                   "  each client owns independent request/response data areas; "
                   "payload areas are append-only and payloads carry no header\n"
                   "  request-transfer-mode controls how clients publish request "
                   "payloads; response-transfer-mode controls how the server "
                   "publishes response payloads\n"
                   "  raw timing output always records client request-start / "
                   "response-done timestamps and server observe / exec-done / "
                   "response-done timestamps\n"
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

#if !HYDRARPC_COHERENT
    if (!cpu_has_clflushopt()) {
        fprintf(stderr,
                "clflushopt is required for %s\n",
                HYDRARPC_VARIANT_NAME);
        return 2;
    }
#endif

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
    if (request_transfer_mode != TRANSFER_STAGING ||
        response_transfer_mode != TRANSFER_STAGING) {
        fprintf(stderr,
                "application dedicated mode currently requires staging request/response transfers\n");
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
        slot_count = window_size < request_count ? window_size : request_count;
    }

    if (slot_count == 0) {
        fprintf(stderr, "slot-count must be positive after defaulting\n");
        rc = 2;
        goto cleanup;
    }
    if (slot_count < min_u64(window_size, request_count)) {
        fprintf(stderr,
                "current dedicated baseline requires slot-count (%" PRIu64
                ") >= min(window-size, count-per-client) (%" PRIu64
                ") because slot vacancy checks are disabled\n",
                slot_count,
                min_u64(window_size, request_count));
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
    request_span = payload_span_u64(payload_length_plan_stride(&request_length_plan));
    response_span = payload_span_u64(payload_length_plan_stride(&response_length_plan));
    request_queue_size = sizeof(struct NotifyQueueLine) * slot_count;
    response_queue_size = sizeof(struct NotifyQueueLine) * slot_count;
    request_data_size = (size_t)(request_count * request_span);
    response_data_size = (size_t)(request_count * response_span);

    if ((uint64_t)request_data_size > NOTIFY_OFFSET_MASK + 1u ||
        (uint64_t)response_data_size > NOTIFY_OFFSET_MASK + 1u) {
        fprintf(stderr,
                "dedicated payload area exceeds %" PRIu64 "-bit notify offset range\n",
                (uint64_t)NOTIFY_OFFSET_BITS);
        rc = 2;
        goto cleanup;
    }

    connections = calloc((size_t)client_count, sizeof(*connections));
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
        init_notify_ring(connections[client_id].request_queue, slot_count);
        init_notify_ring(connections[client_id].response_queue, slot_count);
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
                                                &request_length_plan,
                                                &response_length_plan,
                                                response_transfer_mode,
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
                                                &request_length_plan,
                                                &response_length_plan,
                                                send_mode,
                                                send_gap_ns,
                                                slow_send_gap_ns,
                                                request_transfer_mode,
#if HYDRARPC_APP_VARIANT
                                                &app_runtime,
#endif
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

    if (write_host_result_log(result_log_name,
                              rc,
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

    free(connections);
    free(child_pids);
    return rc;
}
