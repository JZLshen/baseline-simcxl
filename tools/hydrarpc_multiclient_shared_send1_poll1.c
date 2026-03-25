#define _GNU_SOURCE

#include <cpuid.h>
#include <errno.h>
#include <gem5/m5ops.h>
#include <inttypes.h>
#include <linux/mempolicy.h>
#include <sched.h>
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

struct RequestQueueLine {
    volatile uint64_t token;
    volatile uint64_t client_id;
    volatile uint64_t req_id;
    uint8_t padding[CACHELINE_SIZE - (3 * sizeof(uint64_t))];
} __attribute__((aligned(CACHELINE_SIZE)));

struct SharedLockSlot {
    volatile uint64_t choosing;
    volatile uint64_t number;
    uint8_t padding[CACHELINE_SIZE - (2 * sizeof(uint64_t))];
} __attribute__((aligned(CACHELINE_SIZE)));

struct Connection {
    volatile uint64_t *request_queue;
    volatile uint64_t *response_queue;
    uint8_t *request_data_area;
    uint8_t *response_data_area;
    struct SharedLockSlot *request_lock_slots;
    volatile uint64_t *request_produce_seq;
    uint64_t per_client_slot_count;
};

struct SharedControl {
    volatile uint64_t ready_count;
    volatile uint64_t start_flag;
    volatile int rc;
};

_Static_assert(sizeof(struct Request) <= CACHELINE_SIZE,
               "Request must fit within one cacheline-backed slot");
_Static_assert(sizeof(struct Response) <= CACHELINE_SIZE,
               "Response must fit within one cacheline-backed slot");
_Static_assert(sizeof(struct RequestLine) == CACHELINE_SIZE,
               "RequestLine must occupy exactly one cacheline");
_Static_assert(sizeof(struct ResponseLine) == CACHELINE_SIZE,
               "ResponseLine must occupy exactly one cacheline");
_Static_assert(sizeof(struct RequestQueueLine) == CACHELINE_SIZE,
               "RequestQueueLine must occupy exactly one cacheline");
_Static_assert(sizeof(struct SharedLockSlot) == CACHELINE_SIZE,
               "SharedLockSlot must occupy exactly one cacheline");

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

static inline volatile struct RequestQueueLine *
request_queue_line_at(volatile uint64_t *queue, uint64_t slot)
{
    return (volatile struct RequestQueueLine *)(void *)queue_entry_ptr(queue,
                                                                       slot);
}

static inline uint64_t
client_slot_index(struct Connection *connection,
                  uint64_t client_id,
                  uint64_t slot)
{
    return (client_id * connection->per_client_slot_count) + slot;
}

static inline struct RequestLine *
request_at_slot(struct Connection *connection, uint64_t client_id, uint64_t slot)
{
    return (struct RequestLine *)(connection->request_data_area +
                                  slot_offset(client_slot_index(connection,
                                                                client_id,
                                                                slot)));
}

static inline struct ResponseLine *
response_at_slot(struct Connection *connection, uint64_t client_id, uint64_t slot)
{
    return (struct ResponseLine *)(connection->response_data_area +
                                   slot_offset(client_slot_index(connection,
                                                                 client_id,
                                                                 slot)));
}

static inline volatile uint64_t *
response_queue_slot_ptr(struct Connection *connection,
                        uint64_t client_id,
                        uint64_t slot)
{
    return queue_entry_ptr(connection->response_queue,
                           client_slot_index(connection, client_id, slot));
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

static inline uint64_t
read_remote_u64(volatile uint64_t *ptr)
{
    clflushopt_line((const void *)ptr);
    _mm_mfence();
    return *ptr;
}

static inline void
write_remote_u64(volatile uint64_t *ptr, uint64_t value)
{
    *ptr = value;
    clflushopt_line((const void *)ptr);
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

static inline uint64_t
read_request_queue_token_remote(volatile struct RequestQueueLine *queue_line)
{
    clflushopt_line((const void *)queue_line);
    _mm_mfence();
    return __atomic_load_n((const uint64_t *)&queue_line->token,
                           __ATOMIC_RELAXED);
}

static inline void
write_request_queue_remote(volatile struct RequestQueueLine *queue_line,
                           uint64_t token,
                           uint64_t client_id,
                           uint64_t req_id)
{
    __atomic_store_n(&queue_line->client_id, client_id, __ATOMIC_RELAXED);
    __atomic_store_n(&queue_line->req_id, req_id, __ATOMIC_RELAXED);
    __atomic_store_n(&queue_line->token, token, __ATOMIC_RELAXED);
    clflushopt_line((const void *)queue_line);
    _mm_sfence();
}

static inline void
load_request_queue_remote(struct RequestQueueLine *dst,
                          volatile struct RequestQueueLine *src)
{
    clflushopt_line((const void *)src);
    _mm_mfence();
    copy_line_from_remote(dst, (const void *)src);
}

static void
init_request_ring_tokens(volatile uint64_t *queue, uint64_t slot_count)
{
    for (uint64_t slot = 0; slot < slot_count; slot++) {
        volatile struct RequestQueueLine *queue_line =
            request_queue_line_at(queue, slot);

        write_request_queue_remote(queue_line, slot, UINT64_MAX, UINT64_MAX);
    }
}

static void
init_response_ring_tokens(struct Connection *connection, uint64_t client_count)
{
    for (uint64_t client_id = 0; client_id < client_count; client_id++) {
        for (uint64_t slot = 0; slot < connection->per_client_slot_count; slot++) {
            write_queue_token_remote(response_queue_slot_ptr(connection,
                                                             client_id,
                                                             slot),
                                     slot);
        }
    }
}

static void
init_shared_lock_slots(struct SharedLockSlot *lock_slots, uint64_t client_count)
{
    for (uint64_t client_id = 0; client_id < client_count; client_id++) {
        struct SharedLockSlot *slot = &lock_slots[client_id];

        write_remote_u64(&slot->choosing, 0);
        write_remote_u64(&slot->number, 0);
    }
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

static inline void
store_shared_u8(uint8_t *ptr, uint8_t value)
{
    __atomic_store_n(ptr, value, __ATOMIC_RELAXED);
}

static void
signal_ready_and_wait(struct SharedControl *control)
{
    __atomic_fetch_add(&control->ready_count, 1, __ATOMIC_RELEASE);
    wait_for_start(&control->start_flag);
}

static inline struct SharedLockSlot *
shared_lock_slot(struct SharedLockSlot *lock_slots, uint64_t client_id)
{
    return &lock_slots[client_id];
}

static void
shared_lock_acquire(struct SharedLockSlot *lock_slots,
                    uint64_t client_id,
                    uint64_t client_count)
{
    struct SharedLockSlot *self = shared_lock_slot(lock_slots, client_id);
    uint64_t my_number = 0;

    write_remote_u64(&self->choosing, 1);

    for (uint64_t other_id = 0; other_id < client_count; other_id++) {
        struct SharedLockSlot *other = shared_lock_slot(lock_slots, other_id);
        uint64_t other_number = read_remote_u64(&other->number);

        if (other_number > my_number)
            my_number = other_number;
    }

    my_number++;
    write_remote_u64(&self->number, my_number);
    write_remote_u64(&self->choosing, 0);

    for (uint64_t other_id = 0; other_id < client_count; other_id++) {
        struct SharedLockSlot *other = shared_lock_slot(lock_slots, other_id);

        if (other_id == client_id)
            continue;

        while (read_remote_u64(&other->choosing) != 0)
            cpu_relax();

        for (;;) {
            uint64_t other_number = read_remote_u64(&other->number);

            if (other_number == 0)
                break;
            if (other_number > my_number)
                break;
            if (other_number == my_number && other_id > client_id)
                break;
            cpu_relax();
        }
    }
}

static void
shared_lock_release(struct SharedLockSlot *lock_slots, uint64_t client_id)
{
    struct SharedLockSlot *self = shared_lock_slot(lock_slots, client_id);

    write_remote_u64(&self->number, 0);
}

static int
run_server_shared(struct Connection *connection,
                  uint64_t client_count,
                  uint64_t slot_count,
                  uint64_t request_count,
                  int server_cpu,
                  struct SharedControl *control)
{
    uint64_t request_consume_seq = 0;
    uint64_t completed_total = 0;
    struct Request request_local;
    struct ResponseLine *response = NULL;
    struct Response response_local;
    struct RequestQueueLine request_queue_snapshot;

    if (pin_to_cpu(server_cpu) != 0) {
        fprintf(stderr, "failed to pin server to cpu %d\n", server_cpu);
        set_control_error(control);
        return 1;
    }

    signal_ready_and_wait(control);

    while (completed_total < client_count * request_count &&
           !control_has_error(control)) {
        uint64_t request_slot = request_consume_seq % slot_count;
        uint64_t request_ready_token = request_consume_seq + 1;
        uint64_t request_empty_token = request_consume_seq + slot_count;
        volatile struct RequestQueueLine *request_queue_line =
            request_queue_line_at(connection->request_queue, request_slot);
        uint64_t payload_slot = 0;
        struct RequestLine *request = NULL;
        volatile uint64_t *response_queue_slot = NULL;

        if (read_request_queue_token_remote(request_queue_line) !=
            request_ready_token) {
            cpu_relax();
            continue;
        }

        load_request_queue_remote(&request_queue_snapshot, request_queue_line);
        if (request_queue_snapshot.token != request_ready_token) {
            cpu_relax();
            continue;
        }
        if (request_queue_snapshot.client_id >= client_count ||
            request_queue_snapshot.req_id >= request_count) {
            fprintf(stderr,
                    "server request metadata out of range client=%" PRIu64
                    " req=%" PRIu64 "\n",
                    request_queue_snapshot.client_id,
                    request_queue_snapshot.req_id);
            set_control_error(control);
            break;
        }

        payload_slot =
            request_queue_snapshot.req_id % connection->per_client_slot_count;
        request = request_at_slot(connection,
                                  request_queue_snapshot.client_id,
                                  payload_slot);
        load_request_remote(&request_local, request);
        if (request_local.client_id != request_queue_snapshot.client_id ||
            request_local.req_id != request_queue_snapshot.req_id) {
            fprintf(stderr,
                    "server request payload mismatch descriptor_client=%" PRIu64
                    " descriptor_req=%" PRIu64
                    " payload_client=%" PRIu64
                    " payload_req=%" PRIu64 "\n",
                    request_queue_snapshot.client_id,
                    request_queue_snapshot.req_id,
                    request_local.client_id,
                    request_local.req_id);
            set_control_error(control);
            break;
        }

        response_queue_slot = response_queue_slot_ptr(connection,
                                                      request_local.client_id,
                                                      payload_slot);
        while (!control_has_error(control)) {
            if (read_queue_token_remote(response_queue_slot) ==
                request_local.req_id)
                break;
            cpu_relax();
        }
        if (control_has_error(control))
            break;

        response = response_at_slot(connection,
                                    request_local.client_id,
                                    payload_slot);
        response_local.client_id = request_local.client_id;
        response_local.req_id = request_local.req_id;
        response_local.sum = request_local.a + request_local.b;

        store_response_remote(response,
                              response_local.client_id,
                              response_local.req_id,
                              response_local.sum);
        write_queue_token_remote(response_queue_slot, response_local.req_id + 1);
        write_request_queue_remote(request_queue_line,
                                   request_empty_token,
                                   UINT64_MAX,
                                   UINT64_MAX);

        request_consume_seq++;
        completed_total++;
    }

    return control_has_error(control) ? 1 : 0;
}

static int
run_client_shared(struct Connection *connection,
                  uint64_t client_id,
                  uint64_t client_count,
                  uint64_t window_size,
                  uint64_t slot_count,
                  uint64_t request_count,
                  uint64_t *start_ns,
                  uint64_t *end_ns,
                  uint64_t *expected_sum,
                  uint64_t *actual_sum,
                  uint8_t *correctness_fail,
                  struct SharedControl *control)
{
    size_t base = (size_t)(client_id * request_count);
    uint64_t rng_state = client_id + 1;
    uint64_t sent = 0;
    uint64_t completed = 0;
    uint64_t pending_req_id = UINT64_MAX;
    uint64_t pending_start = 0;
    uint64_t pending_expected = 0;
    uint64_t pending_a = 0;
    uint64_t pending_b = 0;
    uint64_t *start_local = NULL;
    uint64_t *expected_local = NULL;
    int pending_valid = 0;

    start_local = calloc((size_t)request_count, sizeof(*start_local));
    expected_local = calloc((size_t)request_count, sizeof(*expected_local));
    if (start_local == NULL || expected_local == NULL) {
        fprintf(stderr,
                "client %" PRIu64 " local tracking allocation failed\n",
                client_id);
        free(start_local);
        free(expected_local);
        set_control_error(control);
        return 1;
    }

    if (pin_to_cpu((int)client_id) != 0) {
        fprintf(stderr, "failed to pin client %" PRIu64 " to cpu %" PRIu64 "\n",
                client_id,
                client_id);
        set_control_error(control);
        free(start_local);
        free(expected_local);
        return 1;
    }

    signal_ready_and_wait(control);

    while (completed < request_count && !control_has_error(control)) {
        int made_progress = 0;
        uint64_t active_window_size = (completed == 0) ? 1 : window_size;

        if (!pending_valid &&
            sent < request_count &&
            (sent - completed) < active_window_size) {
            uint64_t payload_slot = sent % connection->per_client_slot_count;
            struct RequestLine *request =
                request_at_slot(connection, client_id, payload_slot);

            pending_req_id = sent;
            pending_start = m5_rpns_ns();
            pending_a = next_rand(&rng_state);
            pending_b = next_rand(&rng_state);
            pending_expected = pending_a + pending_b;

            store_request_remote(request,
                                 client_id,
                                 pending_req_id,
                                 pending_a,
                                 pending_b);
            pending_valid = 1;
            made_progress = 1;
        }

        if (pending_valid && !control_has_error(control)) {
            int enqueued = 0;
            uint64_t request_seq = 0;
            uint64_t request_slot = 0;
            volatile struct RequestQueueLine *request_queue_line = NULL;

            shared_lock_acquire(connection->request_lock_slots,
                                client_id,
                                client_count);
            if (!control_has_error(control)) {
                request_seq = read_remote_u64(connection->request_produce_seq);
                request_slot = request_seq % slot_count;
                request_queue_line = request_queue_line_at(
                    connection->request_queue, request_slot
                );

                if (read_request_queue_token_remote(request_queue_line) ==
                    request_seq) {
                    write_request_queue_remote(request_queue_line,
                                               request_seq + 1,
                                               client_id,
                                               pending_req_id);
                    write_remote_u64(connection->request_produce_seq,
                                     request_seq + 1);
                    start_local[pending_req_id] = pending_start;
                    expected_local[pending_req_id] = pending_expected;
                    enqueued = 1;
                }
            }
            shared_lock_release(connection->request_lock_slots, client_id);

            if (enqueued) {
                pending_valid = 0;
                sent++;
                made_progress = 1;
            } else {
                cpu_relax();
            }
        }

        if (completed < sent) {
            uint64_t slot = completed % connection->per_client_slot_count;
            uint64_t ready_token = completed + 1;
            uint64_t empty_token = completed + connection->per_client_slot_count;
            volatile uint64_t *response_queue_slot =
                response_queue_slot_ptr(connection, client_id, slot);

            if (read_queue_token_remote(response_queue_slot) == ready_token) {
                struct ResponseLine *response =
                    response_at_slot(connection, client_id, slot);
                struct Response response_local;
                uint64_t expected = expected_local[completed];
                uint64_t actual = 0;
                uint64_t end = 0;
                int fail = 0;

                load_response_remote(&response_local, response);
                if (response_local.client_id != client_id ||
                    response_local.req_id != completed) {
                    fprintf(stderr,
                            "client response metadata mismatch "
                            "client=%" PRIu64 " req=%" PRIu64
                            " payload_client=%" PRIu64
                            " payload_req=%" PRIu64 "\n",
                            client_id,
                            completed,
                            response_local.client_id,
                            response_local.req_id);
                    set_control_error(control);
                    break;
                }

                actual = response_local.sum;
                fail = (actual != expected);
                end = m5_rpns_ns();

                store_shared_u64(&start_ns[base + completed],
                                 start_local[completed]);
                store_shared_u64(&end_ns[base + completed], end);
                store_shared_u64(&expected_sum[base + completed], expected);
                store_shared_u8(&correctness_fail[base + completed],
                                (uint8_t)fail);
                if (fail)
                    store_shared_u64(&actual_sum[base + completed], actual);

                write_queue_token_remote(response_queue_slot, empty_token);
                completed++;
                made_progress = 1;
            }
        }

        if (!made_progress)
            cpu_relax();
    }

    free(start_local);
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
              uint64_t *start_ns,
              uint64_t *end_ns,
              uint64_t *expected_sum,
              uint64_t *actual_sum,
              uint8_t *correctness_fail)
{
    for (uint64_t client_id = 0; client_id < client_count; client_id++) {
        for (uint64_t req_id = 0; req_id < request_count; req_id++) {
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
            if (correctness_fail[index]) {
                fprintf(stream,
                        "client_%" PRIu64 "_req_%" PRIu64
                        "_correctness_fail expected=%" PRIu64
                        " actual=%" PRIu64 "\n",
                        client_id,
                        req_id,
                        expected_sum[index],
                        actual_sum[index]);
            }
        }
    }
}

static void
print_results_with_rc(FILE *stream,
                      int rc,
                      uint64_t client_count,
                      uint64_t request_count,
                      uint64_t *start_ns,
                      uint64_t *end_ns,
                      uint64_t *expected_sum,
                      uint64_t *actual_sum,
                      uint8_t *correctness_fail)
{
    fprintf(stream, "benchmark_rc=%d\n", rc);
    write_results(stream,
                  client_count,
                  request_count,
                  start_ns,
                  end_ns,
                  expected_sum,
                  actual_sum,
                  correctness_fail);
}

static int
has_correctness_failures(uint64_t total_requests, uint8_t *correctness_fail)
{
    for (uint64_t i = 0; i < total_requests; i++) {
        if (correctness_fail[i] != 0)
            return 1;
    }

    return 0;
}

int
main(int argc, char **argv)
{
    uint64_t client_count = 1;
    uint64_t request_count = 8;
    uint64_t window_size = 16;
    uint64_t slot_count = 0;
    uint64_t per_client_slot_count = 0;
    int cxl_node = 1;
    int server_cpu = -1;
    long online_cpus = sysconf(_SC_NPROCESSORS_ONLN);
    uint64_t total_requests = 0;
    size_t request_queue_size = 0;
    size_t response_queue_size = 0;
    size_t request_data_size = 0;
    size_t response_data_size = 0;
    size_t request_lock_slots_size = 0;
    size_t request_produce_seq_size = 0;
    struct Connection connection = {0};
    struct SharedControl *control = NULL;
    uint64_t *start_ns = NULL;
    uint64_t *end_ns = NULL;
    uint64_t *expected_sum = NULL;
    uint64_t *actual_sum = NULL;
    uint8_t *correctness_fail = NULL;
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
        } else if (strcmp(argv[i], "--cxl-node") == 0 && i + 1 < argc) {
            cxl_node = (int)parse_u64("cxl-node", argv[++i]);
        } else if (strcmp(argv[i], "--server-cpu") == 0 && i + 1 < argc) {
            server_cpu = (int)parse_u64("server-cpu", argv[++i]);
        } else if (strcmp(argv[i], "--help") == 0 ||
                   strcmp(argv[i], "-h") == 0) {
            printf("Usage: %s [--client-count N] [--count-per-client N] "
                   "[--window-size N] [--slot-count N] [--cxl-node N] "
                   "[--server-cpu N]\n"
                   "  slot-count is shared request-ring depth; default is "
                   "client-count * min(window-size, count-per-client)\n"
                   "  each client keeps its first RPC fully serialized before "
                   "the steady-state window opens\n",
                   argv[0]);
            return 0;
        } else {
            fprintf(stderr, "unknown argument: %s\n", argv[i]);
            return 2;
        }
    }

    if (!cpu_has_clflushopt()) {
        fprintf(stderr,
                "clflushopt is required for hydrarpc_multiclient_shared_send1_poll1\n");
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

    if (server_cpu < 0)
        server_cpu = (int)client_count;

    per_client_slot_count =
        window_size < request_count ? window_size : request_count;
    if (slot_count == 0)
        slot_count = client_count * per_client_slot_count;

    if (slot_count == 0) {
        fprintf(stderr, "slot-count must be positive after defaulting\n");
        rc = 2;
        goto cleanup;
    }

    if (per_client_slot_count == 0) {
        fprintf(stderr,
                "per-client slot count must be positive after defaulting\n");
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
    request_queue_size = sizeof(*connection.request_queue) * slot_count *
                         QUEUE_STRIDE_WORDS;
    response_queue_size = sizeof(*connection.response_queue) * client_count *
                          per_client_slot_count * QUEUE_STRIDE_WORDS;
    request_data_size = (size_t)(CACHELINE_SIZE * client_count *
                                 per_client_slot_count);
    response_data_size = (size_t)(CACHELINE_SIZE * client_count *
                                  per_client_slot_count);
    request_lock_slots_size = sizeof(*connection.request_lock_slots) * client_count;
    request_produce_seq_size = sizeof(*connection.request_produce_seq);

    connection.per_client_slot_count = per_client_slot_count;
    connection.request_queue = alloc_shared_cxl(request_queue_size, cxl_node);
    connection.response_queue = alloc_shared_cxl(response_queue_size, cxl_node);
    connection.request_data_area = alloc_shared_cxl(request_data_size, cxl_node);
    connection.response_data_area = alloc_shared_cxl(response_data_size, cxl_node);
    connection.request_lock_slots =
        alloc_shared_cxl(request_lock_slots_size, cxl_node);
    connection.request_produce_seq =
        alloc_shared_cxl(request_produce_seq_size, cxl_node);
    init_request_ring_tokens(connection.request_queue, slot_count);
    init_response_ring_tokens(&connection, client_count);
    init_shared_lock_slots(connection.request_lock_slots, client_count);
    write_remote_u64(connection.request_produce_seq, 0);

    child_pids = calloc((size_t)(client_count + 1), sizeof(*child_pids));
    control = alloc_shared_zeroed(sizeof(*control));
    start_ns = alloc_shared_zeroed(sizeof(*start_ns) * total_requests);
    end_ns = alloc_shared_zeroed(sizeof(*end_ns) * total_requests);
    expected_sum = alloc_shared_zeroed(sizeof(*expected_sum) * total_requests);
    actual_sum = alloc_shared_zeroed(sizeof(*actual_sum) * total_requests);
    correctness_fail = alloc_shared_zeroed(
        sizeof(*correctness_fail) * total_requests
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
                                             client_id,
                                             client_count,
                                             window_size,
                                             slot_count,
                                             request_count,
                                             start_ns,
                                             end_ns,
                                             expected_sum,
                                             actual_sum,
                                             correctness_fail,
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
    if (rc == 0 && has_correctness_failures(total_requests, correctness_fail))
        rc = 1;

    print_results_with_rc(stdout,
                          rc,
                          client_count,
                          request_count,
                          start_ns,
                          end_ns,
                          expected_sum,
                          actual_sum,
                          correctness_fail);

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
    if (connection.request_lock_slots != NULL) {
        munmap((void *)connection.request_lock_slots,
               shared_mapping_size(request_lock_slots_size));
    }
    if (connection.request_produce_seq != NULL) {
        munmap((void *)connection.request_produce_seq,
               shared_mapping_size(request_produce_seq_size));
    }

    if (control != NULL)
        munmap((void *)control, shared_mapping_size(sizeof(*control)));
    if (start_ns != NULL) {
        munmap((void *)start_ns,
               shared_mapping_size(sizeof(*start_ns) * total_requests));
    }
    if (end_ns != NULL) {
        munmap((void *)end_ns,
               shared_mapping_size(sizeof(*end_ns) * total_requests));
    }
    if (expected_sum != NULL) {
        munmap((void *)expected_sum,
               shared_mapping_size(sizeof(*expected_sum) * total_requests));
    }
    if (actual_sum != NULL) {
        munmap((void *)actual_sum,
               shared_mapping_size(sizeof(*actual_sum) * total_requests));
    }
    if (correctness_fail != NULL) {
        munmap((void *)correctness_fail,
               shared_mapping_size(sizeof(*correctness_fail) * total_requests));
    }

    free(child_pids);
    return rc;
}
