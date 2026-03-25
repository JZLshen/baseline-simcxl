#define _GNU_SOURCE

#include <cpuid.h>
#include <errno.h>
#include <inttypes.h>
#include <linux/mempolicy.h>
#include <signal.h>
#include <sched.h>
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
#include <fcntl.h>

#define CACHELINE_SIZE 64u
#define QUEUE_STRIDE_WORDS (CACHELINE_SIZE / sizeof(uint64_t))
#define X86_M5OPS_PADDR 0xffff0000ull
#define X86_M5OPS_SIZE 0x10000u
#define M5OP_RPNS 0x07u

struct Request {
    uint64_t a;
    uint64_t b;
} __attribute__((aligned(16)));

struct Response {
    uint64_t sum;
};

_Static_assert(sizeof(struct Request) == sizeof(__m128i),
               "Request must remain 16 bytes for aligned 128-bit stores");

struct Connection {
    volatile uint64_t *request_queue;
    volatile uint64_t *response_queue;
    uint8_t *request_data_area;
    uint8_t *response_data_area;
};

struct SlotCursor {
    uint64_t slot;
    uint64_t phase;
};

static volatile uint8_t *m5ops_base = NULL;

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
    volatile uint64_t *rpns_addr = (volatile uint64_t *)(const void *)
        (m5ops_base + ((uint64_t)M5OP_RPNS << 8));

    return *rpns_addr;
}

static void
map_m5ops_or_die(void)
{
    int fd = open("/dev/mem", O_RDWR | O_SYNC);
    void *mapping = NULL;

    if (fd < 0) {
        fprintf(stderr, "open /dev/mem failed: %s\n", strerror(errno));
        exit(1);
    }

    mapping = mmap(NULL,
                   X86_M5OPS_SIZE,
                   PROT_READ | PROT_WRITE,
                   MAP_SHARED,
                   fd,
                   X86_M5OPS_PADDR);
    close(fd);

    if (mapping == MAP_FAILED) {
        fprintf(stderr, "mmap m5ops failed: %s\n", strerror(errno));
        exit(1);
    }

    m5ops_base = (volatile uint8_t *)mapping;
}

static void
unmap_m5ops(void)
{
    if (m5ops_base != NULL) {
        munmap((void *)(uintptr_t)m5ops_base, X86_M5OPS_SIZE);
        m5ops_base = NULL;
    }
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

static uint64_t
parse_inflight_limit(const char *value)
{
    uint64_t window = parse_u64("window-mode", value);
    return window + 1;
}

static void *
alloc_aligned(size_t size)
{
    void *ptr = NULL;
    size_t alloc_size = (size_t)round_up_u64((uint64_t)size, CACHELINE_SIZE);

    if (posix_memalign(&ptr, CACHELINE_SIZE, alloc_size) != 0 || !ptr) {
        fprintf(stderr, "allocation failed for %zu bytes\n", size);
        exit(1);
    }

    return ptr;
}

static size_t
shared_mapping_size(size_t size)
{
    uint64_t align = page_size_u64();

    return (size_t)round_up_u64((uint64_t)size, align);
}

static void *
alloc_shared(size_t size)
{
    size_t mapping_size = shared_mapping_size(size);
    void *ptr = mmap(NULL,
                     mapping_size,
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
    void *ptr = alloc_shared(size);

    bind_mapping_to_node_or_die(ptr, size, cxl_node);
    return ptr;
}

static inline uint64_t
slot_offset(uint64_t slot)
{
    return slot * CACHELINE_SIZE;
}

static inline uint64_t
make_entry(uint64_t offset, uint64_t phase)
{
    return offset | (phase & 0x1u);
}

static inline uint64_t
slot_entry(uint64_t slot, uint64_t phase)
{
    return make_entry(slot_offset(slot), phase);
}

static inline volatile uint64_t *
queue_entry_ptr(volatile uint64_t *queue, uint64_t slot)
{
    return &queue[slot * QUEUE_STRIDE_WORDS];
}

static inline struct Request *
request_at_slot(struct Connection *connection, uint64_t slot)
{
    return (struct Request *)(connection->request_data_area + slot_offset(slot));
}

static inline struct Response *
response_at_slot(struct Connection *connection, uint64_t slot)
{
    return (struct Response *)(connection->response_data_area + slot_offset(slot));
}

static inline void
advance_slot_cursor(struct SlotCursor *cursor, uint64_t slot_count)
{
    cursor->slot++;
    if (cursor->slot == slot_count) {
        cursor->slot = 0;
        cursor->phase ^= 0x1u;
    }
}

static inline __m128i
pack_request(uint64_t a, uint64_t b)
{
    return _mm_set_epi64x((long long)b, (long long)a);
}

__attribute__((noinline)) static void
copy_bytes_to_remote(void *dst, const void *src, size_t size)
{
    memcpy(dst, src, size);
}

__attribute__((noinline)) static void
copy_bytes_from_remote(void *dst, const void *src, size_t size)
{
    volatile const uint8_t *src_bytes =
        (volatile const uint8_t *)(const void *)src;
    uint8_t *dst_bytes = (uint8_t *)dst;

    for (size_t i = 0; i < size; i++)
        dst_bytes[i] = src_bytes[i];
}

static inline void
store_request_payload_local(struct Request *request, __m128i payload)
{
    _mm_store_si128((__m128i *)(void *)request, payload);
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
store_request_remote(struct Request *dst, const struct Request *src)
{
    copy_bytes_to_remote(dst, src, sizeof(*src));
    clflushopt_line((const void *)dst);
    _mm_sfence();
}

static inline void
load_request_remote(struct Request *dst, const struct Request *src)
{
    clflushopt_line((const void *)src);
    _mm_mfence();
    copy_bytes_from_remote(dst, src, sizeof(*dst));
}

static inline void
store_response_remote(struct Response *dst, const struct Response *src)
{
    copy_bytes_to_remote(dst, src, sizeof(*src));
    clflushopt_line((const void *)dst);
    _mm_sfence();
}

static inline void
load_response_remote(struct Response *dst, const struct Response *src)
{
    clflushopt_line((const void *)src);
    _mm_mfence();
    copy_bytes_from_remote(dst, src, sizeof(*dst));
}

static void
init_ring_tokens(volatile uint64_t *queue, uint64_t slot_count)
{
    for (uint64_t slot = 0; slot < slot_count; slot++)
        write_queue_token_remote(queue_entry_ptr(queue, slot), slot);
}

static uint64_t
next_rand(uint64_t *state)
{
    *state = (*state * 6364136223846793005ull) + 1442695040888963407ull;
    return *state;
}

static uint64_t
virt_to_phys_addr(const void *ptr)
{
    uint64_t phys = 0;
    uint64_t vaddr = (uint64_t)(uintptr_t)ptr;
    uint64_t page_size = page_size_u64();
    uint64_t page_index = vaddr / page_size;
    uint64_t page_offset = vaddr % page_size;
    uint64_t entry = 0;
    int fd = open("/proc/self/pagemap", O_RDONLY);

    if (fd < 0)
        return 0;

    off_t entry_offset = (off_t)(page_index * sizeof(entry));
    if (pread(fd, &entry, sizeof(entry), entry_offset) != (ssize_t)sizeof(entry)) {
        close(fd);
        return 0;
    }

    close(fd);

    if (((entry >> 63) & 0x1u) == 0)
        return 0;

    phys = ((entry & ((1ull << 55) - 1)) * page_size) + page_offset;
    return phys;
}

static void
dump_slot_layout_line(const char *label, uint64_t slot, const void *ptr)
{
    uint64_t vaddr = (uint64_t)(uintptr_t)ptr;
    uint64_t paddr = virt_to_phys_addr(ptr);

    printf("layout_%s_slot_%" PRIu64 "_va=0x%016" PRIx64 "\n",
           label,
           slot,
           vaddr);
    printf("layout_%s_slot_%" PRIu64 "_pa=0x%016" PRIx64 "\n",
           label,
           slot,
           paddr);
}

static void
dump_connection_layout(struct Connection *connection,
                       uint64_t slot_count,
                       size_t request_queue_size,
                       size_t response_queue_size,
                       size_t request_data_size,
                       size_t response_data_size)
{
    uint64_t last_slot = slot_count - 1;

    printf("layout_slot_count=%" PRIu64 "\n", slot_count);
    printf("layout_request_queue_base_va=0x%016" PRIx64 "\n",
           (uint64_t)(uintptr_t)connection->request_queue);
    printf("layout_request_queue_base_pa=0x%016" PRIx64 "\n",
           virt_to_phys_addr((const void *)connection->request_queue));
    printf("layout_response_queue_base_va=0x%016" PRIx64 "\n",
           (uint64_t)(uintptr_t)connection->response_queue);
    printf("layout_response_queue_base_pa=0x%016" PRIx64 "\n",
           virt_to_phys_addr((const void *)connection->response_queue));
    printf("layout_request_data_base_va=0x%016" PRIx64 "\n",
           (uint64_t)(uintptr_t)connection->request_data_area);
    printf("layout_request_data_base_pa=0x%016" PRIx64 "\n",
           virt_to_phys_addr((const void *)connection->request_data_area));
    printf("layout_response_data_base_va=0x%016" PRIx64 "\n",
           (uint64_t)(uintptr_t)connection->response_data_area);
    printf("layout_response_data_base_pa=0x%016" PRIx64 "\n",
           virt_to_phys_addr((const void *)connection->response_data_area));

    dump_slot_layout_line("request_queue", 0, (const void *)queue_entry_ptr(connection->request_queue, 0));
    dump_slot_layout_line("response_queue", 0, (const void *)queue_entry_ptr(connection->response_queue, 0));
    dump_slot_layout_line("request_data", 0, (const void *)request_at_slot(connection, 0));
    dump_slot_layout_line("response_data", 0, (const void *)response_at_slot(connection, 0));

    if (last_slot != 0) {
        dump_slot_layout_line("request_queue", last_slot, (const void *)queue_entry_ptr(connection->request_queue, last_slot));
        dump_slot_layout_line("response_queue", last_slot, (const void *)queue_entry_ptr(connection->response_queue, last_slot));
        dump_slot_layout_line("request_data", last_slot, (const void *)request_at_slot(connection, last_slot));
        dump_slot_layout_line("response_data", last_slot, (const void *)response_at_slot(connection, last_slot));
    }
}

static void
run_server_windowed(struct Connection *connection,
                    uint64_t request_count,
                    uint64_t slot_count)
{
    struct Request request_local;
    struct Response response_local;

    for (uint64_t seq = 0; seq < request_count; seq++) {
        uint64_t slot = seq % slot_count;
        uint64_t request_ready_token = seq + 1;
        uint64_t request_empty_token = seq + slot_count;
        uint64_t response_ready_token = seq + 1;
        volatile uint64_t *request_queue_slot = queue_entry_ptr(
            connection->request_queue, slot
        );
        volatile uint64_t *response_queue_slot = queue_entry_ptr(
            connection->response_queue, slot
        );
        struct Request *request = request_at_slot(connection, slot);
        struct Response *response = response_at_slot(connection, slot);

        for (;;) {
            if (read_queue_token_remote(request_queue_slot) == request_ready_token)
                break;
            cpu_relax();
        }

        load_request_remote(&request_local, request);
        uint64_t a = request_local.a;
        uint64_t b = request_local.b;
        write_queue_token_remote(request_queue_slot, request_empty_token);

        response_local.sum = a + b;
        while (read_queue_token_remote(response_queue_slot) != seq)
            cpu_relax();
        store_response_remote(response, &response_local);
        write_queue_token_remote(response_queue_slot, response_ready_token);
    }
}

static void
run_client_windowed(struct Connection *connection,
                    uint64_t *rng_state,
                    uint64_t request_count,
                    uint64_t inflight_limit,
                    uint64_t slot_count,
                    uint64_t *start_ns,
                    uint64_t *end_ns,
                    uint64_t *expected_sum,
                    uint64_t *actual_sum,
                    uint8_t *correctness_fail)
{
    struct Request request_local;
    volatile uint64_t *request_queue_slot = NULL;
    volatile uint64_t *response_queue_slot = NULL;
    struct Request *request = NULL;
    struct Response *response = NULL;
    uint64_t slot = 0;
    uint64_t expected_entry = 0;
    uint64_t a = 0;
    uint64_t b = 0;
    uint64_t sum = 0;
    uint64_t start = 0;
    uint64_t end = 0;
    int fail = 0;
    __m128i payload;
    uint64_t send_seq = 0;
    uint64_t recv_seq = 0;

    while (recv_seq < request_count) {
        while (send_seq < request_count &&
               (send_seq - recv_seq) < inflight_limit) {
            start = m5_rpns_ns();
            slot = send_seq % slot_count;
            expected_entry = send_seq;
            request_queue_slot = queue_entry_ptr(
                connection->request_queue, slot
            );
            if (read_queue_token_remote(request_queue_slot) != expected_entry)
                break;
            request = request_at_slot(connection, slot);
            a = next_rand(rng_state);
            b = next_rand(rng_state);
            payload = pack_request(a, b);

            start_ns[send_seq] = start;
            expected_sum[send_seq] = a + b;
            store_request_payload_local(&request_local, payload);
            store_request_remote(request, &request_local);
            write_queue_token_remote(request_queue_slot, send_seq + 1);

            send_seq++;
        }

        slot = recv_seq % slot_count;
        expected_entry = recv_seq + 1;
        response_queue_slot = queue_entry_ptr(
            connection->response_queue, slot
        );
        response = response_at_slot(connection, slot);

        if (read_queue_token_remote(response_queue_slot) != expected_entry) {
            cpu_relax();
            continue;
        }

        {
            struct Response response_local;
            load_response_remote(&response_local, response);
            sum = response_local.sum;
        }
        fail = (sum != expected_sum[recv_seq]);
        end = m5_rpns_ns();

        end_ns[recv_seq] = end;
        correctness_fail[recv_seq] = (uint8_t)fail;
        if (fail)
            actual_sum[recv_seq] = sum;
        write_queue_token_remote(response_queue_slot, recv_seq + slot_count);
        recv_seq++;
    }
}

int
main(int argc, char **argv)
{
    uint64_t request_count = 1;
    uint64_t inflight_limit = 1;
    uint64_t slot_count = 32;
    int cxl_node = 1;
    int client_cpu = 0;
    int server_cpu = 1;
    int dump_layout = 0;
    long online_cpus = sysconf(_SC_NPROCESSORS_ONLN);
    struct Connection connection = {0};
    uint64_t *expected_sum = NULL;
    uint64_t *actual_sum = NULL;
    uint64_t *start_ns = NULL;
    uint64_t *end_ns = NULL;
    uint8_t *correctness_fail = NULL;
    uint64_t rng_state = 1;
    size_t request_queue_size = 0;
    size_t response_queue_size = 0;
    size_t request_data_size = 0;
    size_t response_data_size = 0;
    pid_t server_pid = -1;
    int ready_pipe[2] = {-1, -1};
    int rc = 0;

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--count") == 0 && i + 1 < argc) {
            request_count = parse_u64("count", argv[++i]);
        } else if (strcmp(argv[i], "--window-mode") == 0 && i + 1 < argc) {
            inflight_limit = parse_inflight_limit(argv[++i]);
        } else if (strcmp(argv[i], "--slot-count") == 0 && i + 1 < argc) {
            slot_count = parse_u64("slot-count", argv[++i]);
        } else if (strcmp(argv[i], "--cxl-node") == 0 && i + 1 < argc) {
            cxl_node = (int)parse_u64("cxl-node", argv[++i]);
        } else if (strcmp(argv[i], "--client-cpu") == 0 && i + 1 < argc) {
            client_cpu = (int)parse_u64("client-cpu", argv[++i]);
        } else if (strcmp(argv[i], "--server-cpu") == 0 && i + 1 < argc) {
            server_cpu = (int)parse_u64("server-cpu", argv[++i]);
        } else if (strcmp(argv[i], "--dump-layout") == 0) {
            dump_layout = 1;
        } else if (strcmp(argv[i], "--help") == 0 ||
                   strcmp(argv[i], "-h") == 0) {
            printf("Usage: %s [--count N] [--window-mode N] "
                   "[--slot-count N] [--cxl-node N] "
                   "[--client-cpu N] [--server-cpu N] [--dump-layout]\n",
                   argv[0]);
            return 0;
        } else {
            fprintf(stderr, "unknown argument: %s\n", argv[i]);
            return 2;
        }
    }

    if (!cpu_has_clflushopt()) {
        fprintf(stderr, "clflushopt is required for hydrarpc_baseline_rtc\n");
        return 2;
    }

    map_m5ops_or_die();

    if (request_count == 0) {
        fprintf(stderr, "count must be positive\n");
        return 2;
    }

    if (slot_count == 0) {
        fprintf(stderr, "slot-count must be positive\n");
        return 2;
    }

    if (slot_count < inflight_limit) {
        fprintf(stderr,
                "slot-count (%" PRIu64 ") must be >= outstanding depth (%" PRIu64
                ")\n",
                slot_count,
                inflight_limit);
        return 2;
    }

    if (client_cpu == server_cpu) {
        fprintf(stderr, "client-cpu and server-cpu must differ\n");
        return 2;
    }

    if (online_cpus <= client_cpu || online_cpus <= server_cpu) {
        fprintf(stderr,
                "need online cpus > max(client-cpu, server-cpu), got %ld\n",
                online_cpus);
        return 2;
    }

    request_queue_size =
        sizeof(*connection.request_queue) * slot_count * QUEUE_STRIDE_WORDS;
    response_queue_size =
        sizeof(*connection.response_queue) * slot_count * QUEUE_STRIDE_WORDS;
    request_data_size = (size_t)(CACHELINE_SIZE * slot_count);
    response_data_size = (size_t)(CACHELINE_SIZE * slot_count);

    connection.request_queue = alloc_shared_cxl(request_queue_size, cxl_node);
    connection.response_queue = alloc_shared_cxl(response_queue_size, cxl_node);
    connection.request_data_area = alloc_shared_cxl(request_data_size, cxl_node);
    connection.response_data_area =
        alloc_shared_cxl(response_data_size, cxl_node);
    init_ring_tokens(connection.request_queue, slot_count);
    init_ring_tokens(connection.response_queue, slot_count);

    if (dump_layout) {
        dump_connection_layout(&connection,
                               slot_count,
                               request_queue_size,
                               response_queue_size,
                               request_data_size,
                               response_data_size);
        fflush(stdout);
    }

    if (pipe(ready_pipe) != 0) {
        fprintf(stderr, "pipe failed: %s\n", strerror(errno));
        rc = 1;
        goto cleanup;
    }

    server_pid = fork();
    if (server_pid < 0) {
        fprintf(stderr, "fork failed: %s\n", strerror(errno));
        rc = 1;
        goto cleanup;
    }

    if (server_pid == 0) {
        char ready = 'R';

        close(ready_pipe[0]);
        if (pin_to_cpu(server_cpu) != 0) {
            fprintf(stderr, "failed to pin server process to cpu %d\n",
                    server_cpu);
            _exit(1);
        }
        if (write(ready_pipe[1], &ready, sizeof(ready)) != sizeof(ready)) {
            fprintf(stderr, "server ready signal failed: %s\n",
                    strerror(errno));
            _exit(1);
        }
        close(ready_pipe[1]);

        run_server_windowed(&connection, request_count, slot_count);

        _exit(0);
    }

    close(ready_pipe[1]);
    ready_pipe[1] = -1;

    expected_sum = alloc_aligned(sizeof(*expected_sum) * request_count);
    actual_sum = alloc_aligned(sizeof(*actual_sum) * request_count);
    start_ns = alloc_aligned(sizeof(*start_ns) * request_count);
    end_ns = alloc_aligned(sizeof(*end_ns) * request_count);
    correctness_fail =
        alloc_aligned(sizeof(*correctness_fail) * request_count);
    memset(expected_sum, 0, sizeof(*expected_sum) * request_count);
    memset(actual_sum, 0, sizeof(*actual_sum) * request_count);
    memset(start_ns, 0, sizeof(*start_ns) * request_count);
    memset(end_ns, 0, sizeof(*end_ns) * request_count);
    memset(correctness_fail, 0, sizeof(*correctness_fail) * request_count);

    if (pin_to_cpu(client_cpu) != 0) {
        fprintf(stderr, "failed to pin client process to cpu %d\n", client_cpu);
        rc = 1;
        goto cleanup_server;
    }

    {
        char ready = 0;
        ssize_t ready_rc = read(ready_pipe[0], &ready, sizeof(ready));

        if (ready_rc != sizeof(ready) || ready != 'R') {
            fprintf(stderr, "client ready wait failed\n");
            rc = 1;
            goto cleanup_server;
        }
    }

    close(ready_pipe[0]);
    ready_pipe[0] = -1;

    run_client_windowed(&connection,
                        &rng_state,
                        request_count,
                        inflight_limit,
                        slot_count,
                        start_ns,
                        end_ns,
                        expected_sum,
                        actual_sum,
                        correctness_fail);

cleanup_server:
    if (ready_pipe[0] != -1) {
        close(ready_pipe[0]);
        ready_pipe[0] = -1;
    }
    if (ready_pipe[1] != -1) {
        close(ready_pipe[1]);
        ready_pipe[1] = -1;
    }

    if (server_pid > 0) {
        int status = 0;

        if (rc != 0)
            kill(server_pid, SIGKILL);
        if (waitpid(server_pid, &status, 0) < 0) {
            fprintf(stderr, "waitpid failed: %s\n", strerror(errno));
            rc = 1;
        } else if (rc == 0) {
            if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
                if (WIFSIGNALED(status)) {
                    fprintf(stderr, "server process killed by signal %d\n",
                            WTERMSIG(status));
                } else {
                    fprintf(stderr, "server process exited with failure\n");
                }
                rc = 1;
            }
        }
    }

    if (rc == 0) {
        for (uint64_t i = 0; i < request_count; i++) {
            printf("req_%" PRIu64 "_start_ns=%" PRIu64 "\n", i, start_ns[i]);
            printf("req_%" PRIu64 "_end_ns=%" PRIu64 "\n", i, end_ns[i]);
            if (correctness_fail[i]) {
                printf("req_%" PRIu64 "_correctness_fail expected=%" PRIu64
                       " actual=%" PRIu64 "\n",
                       i, expected_sum[i], actual_sum[i]);
            }
        }
    }

cleanup:
    unmap_m5ops();
    if (ready_pipe[0] != -1)
        close(ready_pipe[0]);
    if (ready_pipe[1] != -1)
        close(ready_pipe[1]);
    if (connection.request_queue != NULL) {
        munmap((void *)connection.request_queue,
               shared_mapping_size(request_queue_size));
    }
    if (connection.response_queue != NULL) {
        munmap((void *)connection.response_queue,
               shared_mapping_size(response_queue_size));
    }
    if (connection.request_data_area != NULL) {
        munmap(connection.request_data_area,
               shared_mapping_size(request_data_size));
    }
    if (connection.response_data_area != NULL) {
        munmap(connection.response_data_area,
               shared_mapping_size(response_data_size));
    }
    free(expected_sum);
    free(actual_sum);
    free(start_ns);
    free(end_ns);
    free(correctness_fail);

    return rc;
}
