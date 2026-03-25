#define _GNU_SOURCE

#include <cpuid.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <sched.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <x86intrin.h>

#define CACHELINE_SIZE 64u
#define QUEUE_STRIDE_WORDS (CACHELINE_SIZE / sizeof(uint64_t))

struct PayloadLine {
    uint64_t words[QUEUE_STRIDE_WORDS];
} __attribute__((aligned(CACHELINE_SIZE)));

_Static_assert(sizeof(struct PayloadLine) == CACHELINE_SIZE,
               "PayloadLine must remain one cache line");

struct Ring {
    volatile uint64_t *request_queue;
    volatile uint64_t *response_queue;
    uint8_t *request_data;
    uint8_t *response_data;
    uint64_t slots;
};

struct SlotCursor {
    uint64_t slot;
    uint64_t phase;
};

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

static size_t
shared_mapping_size(size_t size)
{
    return (size_t)round_up_u64((uint64_t)size, page_size_u64());
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

static inline struct PayloadLine *
payload_at_slot(uint8_t *base, uint64_t slot)
{
    return (struct PayloadLine *)(base + slot_offset(slot));
}

static inline void
advance_slot_cursor(struct SlotCursor *cursor, uint64_t slots)
{
    cursor->slot++;
    if (cursor->slot == slots) {
        cursor->slot = 0;
        cursor->phase ^= 0x1u;
    }
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
load_payload_remote(struct PayloadLine *dst, const struct PayloadLine *src)
{
    clflushopt_line((const void *)src);
    _mm_mfence();
    copy_line_from_remote(dst, src);
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

    if (pread(fd, &entry, sizeof(entry), (off_t)(page_index * sizeof(entry))) !=
        (ssize_t)sizeof(entry)) {
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
    printf("layout_%s_slot_%" PRIu64 "_va=0x%016" PRIx64 "\n",
           label,
           slot,
           (uint64_t)(uintptr_t)ptr);
    printf("layout_%s_slot_%" PRIu64 "_pa=0x%016" PRIx64 "\n",
           label,
           slot,
           virt_to_phys_addr(ptr));
}

static void
dump_ring_layout(struct Ring *ring,
                 size_t request_queue_size,
                 size_t response_queue_size,
                 size_t request_data_size,
                 size_t response_data_size)
{
    uint64_t last_slot = ring->slots - 1;

    printf("layout_slots=%" PRIu64 "\n", ring->slots);
    printf("layout_request_queue_base_pa=0x%016" PRIx64 "\n",
           virt_to_phys_addr((const void *)ring->request_queue));
    printf("layout_response_queue_base_pa=0x%016" PRIx64 "\n",
           virt_to_phys_addr((const void *)ring->response_queue));
    printf("layout_request_data_base_pa=0x%016" PRIx64 "\n",
           virt_to_phys_addr((const void *)ring->request_data));
    printf("layout_response_data_base_pa=0x%016" PRIx64 "\n",
           virt_to_phys_addr((const void *)ring->response_data));

    dump_slot_layout_line("request_queue", 0, (const void *)queue_entry_ptr(ring->request_queue, 0));
    dump_slot_layout_line("response_queue", 0, (const void *)queue_entry_ptr(ring->response_queue, 0));
    dump_slot_layout_line("request_data", 0, (const void *)payload_at_slot(ring->request_data, 0));
    dump_slot_layout_line("response_data", 0, (const void *)payload_at_slot(ring->response_data, 0));

    if (last_slot != 0) {
        dump_slot_layout_line("request_queue", last_slot, (const void *)queue_entry_ptr(ring->request_queue, last_slot));
        dump_slot_layout_line("response_queue", last_slot, (const void *)queue_entry_ptr(ring->response_queue, last_slot));
        dump_slot_layout_line("request_data", last_slot, (const void *)payload_at_slot(ring->request_data, last_slot));
        dump_slot_layout_line("response_data", last_slot, (const void *)payload_at_slot(ring->response_data, last_slot));
    }
}

static void
run_server(struct Ring *ring, uint64_t request_count)
{
    struct SlotCursor cursor = {.slot = 0, .phase = 1};
    struct PayloadLine request_local;
    struct PayloadLine response_local;

    memset(&response_local, 0, sizeof(response_local));

    for (uint64_t seq = 0; seq < request_count; seq++) {
        uint64_t slot = cursor.slot;
        uint64_t expected_entry = slot_entry(slot, cursor.phase);
        volatile uint64_t *request_queue_slot =
            queue_entry_ptr(ring->request_queue, slot);
        volatile uint64_t *response_queue_slot =
            queue_entry_ptr(ring->response_queue, slot);
        struct PayloadLine *request =
            payload_at_slot(ring->request_data, slot);
        struct PayloadLine *response =
            payload_at_slot(ring->response_data, slot);

        for (;;) {
            if (read_queue_token_remote(request_queue_slot) == expected_entry)
                break;
        }

        load_payload_remote(&request_local, request);
        response_local.words[0] = request_local.words[0] + 1u;
        response_local.words[1] =
            request_local.words[1] ^ 0xfeedfacecafebeefull;
        copy_line_to_remote(response, &response_local);
        clflushopt_line((const void *)response);
        _mm_sfence();

        *response_queue_slot = expected_entry;
        clflushopt_line((const void *)response_queue_slot);
        _mm_sfence();

        advance_slot_cursor(&cursor, ring->slots);
    }
}

static int
run_client(struct Ring *ring, uint64_t request_count, uint64_t inflight_limit)
{
    struct SlotCursor send_cursor = {.slot = 0, .phase = 1};
    struct SlotCursor recv_cursor = {.slot = 0, .phase = 1};
    struct PayloadLine request_local;
    uint64_t send_seq = 0;
    uint64_t recv_seq = 0;

    memset(&request_local, 0, sizeof(request_local));

    while (recv_seq < request_count) {
        while (send_seq < request_count &&
               (send_seq - recv_seq) < inflight_limit) {
            uint64_t slot = send_cursor.slot;
            uint64_t expected_entry = slot_entry(slot, send_cursor.phase);
            volatile uint64_t *request_queue_slot =
                queue_entry_ptr(ring->request_queue, slot);
            struct PayloadLine *request =
                payload_at_slot(ring->request_data, slot);

            request_local.words[0] = send_seq;
            request_local.words[1] = expected_entry;
            copy_line_to_remote(request, &request_local);
            clflushopt_line((const void *)request);
            _mm_sfence();

            *request_queue_slot = expected_entry;
            clflushopt_line((const void *)request_queue_slot);
            _mm_sfence();

            send_seq++;
            advance_slot_cursor(&send_cursor, ring->slots);
        }

        {
            uint64_t slot = recv_cursor.slot;
            uint64_t expected_entry = slot_entry(slot, recv_cursor.phase);
            volatile uint64_t *response_queue_slot =
                queue_entry_ptr(ring->response_queue, slot);
            struct PayloadLine *response =
                payload_at_slot(ring->response_data, slot);
            struct PayloadLine response_local;

            for (;;) {
                if (read_queue_token_remote(response_queue_slot) ==
                    expected_entry)
                    break;
            }

            load_payload_remote(&response_local, response);
            if (response_local.words[0] != recv_seq + 1u ||
                response_local.words[1] !=
                    (expected_entry ^ 0xfeedfacecafebeefull)) {
                printf("correctness_fail seq=%" PRIu64
                       " slot=%" PRIu64
                       " phase=%" PRIu64
                       " got0=0x%016" PRIx64
                       " got1=0x%016" PRIx64
                       " expected0=0x%016" PRIx64
                       " expected1=0x%016" PRIx64 "\n",
                       recv_seq,
                       slot,
                       recv_cursor.phase,
                       response_local.words[0],
                       response_local.words[1],
                       (uint64_t)(recv_seq + 1u),
                       (uint64_t)(expected_entry ^ 0xfeedfacecafebeefull));
                return 1;
            }

            recv_seq++;
            advance_slot_cursor(&recv_cursor, ring->slots);
        }
    }

    printf("minrepro_done_count=%" PRIu64 "\n", request_count);
    return 0;
}

int
main(int argc, char **argv)
{
    uint64_t request_count = 40;
    uint64_t slot_count = 32;
    uint64_t inflight_limit = 2;
    int client_cpu = 0;
    int server_cpu = 1;
    int dump_layout = 0;
    long online_cpus = sysconf(_SC_NPROCESSORS_ONLN);
    struct Ring ring = {0};
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
        } else if (strcmp(argv[i], "--slots") == 0 && i + 1 < argc) {
            slot_count = parse_u64("slots", argv[++i]);
        } else if (strcmp(argv[i], "--inflight") == 0 && i + 1 < argc) {
            inflight_limit = parse_u64("inflight", argv[++i]);
        } else if (strcmp(argv[i], "--client-cpu") == 0 && i + 1 < argc) {
            client_cpu = (int)parse_u64("client-cpu", argv[++i]);
        } else if (strcmp(argv[i], "--server-cpu") == 0 && i + 1 < argc) {
            server_cpu = (int)parse_u64("server-cpu", argv[++i]);
        } else if (strcmp(argv[i], "--dump-layout") == 0) {
            dump_layout = 1;
        } else if (strcmp(argv[i], "--help") == 0 ||
                   strcmp(argv[i], "-h") == 0) {
            printf("Usage: %s [--count N] [--slots N] [--inflight N] "
                   "[--client-cpu N] [--server-cpu N] [--dump-layout]\n",
                   argv[0]);
            return 0;
        } else {
            fprintf(stderr, "unknown argument: %s\n", argv[i]);
            return 2;
        }
    }

    if (!cpu_has_clflushopt()) {
        fprintf(stderr, "clflushopt is required for cxl_clean_reuse_minrepro\n");
        return 2;
    }

    if (request_count == 0 || slot_count == 0 || inflight_limit == 0) {
        fprintf(stderr, "count, slots, inflight must be positive\n");
        return 2;
    }

    if (slot_count < inflight_limit) {
        fprintf(stderr,
                "slots (%" PRIu64 ") must be >= inflight (%" PRIu64 ")\n",
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

    ring.slots = slot_count;
    request_queue_size =
        sizeof(*ring.request_queue) * ring.slots * QUEUE_STRIDE_WORDS;
    response_queue_size =
        sizeof(*ring.response_queue) * ring.slots * QUEUE_STRIDE_WORDS;
    request_data_size = (size_t)(CACHELINE_SIZE * ring.slots);
    response_data_size = (size_t)(CACHELINE_SIZE * ring.slots);

    ring.request_queue = alloc_shared(request_queue_size);
    ring.response_queue = alloc_shared(response_queue_size);
    ring.request_data = alloc_shared(request_data_size);
    ring.response_data = alloc_shared(response_data_size);

    if (dump_layout) {
        dump_ring_layout(&ring,
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
        run_server(&ring, request_count);
        _exit(0);
    }

    close(ready_pipe[1]);
    ready_pipe[1] = -1;

    if (pin_to_cpu(client_cpu) != 0) {
        fprintf(stderr, "failed to pin client process to cpu %d\n", client_cpu);
        rc = 1;
        goto cleanup_server;
    }

    {
        char ready = 0;
        ssize_t ready_rc = read(ready_pipe[0], &ready, sizeof(ready));

        if (ready_rc != (ssize_t)sizeof(ready) || ready != 'R') {
            fprintf(stderr, "client ready wait failed\n");
            rc = 1;
            goto cleanup_server;
        }
    }

    close(ready_pipe[0]);
    ready_pipe[0] = -1;

    rc = run_client(&ring, request_count, inflight_limit);

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
        } else if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
            if (WIFSIGNALED(status))
                fprintf(stderr, "server process killed by signal %d\n",
                        WTERMSIG(status));
            else
                fprintf(stderr, "server process exited with failure\n");
            rc = 1;
        }
    }

cleanup:
    if (ready_pipe[0] != -1)
        close(ready_pipe[0]);
    if (ready_pipe[1] != -1)
        close(ready_pipe[1]);
    if (ring.request_queue != NULL)
        munmap((void *)ring.request_queue,
               shared_mapping_size(request_queue_size));
    if (ring.response_queue != NULL)
        munmap((void *)ring.response_queue,
               shared_mapping_size(response_queue_size));
    if (ring.request_data != NULL)
        munmap(ring.request_data, shared_mapping_size(request_data_size));
    if (ring.response_data != NULL)
        munmap(ring.response_data, shared_mapping_size(response_data_size));

    return rc;
}
