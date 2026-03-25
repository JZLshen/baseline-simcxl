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
#include <unistd.h>
#include <x86intrin.h>

#define CACHELINE_SIZE 64u
#define CPU_GHZ 2.4
#define DEFAULT_SLOTS 8u
#define PAGE_FALLBACK 4096u

static const size_t k_sizes[] = {
    8u,
    16u,
    32u,
    64u,
    96u,
    128u,
    192u,
    256u,
    384u,
    512u,
    768u,
    1024u,
    2048u,
    4096u,
    8192u,
    16384u,
    32768u,
    65536u,
    131072u,
    262144u,
    524288u,
    1048576u,
    2097152u,
    4194304u,
};

struct stats {
    uint64_t min_cycles;
    uint64_t max_cycles;
    double avg_cycles;
    uint64_t p50_cycles;
    uint64_t p95_cycles;
};

static inline uint64_t
round_up_u64(uint64_t value, uint64_t align)
{
    return ((value + align - 1u) / align) * align;
}

static inline double
cycles_to_ns(uint64_t cycles)
{
    return (double)cycles / CPU_GHZ;
}

static inline uint64_t
rdtscp_cycles(void)
{
    uint32_t lo = 0;
    uint32_t hi = 0;
    uint32_t aux = 0;

    _mm_lfence();
    asm volatile("rdtscp" : "=a"(lo), "=d"(hi), "=c"(aux) :: "memory");
    _mm_lfence();
    return ((uint64_t)hi << 32) | lo;
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

    if (!value[0] || (end != NULL && *end != '\0')) {
        fprintf(stderr, "invalid %s: %s\n", name, value);
        exit(2);
    }

    return (uint64_t)parsed;
}

static size_t
parse_size_or_die(const char *name, const char *value)
{
    char *end = NULL;
    unsigned long long parsed = strtoull(value, &end, 10);
    unsigned long long scale = 1u;

    if (!value[0]) {
        fprintf(stderr, "invalid %s: %s\n", name, value);
        exit(2);
    }

    if (end != NULL && *end != '\0') {
        if ((end[0] == 'k' || end[0] == 'K') && end[1] == '\0') {
            scale = 1024ull;
        } else if ((end[0] == 'm' || end[0] == 'M') && end[1] == '\0') {
            scale = 1024ull * 1024ull;
        } else {
            fprintf(stderr, "invalid %s suffix: %s\n", name, value);
            exit(2);
        }
    }

    return (size_t)(parsed * scale);
}

static void *
alloc_aligned(size_t size)
{
    void *ptr = NULL;

    if (posix_memalign(&ptr, CACHELINE_SIZE, size) != 0 || ptr == NULL) {
        fprintf(stderr, "allocation failed for %zu bytes\n", size);
        exit(1);
    }

    return ptr;
}

static uint64_t
virt_to_phys_addr(const void *ptr)
{
    uint64_t vaddr = (uint64_t)(uintptr_t)ptr;
    long page_size = sysconf(_SC_PAGESIZE);
    uint64_t page_u64 = (page_size > 0) ? (uint64_t)page_size : PAGE_FALLBACK;
    uint64_t page_index = vaddr / page_u64;
    uint64_t page_offset = vaddr % page_u64;
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

    return ((entry & ((1ull << 55) - 1ull)) * page_u64) + page_offset;
}

static void
write_pattern(volatile uint8_t *dst, size_t size, uint64_t seed)
{
    size_t offset = 0;

    while (offset + sizeof(uint64_t) <= size) {
        *(volatile uint64_t *)(void *)(dst + offset) = seed + (uint64_t)offset;
        offset += sizeof(uint64_t);
    }

    while (offset < size) {
        dst[offset] = (uint8_t)((seed + (uint64_t)offset) & 0xffu);
        offset++;
    }
}

static void
flush_range(const void *ptr, size_t size)
{
    const uint8_t *line = (const uint8_t *)ptr;
    size_t flushed = 0;
    size_t aligned_size = (size_t)round_up_u64((uint64_t)size, CACHELINE_SIZE);

    while (flushed < aligned_size) {
        clflushopt_line((const void *)(line + flushed));
        flushed += CACHELINE_SIZE;
    }
}

static void
prefault_region(volatile uint8_t *region, size_t region_bytes)
{
    long page_size = sysconf(_SC_PAGESIZE);
    size_t step = (page_size > 0) ? (size_t)page_size : PAGE_FALLBACK;
    size_t offset = 0;

    while (offset < region_bytes) {
        region[offset] = (uint8_t)(offset / step);
        clflushopt_line((const void *)(region + offset));
        offset += step;
    }

    if (region_bytes > 0) {
        size_t tail = region_bytes - 1u;
        region[tail] = (uint8_t)(tail & 0xffu);
        clflushopt_line((const void *)(region + (tail & ~(CACHELINE_SIZE - 1u))));
    }

    _mm_sfence();
}

static int
compare_u64(const void *lhs, const void *rhs)
{
    const uint64_t a = *(const uint64_t *)lhs;
    const uint64_t b = *(const uint64_t *)rhs;

    if (a < b)
        return -1;
    if (a > b)
        return 1;
    return 0;
}

static struct stats
compute_stats(uint64_t *samples, size_t count)
{
    struct stats out;
    uint64_t *sorted = NULL;
    uint64_t sum = 0;
    size_t p50_index = 0;
    size_t p95_index = 0;

    out.min_cycles = 0;
    out.max_cycles = 0;
    out.avg_cycles = 0.0;
    out.p50_cycles = 0;
    out.p95_cycles = 0;

    if (count == 0)
        return out;

    sorted = (uint64_t *)alloc_aligned(sizeof(*sorted) * count);
    memcpy(sorted, samples, sizeof(*sorted) * count);
    qsort(sorted, count, sizeof(*sorted), compare_u64);

    out.min_cycles = sorted[0];
    out.max_cycles = sorted[count - 1u];

    for (size_t i = 0; i < count; i++)
        sum += samples[i];

    out.avg_cycles = (double)sum / (double)count;
    p50_index = count / 2u;
    p95_index = (count * 95u + 99u) / 100u;
    if (p95_index == 0)
        p95_index = 1u;
    p95_index -= 1u;
    if (p95_index >= count)
        p95_index = count - 1u;

    out.p50_cycles = sorted[p50_index];
    out.p95_cycles = sorted[p95_index];

    free(sorted);
    return out;
}

static size_t
latency_iterations_for_aligned_size(size_t aligned_size)
{
    const size_t target_bytes = 256u * 1024u;
    size_t iters = target_bytes / aligned_size;

    if (iters < 8u)
        iters = 8u;
    if (iters > 384u)
        iters = 384u;
    return iters;
}

static size_t
bandwidth_iterations_for_aligned_size(size_t aligned_size)
{
    const size_t target_bytes = 4u * 1024u * 1024u;
    size_t iters = target_bytes / aligned_size;

    if (iters < 4u)
        iters = 4u;
    if (iters > 4096u)
        iters = 4096u;
    return iters;
}

static const char *
target_node_hint(uint64_t phys_addr)
{
    if (phys_addr >= 0x100000000ull)
        return "cxl_node1";
    if (phys_addr > 0)
        return "dram_node0";
    return "unknown";
}

int
main(int argc, char **argv)
{
    int cpu_id = 0;
    const char *mode_label = "unknown";
    size_t min_size = k_sizes[0];
    size_t max_size = k_sizes[(sizeof(k_sizes) / sizeof(k_sizes[0])) - 1u];
    long online_cpus = sysconf(_SC_NPROCESSORS_ONLN);

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--cpu") == 0 && i + 1 < argc) {
            cpu_id = (int)parse_u64("cpu", argv[++i]);
        } else if (strcmp(argv[i], "--mode-label") == 0 && i + 1 < argc) {
            mode_label = argv[++i];
        } else if (strcmp(argv[i], "--min-size") == 0 && i + 1 < argc) {
            min_size = parse_size_or_die("min-size", argv[++i]);
        } else if (strcmp(argv[i], "--max-size") == 0 && i + 1 < argc) {
            max_size = parse_size_or_die("max-size", argv[++i]);
        } else if (strcmp(argv[i], "--help") == 0 ||
                   strcmp(argv[i], "-h") == 0) {
            printf("Usage: %s [--cpu N] [--mode-label label] [--min-size N] [--max-size N]\n",
                   argv[0]);
            return 0;
        } else {
            fprintf(stderr, "unknown argument: %s\n", argv[i]);
            return 2;
        }
    }

    if (!cpu_has_clflushopt()) {
        fprintf(stderr, "clflushopt is required for write_size_sweep\n");
        return 2;
    }

    if (online_cpus <= cpu_id) {
        fprintf(stderr, "need online cpus > cpu, got %ld\n", online_cpus);
        return 2;
    }

    if (pin_to_cpu(cpu_id) != 0) {
        fprintf(stderr, "failed to pin process to cpu %d: %s\n",
                cpu_id,
                strerror(errno));
        return 1;
    }

    printf("WRITE_SWEEP_CONFIG,mode=%s,cpu=%d,cpu_ghz=%.1f,slots=%u,min_size=%zu,max_size=%zu\n",
           mode_label,
           cpu_id,
           CPU_GHZ,
           DEFAULT_SLOTS,
           min_size,
           max_size);

    for (size_t idx = 0; idx < sizeof(k_sizes) / sizeof(k_sizes[0]); idx++) {
        const size_t size_bytes = k_sizes[idx];
        const size_t aligned_size = (size_t)round_up_u64((uint64_t)size_bytes, CACHELINE_SIZE);
        const size_t slot_span = aligned_size;
        const size_t slots = DEFAULT_SLOTS;
        const size_t region_bytes = slot_span * slots;
        const size_t lat_iters = latency_iterations_for_aligned_size(aligned_size);
        const size_t bw_iters = bandwidth_iterations_for_aligned_size(aligned_size);
        volatile uint8_t *lat_region = NULL;
        volatile uint8_t *bw_region = NULL;
        uint64_t *lat_samples = NULL;
        uint64_t sample_pa = 0;
        struct stats lat_stats;
        uint64_t bw_start = 0;
        uint64_t bw_end = 0;
        double bw_payload_gib_s = 0.0;
        double bw_effective_gib_s = 0.0;
        uint64_t bw_total_payload = 0;
        uint64_t bw_total_effective = 0;

        if (size_bytes < min_size || size_bytes > max_size)
            continue;

        lat_region = (volatile uint8_t *)alloc_aligned(region_bytes);
        bw_region = (volatile uint8_t *)alloc_aligned(region_bytes);
        lat_samples = (uint64_t *)alloc_aligned(sizeof(*lat_samples) * lat_iters);

        prefault_region(lat_region, region_bytes);
        prefault_region(bw_region, region_bytes);

        sample_pa = virt_to_phys_addr((const void *)lat_region);

        for (size_t iter = 0; iter < lat_iters; iter++) {
            volatile uint8_t *dst = lat_region + ((iter % slots) * slot_span);
            uint64_t t0 = 0;
            uint64_t t1 = 0;

            t0 = rdtscp_cycles();
            write_pattern(dst, size_bytes, (uint64_t)(0x1000u + iter));
            flush_range((const void *)dst, size_bytes);
            _mm_sfence();
            t1 = rdtscp_cycles();
            lat_samples[iter] = t1 - t0;
        }

        lat_stats = compute_stats(lat_samples, lat_iters);

        bw_start = rdtscp_cycles();
        for (size_t iter = 0; iter < bw_iters; iter++) {
            volatile uint8_t *dst = bw_region + ((iter % slots) * slot_span);
            write_pattern(dst, size_bytes, (uint64_t)(0x500000u + iter));
            flush_range((const void *)dst, size_bytes);
        }
        _mm_sfence();
        bw_end = rdtscp_cycles();

        bw_total_payload = (uint64_t)bw_iters * (uint64_t)size_bytes;
        bw_total_effective = (uint64_t)bw_iters * (uint64_t)aligned_size;
        if (bw_end > bw_start) {
            double seconds = ((double)(bw_end - bw_start) / CPU_GHZ) / 1.0e9;
            bw_payload_gib_s = ((double)bw_total_payload / (1024.0 * 1024.0 * 1024.0)) / seconds;
            bw_effective_gib_s = ((double)bw_total_effective / (1024.0 * 1024.0 * 1024.0)) / seconds;
        }

        printf("WRITE_SWEEP,metric=latency,mode=%s,size_bytes=%zu,aligned_bytes=%zu,"
               "iterations=%zu,sample_target_pa=0x%016" PRIx64 ",target_node_hint=%s,"
               "avg_ns=%.3f,p50_ns=%.3f,p95_ns=%.3f,min_ns=%.3f,max_ns=%.3f\n",
               mode_label,
               size_bytes,
               aligned_size,
               lat_iters,
               sample_pa,
               target_node_hint(sample_pa),
               lat_stats.avg_cycles / CPU_GHZ,
               cycles_to_ns(lat_stats.p50_cycles),
               cycles_to_ns(lat_stats.p95_cycles),
               cycles_to_ns(lat_stats.min_cycles),
               cycles_to_ns(lat_stats.max_cycles));

        printf("WRITE_SWEEP,metric=bandwidth,mode=%s,size_bytes=%zu,aligned_bytes=%zu,"
               "iterations=%zu,sample_target_pa=0x%016" PRIx64 ",target_node_hint=%s,"
               "elapsed_ns=%.3f,total_payload_bytes=%" PRIu64 ",total_effective_bytes=%" PRIu64
               ",payload_gib_s=%.6f,effective_gib_s=%.6f\n",
               mode_label,
               size_bytes,
               aligned_size,
               bw_iters,
               sample_pa,
               target_node_hint(sample_pa),
               cycles_to_ns(bw_end - bw_start),
               bw_total_payload,
               bw_total_effective,
               bw_payload_gib_s,
               bw_effective_gib_s);

        free((void *)lat_region);
        free((void *)bw_region);
        free(lat_samples);
    }

    return 0;
}
