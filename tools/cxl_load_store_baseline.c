#define _GNU_SOURCE

#include <cpuid.h>
#include <errno.h>
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

static inline uint64_t
round_up_u64(uint64_t value, uint64_t align)
{
    return (value + align - 1) / align * align;
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

static inline uint64_t
read_remote_word(volatile uint64_t *ptr)
{
    clflushopt_line((const void *)ptr);
    _mm_mfence();
    return *ptr;
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

static void *
alloc_zeroed_aligned(size_t size)
{
    void *ptr = NULL;
    size_t alloc_size = (size_t)round_up_u64((uint64_t)size, CACHELINE_SIZE);

    if (posix_memalign(&ptr, CACHELINE_SIZE, alloc_size) != 0 || !ptr) {
        fprintf(stderr, "allocation failed for %zu bytes\n", size);
        exit(1);
    }

    memset(ptr, 0, alloc_size);
    return ptr;
}

static uint64_t
next_rand(uint64_t *state)
{
    *state = (*state * 6364136223846793005ull) + 1442695040888963407ull;
    return *state;
}

int
main(int argc, char **argv)
{
    uint64_t count = 1;
    int cpu_id = 0;
    long online_cpus = sysconf(_SC_NPROCESSORS_ONLN);
    uint64_t rng_state = 1;
    uint64_t *store_expected = NULL;
    uint64_t *load_expected = NULL;
    uint64_t *store_start_tsc = NULL;
    uint64_t *store_end_tsc = NULL;
    uint64_t *load_start_tsc = NULL;
    uint64_t *load_end_tsc = NULL;
    uint8_t *remote_region = NULL;
    volatile uint64_t *store_word = NULL;
    volatile uint64_t *load_word = NULL;
    int rc = 0;

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--count") == 0 && i + 1 < argc) {
            count = parse_u64("count", argv[++i]);
        } else if (strcmp(argv[i], "--cpu") == 0 && i + 1 < argc) {
            cpu_id = (int)parse_u64("cpu", argv[++i]);
        } else if (strcmp(argv[i], "--help") == 0 ||
                   strcmp(argv[i], "-h") == 0) {
            printf("Usage: %s [--count N] [--cpu N]\n", argv[0]);
            return 0;
        } else {
            fprintf(stderr, "unknown argument: %s\n", argv[i]);
            return 2;
        }
    }

    if (!cpu_has_clflushopt()) {
        fprintf(stderr, "clflushopt is required for cxl_load_store_baseline\n");
        return 2;
    }

    if (count == 0) {
        fprintf(stderr, "count must be positive\n");
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

    remote_region = alloc_zeroed_aligned(CACHELINE_SIZE * 2);
    store_word = (volatile uint64_t *)(void *)(remote_region + 0);
    load_word = (volatile uint64_t *)(void *)(remote_region + CACHELINE_SIZE);

    store_expected = alloc_zeroed_aligned(sizeof(*store_expected) * count);
    load_expected = alloc_zeroed_aligned(sizeof(*load_expected) * count);
    store_start_tsc = alloc_zeroed_aligned(sizeof(*store_start_tsc) * count);
    store_end_tsc = alloc_zeroed_aligned(sizeof(*store_end_tsc) * count);
    load_start_tsc = alloc_zeroed_aligned(sizeof(*load_start_tsc) * count);
    load_end_tsc = alloc_zeroed_aligned(sizeof(*load_end_tsc) * count);

    for (uint64_t i = 0; i < count; i++) {
        store_expected[i] = next_rand(&rng_state);
        load_expected[i] = next_rand(&rng_state);
    }

    for (uint64_t i = 0; i < count; i++) {
        uint64_t store_value = store_expected[i];
        uint64_t load_value = load_expected[i];
        uint64_t observed_store = 0;
        uint64_t observed_load = 0;
        uint64_t store_delta_tsc = 0;
        uint64_t load_delta_tsc = 0;

        *store_word = 0;
        clflushopt_line((const void *)store_word);
        _mm_sfence();

        uint64_t store_start = rdtscp_cycles();
        *store_word = store_value;
        clflushopt_line((const void *)store_word);
        _mm_sfence();
        uint64_t store_end = rdtscp_cycles();

        store_start_tsc[i] = store_start;
        store_end_tsc[i] = store_end;
        store_delta_tsc = store_end_tsc[i] - store_start_tsc[i];

        observed_store = read_remote_word(store_word);

        *load_word = load_value;
        clflushopt_line((const void *)load_word);
        _mm_sfence();

        uint64_t load_start = rdtscp_cycles();
        observed_load = read_remote_word(load_word);
        uint64_t load_end = rdtscp_cycles();

        load_start_tsc[i] = load_start;
        load_end_tsc[i] = load_end;
        load_delta_tsc = load_end_tsc[i] - load_start_tsc[i];

        printf("iter_%" PRIu64 "_store_start_tsc=%" PRIu64 "\n",
               i,
               store_start_tsc[i]);
        printf("iter_%" PRIu64 "_store_end_tsc=%" PRIu64 "\n",
               i,
               store_end_tsc[i]);
        printf("iter_%" PRIu64 "_store_delta_tsc=%" PRIu64 "\n", i, store_delta_tsc);
        printf("iter_%" PRIu64 "_store_delta_ns=%.3f\n",
               i,
               cycles_to_ns(store_delta_tsc));
        if (observed_store != store_value) {
            printf("iter_%" PRIu64 "_store_correctness_fail expected=%" PRIu64
                   " actual=%" PRIu64 "\n",
                   i,
                   store_value,
                   observed_store);
            rc = 1;
        }

        printf("iter_%" PRIu64 "_load_start_tsc=%" PRIu64 "\n",
               i,
               load_start_tsc[i]);
        printf("iter_%" PRIu64 "_load_end_tsc=%" PRIu64 "\n",
               i,
               load_end_tsc[i]);
        printf("iter_%" PRIu64 "_load_delta_tsc=%" PRIu64 "\n", i, load_delta_tsc);
        printf("iter_%" PRIu64 "_load_delta_ns=%.3f\n",
               i,
               cycles_to_ns(load_delta_tsc));
        if (observed_load != load_value) {
            printf("iter_%" PRIu64 "_load_correctness_fail expected=%" PRIu64
                   " actual=%" PRIu64 "\n",
                   i,
                   load_value,
                   observed_load);
            rc = 1;
        }
    }

    free(remote_region);
    free(store_expected);
    free(load_expected);
    free(store_start_tsc);
    free(store_end_tsc);
    free(load_start_tsc);
    free(load_end_tsc);

    return rc;
}
