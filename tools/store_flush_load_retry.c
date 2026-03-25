#include <cpuid.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <x86intrin.h>

static inline uint64_t rdtscp_cycles(void)
{
    unsigned int aux = 0;
    _mm_lfence();
    uint64_t t = __rdtscp(&aux);
    _mm_lfence();
    return t;
}

static double read_cpu_mhz(void)
{
    return 2400.0;
}

static int cpu_has_clflushopt(void)
{
    unsigned int eax = 0, ebx = 0, ecx = 0, edx = 0;
    if (!__get_cpuid_count(7, 0, &eax, &ebx, &ecx, &edx)) {
        return 0;
    }
    return ((ebx >> 23) & 0x1U) ? 1 : 0;
}

static inline void clflush_line(const void *p)
{
    asm volatile("clflush (%0)" : : "r"(p) : "memory");
}

static inline void clflushopt_line(const void *p)
{
    asm volatile("clflushopt (%0)" : : "r"(p) : "memory");
}

int main(void)
{
    const uint64_t old_val = 0x1122334455667788ULL;
    const uint64_t new_val = 0xa5a55a5a0badf00dULL;
    const int max_attempts = 1000000;

    void *raw = NULL;
    if (posix_memalign(&raw, 64, 64) != 0) {
        perror("posix_memalign");
        return 1;
    }
    volatile uint64_t *p = (volatile uint64_t *)raw;

    const int has_clflushopt = cpu_has_clflushopt();
    const double mhz = read_cpu_mhz();
    const double cyc_to_ns = 1000.0 / mhz;

    p[0] = old_val;
    _mm_mfence();
    clflush_line((const void *)p);
    _mm_mfence();

    uint64_t base = rdtscp_cycles();

    p[0] = new_val;
    if (has_clflushopt) {
        clflushopt_line((const void *)p);
    } else {
        clflush_line((const void *)p);
    }
    _mm_sfence();
    uint64_t t_sfence_done = rdtscp_cycles() - base;

    int success = 0;
    int success_attempt = -1;
    uint64_t success_value = 0;
    uint64_t t_success_load_start = 0;
    uint64_t t_success_load_done = 0;

    uint64_t attempt1_value = 0;
    uint64_t t_attempt1_load_start = 0;
    uint64_t t_attempt1_load_done = 0;

    for (int attempt = 1; attempt <= max_attempts; attempt++) {
        if (has_clflushopt) {
            clflushopt_line((const void *)p);
        } else {
            clflush_line((const void *)p);
        }
        _mm_mfence();

        uint64_t t_load_start = rdtscp_cycles() - base;
        uint64_t got = p[0];
        _mm_lfence();
        uint64_t t_load_done = rdtscp_cycles() - base;

        if (attempt == 1) {
            attempt1_value = got;
            t_attempt1_load_start = t_load_start;
            t_attempt1_load_done = t_load_done;
        }

        if (got == new_val) {
            success = 1;
            success_attempt = attempt;
            success_value = got;
            t_success_load_start = t_load_start;
            t_success_load_done = t_load_done;
            break;
        }
    }

    printf("cpu_mhz=%.3f\n", mhz);
    printf("used_clflushopt=%d\n", has_clflushopt);
    printf("ptr=%p\n", raw);
    printf("target_value=0x%016lx\n", new_val);
    printf("t_sfence_done_cycles=%lu\n", t_sfence_done);
    printf("t_sfence_done_ns=%.3f\n", t_sfence_done * cyc_to_ns);

    printf("attempt1_value=0x%016lx\n", attempt1_value);
    printf("attempt1_load_start_cycles=%lu\n", t_attempt1_load_start);
    printf("attempt1_load_done_cycles=%lu\n", t_attempt1_load_done);
    printf("attempt1_load_latency_cycles=%lu\n",
           t_attempt1_load_done - t_attempt1_load_start);
    printf("attempt1_load_done_ns=%.3f\n", t_attempt1_load_done * cyc_to_ns);

    printf("success=%d\n", success);
    printf("success_attempt=%d\n", success_attempt);
    printf("success_value=0x%016lx\n", success_value);
    printf("success_load_start_cycles=%lu\n", t_success_load_start);
    printf("success_load_done_cycles=%lu\n", t_success_load_done);
    printf("success_load_latency_cycles=%lu\n",
           t_success_load_done - t_success_load_start);
    printf("success_load_done_ns=%.3f\n", t_success_load_done * cyc_to_ns);
    if (success) {
        uint64_t delta = t_success_load_done - t_sfence_done;
        printf("sfence_to_success_load_cycles=%lu\n", delta);
        printf("sfence_to_success_load_ns=%.3f\n", delta * cyc_to_ns);
    } else {
        printf("sfence_to_success_load_cycles=-1\n");
        printf("sfence_to_success_load_ns=-1\n");
    }

    free(raw);
    return success ? 0 : 2;
}
