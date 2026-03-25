#define _GNU_SOURCE

#include <cpuid.h>
#include <errno.h>
#include <pthread.h>
#include <sched.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdatomic.h>
#include <unistd.h>
#include <x86intrin.h>

struct SharedCtx {
    volatile uint64_t *ptr;
    uint64_t old_val;
    uint64_t new_val;
    int max_attempts;
    int has_clflushopt;

    atomic_int go;
    atomic_int ready_count;
    atomic_int writer_err;
    atomic_int reader_err;

    uint64_t base_cycles;

    uint64_t writer_store_start;
    uint64_t writer_sfence_done;

    uint64_t reader_first_value;
    uint64_t reader_loop_start;
    uint64_t reader_first_load_start;
    uint64_t reader_first_load_done;

    int reader_success;
    int reader_success_attempt;
    uint64_t reader_success_value;
    uint64_t reader_success_load_start;
    uint64_t reader_success_load_done;
};

static inline uint64_t rdtscp_cycles(void)
{
    unsigned int aux = 0;
    _mm_lfence();
    uint64_t t = __rdtscp(&aux);
    _mm_lfence();
    return t;
}

static double read_cpu_mhz(void) { return 2400.0; }

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

static inline void flush_line_for_remote_read(const struct SharedCtx *ctx)
{
    if (ctx->has_clflushopt) {
        clflushopt_line((const void *)ctx->ptr);
    } else {
        clflush_line((const void *)ctx->ptr);
    }
}

static int pin_to_cpu(int cpu)
{
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(cpu, &set);
    return pthread_setaffinity_np(pthread_self(), sizeof(set), &set);
}

static void *writer_thread(void *arg)
{
    struct SharedCtx *ctx = (struct SharedCtx *)arg;
    if (pin_to_cpu(0) != 0) {
        atomic_store_explicit(&ctx->writer_err, 1, memory_order_release);
        return (void *)(intptr_t)1;
    }
    atomic_fetch_add_explicit(&ctx->ready_count, 1, memory_order_release);

    while (atomic_load_explicit(&ctx->go, memory_order_acquire) == 0) {
        _mm_pause();
    }

    ctx->writer_store_start = rdtscp_cycles() - ctx->base_cycles;
    ctx->ptr[0] = ctx->new_val;
    if (ctx->has_clflushopt) {
        clflushopt_line((const void *)ctx->ptr);
    } else {
        clflush_line((const void *)ctx->ptr);
    }
    _mm_sfence();

    ctx->writer_sfence_done = rdtscp_cycles() - ctx->base_cycles;
    return 0;
}

static void *reader_thread(void *arg)
{
    struct SharedCtx *ctx = (struct SharedCtx *)arg;
    if (pin_to_cpu(1) != 0) {
        atomic_store_explicit(&ctx->reader_err, 1, memory_order_release);
        return (void *)(intptr_t)1;
    }
    atomic_fetch_add_explicit(&ctx->ready_count, 1, memory_order_release);

    while (atomic_load_explicit(&ctx->go, memory_order_acquire) == 0) {
        _mm_pause();
    }
    ctx->reader_loop_start = rdtscp_cycles() - ctx->base_cycles;

    ctx->reader_success = 0;
    ctx->reader_success_attempt = -1;
    ctx->reader_success_value = 0;
    ctx->reader_success_load_start = 0;
    ctx->reader_success_load_done = 0;

    for (int attempt = 1; attempt <= ctx->max_attempts; attempt++) {
        flush_line_for_remote_read(ctx);
        _mm_mfence();

        uint64_t t_load_start = rdtscp_cycles() - ctx->base_cycles;
        uint64_t got = ctx->ptr[0];
        _mm_lfence();
        uint64_t t_load_done = rdtscp_cycles() - ctx->base_cycles;

        if (attempt == 1) {
            ctx->reader_first_value = got;
            ctx->reader_first_load_start = t_load_start;
            ctx->reader_first_load_done = t_load_done;
        }

        if (got == ctx->new_val) {
            ctx->reader_success = 1;
            ctx->reader_success_attempt = attempt;
            ctx->reader_success_value = got;
            ctx->reader_success_load_start = t_load_start;
            ctx->reader_success_load_done = t_load_done;
            break;
        }
    }
    return 0;
}

int main(void)
{
    const int online_cpus = (int)sysconf(_SC_NPROCESSORS_ONLN);
    if (online_cpus < 2) {
        fprintf(stderr, "need >=2 CPUs, online_cpus=%d\n", online_cpus);
        return 3;
    }

    struct SharedCtx ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.old_val = 0x1122334455667788ULL;
    ctx.new_val = 0xa5a55a5a0badf00dULL;
    // Keep runtime bounded under timing simulation while still allowing retries.
    ctx.max_attempts = 10000;
    ctx.has_clflushopt = cpu_has_clflushopt();
    atomic_init(&ctx.go, 0);
    atomic_init(&ctx.ready_count, 0);
    atomic_init(&ctx.writer_err, 0);
    atomic_init(&ctx.reader_err, 0);

    void *raw = NULL;
    if (posix_memalign(&raw, 64, 64) != 0) {
        perror("posix_memalign");
        return 1;
    }
    ctx.ptr = (volatile uint64_t *)raw;

    ctx.ptr[0] = ctx.old_val;
    _mm_mfence();
    flush_line_for_remote_read(&ctx);
    _mm_mfence();

    pthread_t tw, tr;
    if (pthread_create(&tw, NULL, writer_thread, &ctx) != 0) {
        perror("pthread_create writer");
        free(raw);
        return 1;
    }
    if (pthread_create(&tr, NULL, reader_thread, &ctx) != 0) {
        perror("pthread_create reader");
        free(raw);
        return 1;
    }

    while (atomic_load_explicit(&ctx.ready_count, memory_order_acquire) < 2 &&
           atomic_load_explicit(&ctx.writer_err, memory_order_acquire) == 0 &&
           atomic_load_explicit(&ctx.reader_err, memory_order_acquire) == 0) {
        _mm_pause();
    }
    if (atomic_load_explicit(&ctx.writer_err, memory_order_acquire) != 0 ||
        atomic_load_explicit(&ctx.reader_err, memory_order_acquire) != 0) {
        pthread_join(tw, NULL);
        pthread_join(tr, NULL);
        free(raw);
        fprintf(stderr, "thread readiness failed before benchmark start\n");
        return 4;
    }

    ctx.base_cycles = rdtscp_cycles();
    atomic_store_explicit(&ctx.go, 1, memory_order_release);

    void *wret = NULL;
    void *rret = NULL;
    pthread_join(tw, &wret);
    pthread_join(tr, &rret);
    if ((intptr_t)wret != 0 || (intptr_t)rret != 0) {
        fprintf(stderr, "thread affinity failed: writer=%ld reader=%ld\n",
                (long)(intptr_t)wret, (long)(intptr_t)rret);
        free(raw);
        return 4;
    }

    const double mhz = read_cpu_mhz();
    const double cyc_to_ns = 1000.0 / mhz;

    printf("online_cpus=%d\n", online_cpus);
    printf("cpu_mhz=%.3f\n", mhz);
    printf("used_clflushopt=%d\n", ctx.has_clflushopt);
    printf("writer_cpu=0\n");
    printf("reader_cpu=1\n");
    printf("ptr=%p\n", raw);
    printf("target_value=0x%016lx\n", ctx.new_val);
    printf("writer_store_start_cycles=%lu\n", ctx.writer_store_start);
    printf("writer_sfence_done_cycles=%lu\n", ctx.writer_sfence_done);
    printf("writer_sfence_done_ns=%.3f\n", ctx.writer_sfence_done * cyc_to_ns);

    printf("reader_first_value=0x%016lx\n", ctx.reader_first_value);
    printf("reader_loop_start_cycles=%lu\n", ctx.reader_loop_start);
    printf("reader_loop_start_ns=%.3f\n", ctx.reader_loop_start * cyc_to_ns);
    printf("reader_first_load_start_cycles=%lu\n", ctx.reader_first_load_start);
    printf("reader_first_load_done_cycles=%lu\n", ctx.reader_first_load_done);
    printf("reader_first_load_latency_cycles=%lu\n",
           ctx.reader_first_load_done - ctx.reader_first_load_start);
    printf("reader_first_load_done_ns=%.3f\n",
           ctx.reader_first_load_done * cyc_to_ns);

    printf("reader_success=%d\n", ctx.reader_success);
    printf("reader_success_attempt=%d\n", ctx.reader_success_attempt);
    printf("reader_success_value=0x%016lx\n", ctx.reader_success_value);
    printf("reader_success_load_start_cycles=%lu\n", ctx.reader_success_load_start);
    printf("reader_success_load_done_cycles=%lu\n", ctx.reader_success_load_done);
    printf("reader_success_load_latency_cycles=%lu\n",
           ctx.reader_success_load_done - ctx.reader_success_load_start);
    printf("reader_success_load_done_ns=%.3f\n",
           ctx.reader_success_load_done * cyc_to_ns);

    if (ctx.reader_success) {
        uint64_t delta = ctx.reader_success_load_done - ctx.writer_sfence_done;
        printf("sfence_to_reader_success_load_cycles=%lu\n", delta);
        printf("sfence_to_reader_success_load_ns=%.3f\n", delta * cyc_to_ns);
    } else {
        printf("sfence_to_reader_success_load_cycles=-1\n");
        printf("sfence_to_reader_success_load_ns=-1\n");
    }

    free(raw);
    return ctx.reader_success ? 0 : 2;
}
