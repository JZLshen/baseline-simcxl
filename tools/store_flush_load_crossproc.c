#define _GNU_SOURCE

#include <cpuid.h>
#include <errno.h>
#include <inttypes.h>
#include <sched.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdatomic.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <unistd.h>
#include <x86intrin.h>

struct SharedCtx {
    volatile uint64_t data __attribute__((aligned(64)));
    uint64_t old_val;
    uint64_t new_val;
    int max_attempts;
    int has_clflushopt;

    atomic_int go;
    atomic_int writer_err;
    atomic_int reader_err;
    atomic_int ready_count;

    uint64_t base_cycles;

    int64_t writer_store_start;
    int64_t writer_sfence_done;

    uint64_t reader_first_value;
    int64_t reader_loop_start;
    int64_t reader_first_load_start;
    int64_t reader_first_load_done;
    int reader_total_attempts;

    int reader_success;
    int reader_success_attempt;
    uint64_t reader_success_value;
    int64_t reader_success_load_start;
    int64_t reader_success_load_done;
};

static inline uint64_t
rdtscp_cycles(void)
{
    unsigned int aux = 0;
    _mm_lfence();
    uint64_t t = __rdtscp(&aux);
    _mm_lfence();
    return t;
}

static double read_cpu_mhz(void) { return 2400.0; }

static int
cpu_has_clflushopt(void)
{
    unsigned int eax = 0, ebx = 0, ecx = 0, edx = 0;
    if (!__get_cpuid_count(7, 0, &eax, &ebx, &ecx, &edx)) {
        return 0;
    }
    return ((ebx >> 23) & 0x1U) ? 1 : 0;
}

static inline void
clflush_line(const void *p)
{
    asm volatile("clflush (%0)" : : "r"(p) : "memory");
}

static inline void
clflushopt_line(const void *p)
{
    asm volatile("clflushopt (%0)" : : "r"(p) : "memory");
}

static inline void
flush_line_for_remote_read(const struct SharedCtx *ctx)
{
    if (ctx->has_clflushopt) {
        clflushopt_line((const void *)&ctx->data);
    } else {
        clflush_line((const void *)&ctx->data);
    }
}

static int
pin_process_to_cpu(int cpu)
{
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(cpu, &set);
    return sched_setaffinity(0, sizeof(set), &set);
}

static int
writer_proc(struct SharedCtx *ctx)
{
    if (pin_process_to_cpu(0) != 0) {
        return 1;
    }
    atomic_fetch_add_explicit(&ctx->ready_count, 1, memory_order_release);

    while (atomic_load_explicit(&ctx->go, memory_order_acquire) == 0) {
        _mm_pause();
    }

    ctx->writer_store_start =
        (int64_t)rdtscp_cycles() - (int64_t)ctx->base_cycles;
    ctx->data = ctx->new_val;
    if (ctx->has_clflushopt) {
        clflushopt_line((const void *)&ctx->data);
    } else {
        clflush_line((const void *)&ctx->data);
    }
    _mm_sfence();

    ctx->writer_sfence_done =
        (int64_t)rdtscp_cycles() - (int64_t)ctx->base_cycles;
    return 0;
}

static int
reader_proc(struct SharedCtx *ctx)
{
    if (pin_process_to_cpu(1) != 0) {
        return 1;
    }
    atomic_fetch_add_explicit(&ctx->ready_count, 1, memory_order_release);

    while (atomic_load_explicit(&ctx->go, memory_order_acquire) == 0) {
        _mm_pause();
    }
    ctx->reader_loop_start =
        (int64_t)rdtscp_cycles() - (int64_t)ctx->base_cycles;

    ctx->reader_success = 0;
    ctx->reader_success_attempt = -1;
    ctx->reader_success_value = 0;
    ctx->reader_success_load_start = 0;
    ctx->reader_success_load_done = 0;
    ctx->reader_total_attempts = 0;

    for (int attempt = 1; attempt <= ctx->max_attempts; attempt++) {
        ctx->reader_total_attempts = attempt;
        flush_line_for_remote_read(ctx);
        _mm_mfence();

        int64_t t_load_start =
            (int64_t)rdtscp_cycles() - (int64_t)ctx->base_cycles;
        uint64_t got = ctx->data;
        _mm_lfence();
        int64_t t_load_done =
            (int64_t)rdtscp_cycles() - (int64_t)ctx->base_cycles;

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

int
main(void)
{
    const int online_cpus = (int)sysconf(_SC_NPROCESSORS_ONLN);
    if (online_cpus < 2) {
        fprintf(stderr, "need >=2 CPUs, online_cpus=%d\n", online_cpus);
        return 3;
    }

    struct SharedCtx *ctx = mmap(
        NULL, sizeof(*ctx), PROT_READ | PROT_WRITE,
        MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    if (ctx == MAP_FAILED) {
        perror("mmap");
        return 1;
    }
    memset(ctx, 0, sizeof(*ctx));

    ctx->old_val = 0x1122334455667788ULL;
    ctx->new_val = 0xa5a55a5a0badf00dULL;
    ctx->max_attempts = 10000;
    ctx->has_clflushopt = cpu_has_clflushopt();
    atomic_init(&ctx->go, 0);
    atomic_init(&ctx->writer_err, 0);
    atomic_init(&ctx->reader_err, 0);
    atomic_init(&ctx->ready_count, 0);

    ctx->data = ctx->old_val;
    _mm_mfence();
    flush_line_for_remote_read(ctx);
    _mm_mfence();

    pid_t writer_pid = fork();
    if (writer_pid == -1) {
        perror("fork writer");
        munmap(ctx, sizeof(*ctx));
        return 1;
    }
    if (writer_pid == 0) {
        int rc = writer_proc(ctx);
        atomic_store_explicit(&ctx->writer_err, rc, memory_order_release);
        _exit(rc ? 11 : 0);
    }

    pid_t reader_pid = fork();
    if (reader_pid == -1) {
        perror("fork reader");
        kill(writer_pid, SIGKILL);
        waitpid(writer_pid, NULL, 0);
        munmap(ctx, sizeof(*ctx));
        return 1;
    }
    if (reader_pid == 0) {
        int rc = reader_proc(ctx);
        atomic_store_explicit(&ctx->reader_err, rc, memory_order_release);
        _exit(rc ? 12 : 0);
    }

    while (atomic_load_explicit(&ctx->ready_count, memory_order_acquire) < 2 &&
           atomic_load_explicit(&ctx->writer_err, memory_order_acquire) == 0 &&
           atomic_load_explicit(&ctx->reader_err, memory_order_acquire) == 0) {
        _mm_pause();
    }
    if (atomic_load_explicit(&ctx->writer_err, memory_order_acquire) != 0 ||
        atomic_load_explicit(&ctx->reader_err, memory_order_acquire) != 0) {
        kill(writer_pid, SIGKILL);
        kill(reader_pid, SIGKILL);
        waitpid(writer_pid, NULL, 0);
        waitpid(reader_pid, NULL, 0);
        munmap(ctx, sizeof(*ctx));
        fprintf(stderr, "process readiness failed before benchmark start\n");
        return 4;
    }

    ctx->base_cycles = rdtscp_cycles();
    atomic_store_explicit(&ctx->go, 1, memory_order_release);

    int st_writer = 0;
    int st_reader = 0;
    waitpid(writer_pid, &st_writer, 0);
    waitpid(reader_pid, &st_reader, 0);

    const int writer_err =
        atomic_load_explicit(&ctx->writer_err, memory_order_acquire);
    const int reader_err =
        atomic_load_explicit(&ctx->reader_err, memory_order_acquire);

    if (!WIFEXITED(st_writer) || WEXITSTATUS(st_writer) != 0 ||
        !WIFEXITED(st_reader) || WEXITSTATUS(st_reader) != 0 ||
        writer_err != 0 || reader_err != 0) {
        fprintf(stderr,
                "child failed: writer_exit=%d reader_exit=%d writer_err=%d reader_err=%d\n",
                WIFEXITED(st_writer) ? WEXITSTATUS(st_writer) : -1,
                WIFEXITED(st_reader) ? WEXITSTATUS(st_reader) : -1,
                writer_err, reader_err);
        munmap(ctx, sizeof(*ctx));
        return 4;
    }

    const double mhz = read_cpu_mhz();
    const double cyc_to_ns = 1000.0 / mhz;

    printf("online_cpus=%d\n", online_cpus);
    printf("cpu_mhz=%.3f\n", mhz);
    printf("used_clflushopt=%d\n", ctx->has_clflushopt);
    printf("writer_cpu=0\n");
    printf("reader_cpu=1\n");
    printf("writer_pid=%d\n", (int)writer_pid);
    printf("reader_pid=%d\n", (int)reader_pid);
    printf("ptr=%p\n", (void *)&ctx->data);
    printf("target_value=0x%016lx\n", ctx->new_val);
    printf("writer_store_start_cycles=%" PRId64 "\n", ctx->writer_store_start);
    printf("writer_sfence_done_cycles=%" PRId64 "\n", ctx->writer_sfence_done);
    printf("writer_sfence_done_ns=%.3f\n", ctx->writer_sfence_done * cyc_to_ns);

    printf("reader_first_value=0x%016lx\n", ctx->reader_first_value);
    printf("reader_loop_start_cycles=%" PRId64 "\n", ctx->reader_loop_start);
    printf("reader_loop_start_ns=%.3f\n", ctx->reader_loop_start * cyc_to_ns);
    printf("reader_first_load_start_cycles=%" PRId64 "\n",
           ctx->reader_first_load_start);
    printf("reader_first_load_done_cycles=%" PRId64 "\n",
           ctx->reader_first_load_done);
    printf("reader_first_load_latency_cycles=%" PRId64 "\n",
           ctx->reader_first_load_done - ctx->reader_first_load_start);
    printf("reader_first_load_done_ns=%.3f\n",
           ctx->reader_first_load_done * cyc_to_ns);

    printf("reader_success=%d\n", ctx->reader_success);
    printf("reader_success_attempt=%d\n", ctx->reader_success_attempt);
    printf("reader_total_attempts=%d\n", ctx->reader_total_attempts);
    printf("reader_success_value=0x%016lx\n", ctx->reader_success_value);
    printf("reader_success_load_start_cycles=%" PRId64 "\n",
           ctx->reader_success_load_start);
    printf("reader_success_load_done_cycles=%" PRId64 "\n",
           ctx->reader_success_load_done);
    printf("reader_success_load_latency_cycles=%" PRId64 "\n",
           ctx->reader_success_load_done - ctx->reader_success_load_start);
    printf("reader_success_load_done_ns=%.3f\n",
           ctx->reader_success_load_done * cyc_to_ns);

    if (ctx->reader_success) {
        const int64_t loop_to_success =
            (int64_t)ctx->reader_success_load_done - (int64_t)ctx->reader_loop_start;
        const int64_t store_to_success =
            (int64_t)ctx->reader_success_load_done - (int64_t)ctx->writer_store_start;
        const int64_t sfence_to_success =
            (int64_t)ctx->reader_success_load_done - (int64_t)ctx->writer_sfence_done;

        printf("reader_loop_to_reader_success_load_cycles=%" PRId64 "\n",
               loop_to_success);
        printf("reader_loop_to_reader_success_load_ns=%.3f\n",
               loop_to_success * cyc_to_ns);
        printf("crossproc_store_to_reader_success_load_cycles=%" PRId64 "\n",
               store_to_success);
        printf("crossproc_store_to_reader_success_load_ns=%.3f\n",
               store_to_success * cyc_to_ns);
        printf("crossproc_sfence_to_reader_success_load_cycles=%" PRId64 "\n",
               sfence_to_success);
        printf("crossproc_sfence_to_reader_success_load_ns=%.3f\n",
               sfence_to_success * cyc_to_ns);
    } else {
        printf("reader_loop_to_reader_success_load_cycles=-1\n");
        printf("reader_loop_to_reader_success_load_ns=-1\n");
        printf("crossproc_store_to_reader_success_load_cycles=-1\n");
        printf("crossproc_store_to_reader_success_load_ns=-1\n");
        printf("crossproc_sfence_to_reader_success_load_cycles=-1\n");
        printf("crossproc_sfence_to_reader_success_load_ns=-1\n");
    }

    int ret = ctx->reader_success ? 0 : 2;
    munmap(ctx, sizeof(*ctx));
    return ret;
}
