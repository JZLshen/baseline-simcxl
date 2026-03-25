#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <unistd.h>
#include <x86intrin.h>

#define X86_M5OPS_PADDR 0xffff0000ull
#define X86_M5OPS_SIZE 0x10000u
#define M5OP_RPNS 0x07u
#define SIM_CPU_MHZ 2400.0

static volatile uint8_t *m5ops_base = NULL;

static inline uint64_t rdtscp_cycles(void) {
    unsigned int aux = 0;
    return __rdtscp(&aux);
}

static inline uint64_t m5_rpns_ns(void) {
    volatile uint64_t *rpns_addr =
        (volatile uint64_t *)(const void *)(m5ops_base + ((uint64_t)M5OP_RPNS << 8));
    return *rpns_addr;
}

static void map_m5ops_or_die(void) {
    int fd = open("/dev/mem", O_RDWR | O_SYNC);
    void *mapping = NULL;

    if (fd < 0) {
        perror("open /dev/mem");
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
        perror("mmap m5ops");
        exit(1);
    }

    m5ops_base = (volatile uint8_t *)mapping;
}

static void unmap_m5ops(void) {
    if (m5ops_base != NULL) {
        munmap((void *)(uintptr_t)m5ops_base, X86_M5OPS_SIZE);
        m5ops_base = NULL;
    }
}

static uint64_t virt_to_phys_addr(const void *ptr) {
    uint64_t vaddr = (uint64_t)(uintptr_t)ptr;
    long page_size = sysconf(_SC_PAGESIZE);
    uint64_t page_u64 = (page_size > 0) ? (uint64_t)page_size : 4096u;
    uint64_t page_index = vaddr / page_u64;
    uint64_t page_offset = vaddr % page_u64;
    uint64_t entry = 0;
    int fd = open("/proc/self/pagemap", O_RDONLY);

    if (fd < 0) {
        return 0;
    }
    if (pread(fd, &entry, sizeof(entry), (off_t)(page_index * sizeof(entry))) !=
        (ssize_t)sizeof(entry)) {
        close(fd);
        return 0;
    }
    close(fd);
    if (((entry >> 63) & 0x1u) == 0) {
        return 0;
    }
    return ((entry & ((1ull << 55) - 1)) * page_u64) + page_offset;
}

int main(void) {
    const int iters = 512;
    void *raw = NULL;
    volatile uint64_t *measure_line = NULL;
    uint64_t target_pa = 0;
    uint64_t trace_window_start_ns = 0;
    uint64_t trace_window_end_ns = 0;

    if (posix_memalign(&raw, 64, 64) != 0) {
        perror("posix_memalign");
        return 1;
    }

    measure_line = (volatile uint64_t *)raw;
    map_m5ops_or_die();

    uint64_t min_cyc = UINT64_MAX;
    uint64_t max_cyc = 0;
    uint64_t sum_cyc = 0;
    uint64_t first_cyc = 0;

    trace_window_start_ns = m5_rpns_ns();

    for (int i = 0; i < iters; i++) {
        uint64_t t0 = rdtscp_cycles();
        measure_line[0] = (uint64_t)(i + 1);
        measure_line[1] = (uint64_t)(i + 2);
        measure_line[2] = (uint64_t)(i + 3);
        measure_line[3] = (uint64_t)(i + 4);
        measure_line[4] = (uint64_t)(i + 5);
        measure_line[5] = (uint64_t)(i + 6);
        measure_line[6] = (uint64_t)(i + 7);
        measure_line[7] = (uint64_t)(i + 8);
        _mm_mfence();
        uint64_t t1 = rdtscp_cycles();
        _mm_clflush((const void *)measure_line);
        _mm_mfence();

        if (target_pa == 0) {
            target_pa = virt_to_phys_addr((const void *)measure_line);
        }

        uint64_t d = t1 - t0;
        if (first_cyc == 0) {
            first_cyc = d;
        }
        if (d < min_cyc) {
            min_cyc = d;
        }
        if (d > max_cyc) {
            max_cyc = d;
        }
        sum_cyc += d;
    }

    trace_window_end_ns = m5_rpns_ns();

    if (target_pa == 0) {
        fprintf(stderr, "failed to resolve target_pa after measured stores\n");
        unmap_m5ops();
        free(raw);
        return 1;
    }

    double mhz = SIM_CPU_MHZ;
    double cyc_to_ns = 1000.0 / mhz;
    double avg_cyc = (double)sum_cyc / (double)iters;

    printf("target_pa=0x%016lx\n", target_pa);
    printf("trace_window_start_ns=%lu\n", trace_window_start_ns);
    printf("trace_window_end_ns=%lu\n", trace_window_end_ns);
    printf("cpu_mhz=%.3f\n", mhz);
    printf("samples=%d\n", iters);
    printf("first_cycles=%lu\n", first_cyc);
    printf("avg_cycles=%.3f\n", avg_cyc);
    printf("min_cycles=%lu\n", min_cyc);
    printf("max_cycles=%lu\n", max_cyc);
    printf("first_ns=%.3f\n", first_cyc * cyc_to_ns);
    printf("avg_ns=%.3f\n", avg_cyc * cyc_to_ns);
    printf("min_ns=%.3f\n", min_cyc * cyc_to_ns);
    printf("max_ns=%.3f\n", max_cyc * cyc_to_ns);

    unmap_m5ops();
    free(raw);
    return 0;
}
