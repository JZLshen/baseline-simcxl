#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <string>
#include <vector>
#include <x86intrin.h>

static inline uint64_t rdtscp_cycles() {
    unsigned int aux;
    return __rdtscp(&aux);
}

static double read_cpu_mhz() {
    std::ifstream in("/proc/cpuinfo");
    std::string line;
    while (std::getline(in, line)) {
        const std::string key = "cpu MHz";
        if (line.rfind(key, 0) == 0) {
            auto pos = line.find(":");
            if (pos != std::string::npos) {
                return std::stod(line.substr(pos + 1));
            }
        }
    }
    return 2400.0;
}

int main() {
    constexpr int kWarmup = 200;
    constexpr int kIters = 5000;

    void *raw = nullptr;
    if (posix_memalign(&raw, 64, 64) != 0) {
        std::perror("posix_memalign");
        return 1;
    }

    volatile uint64_t *p = reinterpret_cast<volatile uint64_t *>(raw);
    for (int i = 0; i < 8; ++i) {
        p[i] = static_cast<uint64_t>(i);
    }

    std::vector<uint64_t> lats;
    lats.reserve(kIters);

    for (int i = 0; i < kWarmup + kIters; ++i) {
        _mm_clflush(const_cast<uint64_t *>(&p[0]));
        _mm_mfence();

        uint64_t t0 = rdtscp_cycles();
        p[0] = static_cast<uint64_t>(i + 1);
        p[1] = static_cast<uint64_t>(i + 2);
        p[2] = static_cast<uint64_t>(i + 3);
        p[3] = static_cast<uint64_t>(i + 4);
        p[4] = static_cast<uint64_t>(i + 5);
        p[5] = static_cast<uint64_t>(i + 6);
        p[6] = static_cast<uint64_t>(i + 7);
        p[7] = static_cast<uint64_t>(i + 8);
        _mm_mfence();
        uint64_t t1 = rdtscp_cycles();

        if (i >= kWarmup) {
            lats.push_back(t1 - t0);
        }
    }

    std::sort(lats.begin(), lats.end());
    const auto p50 = lats[lats.size() / 2];
    const auto p95 = lats[static_cast<size_t>(0.95 * (lats.size() - 1))];
    const auto minv = lats.front();
    const auto maxv = lats.back();

    const double mhz = read_cpu_mhz();
    const double cyc_to_ns = 1000.0 / mhz;

    std::printf("cpu_mhz=%.3f\n", mhz);
    std::printf("samples=%zu\n", lats.size());
    std::printf("min_cycles=%lu\n", minv);
    std::printf("p50_cycles=%lu\n", p50);
    std::printf("p95_cycles=%lu\n", p95);
    std::printf("max_cycles=%lu\n", maxv);
    std::printf("min_ns=%.3f\n", minv * cyc_to_ns);
    std::printf("p50_ns=%.3f\n", p50 * cyc_to_ns);
    std::printf("p95_ns=%.3f\n", p95 * cyc_to_ns);
    std::printf("max_ns=%.3f\n", maxv * cyc_to_ns);

    free(raw);
    return 0;
}
