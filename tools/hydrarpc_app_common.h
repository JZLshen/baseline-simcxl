#ifndef HYDRARPC_APP_COMMON_H
#define HYDRARPC_APP_COMMON_H

#include <stddef.h>
#include <stdint.h>
#include <string.h>

#define HYDRARPC_APP_PROFILE_YCSB_A_1K "ycsb_a_1k"
#define HYDRARPC_APP_PROFILE_YCSB_B_1K "ycsb_b_1k"
#define HYDRARPC_APP_PROFILE_YCSB_C_1K "ycsb_c_1k"
#define HYDRARPC_APP_PROFILE_YCSB_F_1K "ycsb_f_1k"
#define HYDRARPC_APP_PROFILE_UDB_RO "udb_ro"

#define HYDRARPC_APP_DEFAULT_RECORD_COUNT 100000ULL
#define HYDRARPC_APP_DEFAULT_DATASET_SEED 0x9B5D3A4781C26EF1ULL
#define HYDRARPC_APP_DEFAULT_WORKLOAD_SEED 0xC7D51A32049EF68BULL
#define HYDRARPC_APP_DEFAULT_ZIPF_THETA 0.99
#define HYDRARPC_APP_DEFAULT_YCSB_KEY_SIZE 16U
#define HYDRARPC_APP_DEFAULT_YCSB_VALUE_SIZE 1024U
#define HYDRARPC_APP_DEFAULT_UDB_KEY_SIZE 27U
#define HYDRARPC_APP_DEFAULT_UDB_VALUE_SIZE 127U
#define HYDRARPC_APP_DEFAULT_UDB_MAX_KEY_SIZE 39U
#define HYDRARPC_APP_DEFAULT_UDB_MAX_VALUE_SIZE 179U
#define HYDRARPC_APP_MAX_KEY_SIZE 255U

#define HYDRARPC_APP_OP_GET 1u
#define HYDRARPC_APP_OP_PUT 2u
#define HYDRARPC_APP_OP_RMW 3u

#define HYDRARPC_APP_STATUS_OK 0u
#define HYDRARPC_APP_STATUS_MISS 1u
#define HYDRARPC_APP_STATUS_INVALID 2u

typedef enum {
    HYDRARPC_APP_KEY_DIST_UNIFORM = 0,
    HYDRARPC_APP_KEY_DIST_ZIPF = 1,
} hydrarpc_app_key_dist_t;

typedef struct {
    const char *name;
    size_t key_size;
    size_t value_size;
    double read_ratio;
    double update_ratio;
    double rmw_ratio;
    hydrarpc_app_key_dist_t key_dist;
    double zipf_theta;
} hydrarpc_app_profile_t;

typedef struct __attribute__((packed)) {
    uint8_t op;
    uint8_t key_len;
    uint16_t reserved0;
    uint32_t value_len;
    uint64_t key_hash;
} hydrarpc_app_request_hdr_t;

typedef struct __attribute__((packed)) {
    uint8_t status;
    uint8_t op;
    uint16_t reserved0;
    uint32_t value_len;
    uint64_t value_checksum;
} hydrarpc_app_response_hdr_t;

static inline uint64_t
hydrarpc_app_rotl64(uint64_t v, unsigned int shift)
{
    return (v << shift) | (v >> (64u - shift));
}

static inline uint64_t
hydrarpc_app_mix64(uint64_t v)
{
    v ^= v >> 33;
    v *= 0xff51afd7ed558ccdULL;
    v ^= v >> 33;
    v *= 0xc4ceb9fe1a85ec53ULL;
    v ^= v >> 33;
    return v;
}

static inline uint64_t
hydrarpc_app_hash_bytes(const void *data, size_t len)
{
    const uint8_t *bytes = (const uint8_t *)data;
    uint64_t acc = 0xCBF29CE484222325ULL ^ (uint64_t)len;
    size_t index = 0;

    while (index + sizeof(uint64_t) <= len) {
        uint64_t word = 0;
        memcpy(&word, bytes + index, sizeof(word));
        acc ^= hydrarpc_app_mix64(word + index + 1u);
        acc = hydrarpc_app_rotl64(acc, 7u) * 0x100000001B3ULL;
        index += sizeof(uint64_t);
    }

    if (index < len) {
        uint64_t tail = 0;
        memcpy(&tail, bytes + index, len - index);
        acc ^= hydrarpc_app_mix64(tail + len);
        acc = hydrarpc_app_rotl64(acc, 11u) * 0x100000001B3ULL;
    }

    return hydrarpc_app_mix64(acc ^ len);
}

static inline uint64_t
hydrarpc_app_checksum_bytes(const void *data, size_t len)
{
    return hydrarpc_app_hash_bytes(data, len);
}

static inline void
hydrarpc_app_fill_key(uint8_t *dst, size_t len, uint64_t dataset_seed,
                      uint64_t key_id)
{
    size_t offset = 0;

    if (!dst || len == 0)
        return;

    while (offset < len) {
        uint64_t word = hydrarpc_app_mix64(dataset_seed ^
                                           (key_id * 0x9E3779B185EBCA87ULL) ^
                                           (uint64_t)(offset + 1));
        size_t chunk = len - offset;
        if (chunk > sizeof(word))
            chunk = sizeof(word);
        memcpy(dst + offset, &word, chunk);
        offset += chunk;
    }

    if (len >= sizeof(key_id))
        memcpy(dst, &key_id, sizeof(key_id));
}

static inline uint64_t
hydrarpc_app_value_seed(uint64_t dataset_seed, uint64_t key_hash,
                        uint64_t update_version)
{
    return hydrarpc_app_mix64(dataset_seed ^
                              (key_hash * 0xD6E8FEB86659FD93ULL) ^
                              (update_version * 0xA0761D6478BD642FULL));
}

static inline void
hydrarpc_app_fill_value(uint8_t *dst, size_t len, uint64_t value_seed)
{
    size_t offset = 0;

    if (!dst || len == 0)
        return;

    while (offset < len) {
        uint64_t word = hydrarpc_app_mix64(value_seed ^
                                           (uint64_t)(offset + 1) *
                                               0x94D049BB133111EBULL);
        size_t chunk = len - offset;
        if (chunk > sizeof(word))
            chunk = sizeof(word);
        memcpy(dst + offset, &word, chunk);
        offset += chunk;
    }
}

static inline size_t
hydrarpc_app_request_wire_size(uint8_t op, size_t key_len, size_t value_len)
{
    return sizeof(hydrarpc_app_request_hdr_t) + key_len +
           ((op == HYDRARPC_APP_OP_PUT || op == HYDRARPC_APP_OP_RMW) ?
                value_len :
                0u);
}

static inline size_t
hydrarpc_app_response_wire_size(uint8_t status, uint8_t op, size_t value_len)
{
    return sizeof(hydrarpc_app_response_hdr_t) +
           ((status == HYDRARPC_APP_STATUS_OK &&
             (op == HYDRARPC_APP_OP_GET ||
              op == HYDRARPC_APP_OP_RMW)) ? value_len : 0u);
}

static inline int
hydrarpc_app_profile_has_variable_layout(const char *name)
{
    return name && strcmp(name, HYDRARPC_APP_PROFILE_UDB_RO) == 0;
}

static inline size_t
hydrarpc_app_profile_max_key_size(const char *name, size_t default_key_size)
{
    return hydrarpc_app_profile_has_variable_layout(name) ?
               HYDRARPC_APP_DEFAULT_UDB_MAX_KEY_SIZE :
               default_key_size;
}

static inline size_t
hydrarpc_app_profile_max_value_size(const char *name,
                                    size_t default_value_size)
{
    return hydrarpc_app_profile_has_variable_layout(name) ?
               HYDRARPC_APP_DEFAULT_UDB_MAX_VALUE_SIZE :
               default_value_size;
}

static inline size_t
hydrarpc_app_select_discrete_length(uint64_t selector_seed, uint64_t key_id,
                                    const uint16_t *choices,
                                    size_t choice_count)
{
    uint64_t mixed;

    if (!choices || choice_count == 0)
        return 0;

    mixed = hydrarpc_app_mix64(selector_seed ^
                               (key_id * 0x9E3779B185EBCA87ULL));
    return (size_t)choices[mixed % choice_count];
}

static inline void
hydrarpc_app_record_layout(const char *profile_name, uint64_t dataset_seed,
                           uint64_t key_id, size_t default_key_size,
                           size_t default_value_size, size_t *out_key_len,
                           size_t *out_value_len)
{
    if (hydrarpc_app_profile_has_variable_layout(profile_name)) {
        static const uint16_t udb_key_lengths[] = {
            16u, 20u, 24u, 24u, 28u, 28u, 28u, 32u, 32u, 39u,
        };
        static const uint16_t udb_value_lengths[] = {
            80u, 96u, 112u, 112u, 128u, 128u, 128u, 144u, 160u, 179u,
        };

        if (out_key_len) {
            *out_key_len = hydrarpc_app_select_discrete_length(
                dataset_seed ^ 0xB8FE6C391B5C4F2DULL,
                key_id,
                udb_key_lengths,
                sizeof(udb_key_lengths) / sizeof(udb_key_lengths[0]));
        }
        if (out_value_len) {
            *out_value_len = hydrarpc_app_select_discrete_length(
                dataset_seed ^ 0x4F1BBCDCBFA54001ULL,
                key_id,
                udb_value_lengths,
                sizeof(udb_value_lengths) / sizeof(udb_value_lengths[0]));
        }
        return;
    }

    if (out_key_len)
        *out_key_len = default_key_size;
    if (out_value_len)
        *out_value_len = default_value_size;
}

static inline int
hydrarpc_app_lookup_profile(const char *name, hydrarpc_app_profile_t *out)
{
    hydrarpc_app_profile_t profile;

    if (!name || !out)
        return -1;

    if (strcmp(name, HYDRARPC_APP_PROFILE_YCSB_A_1K) == 0) {
        profile.name = HYDRARPC_APP_PROFILE_YCSB_A_1K;
        profile.key_size = HYDRARPC_APP_DEFAULT_YCSB_KEY_SIZE;
        profile.value_size = HYDRARPC_APP_DEFAULT_YCSB_VALUE_SIZE;
        profile.read_ratio = 0.5;
        profile.update_ratio = 0.5;
        profile.rmw_ratio = 0.0;
        profile.key_dist = HYDRARPC_APP_KEY_DIST_ZIPF;
        profile.zipf_theta = HYDRARPC_APP_DEFAULT_ZIPF_THETA;
    } else if (strcmp(name, HYDRARPC_APP_PROFILE_YCSB_B_1K) == 0) {
        profile.name = HYDRARPC_APP_PROFILE_YCSB_B_1K;
        profile.key_size = HYDRARPC_APP_DEFAULT_YCSB_KEY_SIZE;
        profile.value_size = HYDRARPC_APP_DEFAULT_YCSB_VALUE_SIZE;
        profile.read_ratio = 0.95;
        profile.update_ratio = 0.05;
        profile.rmw_ratio = 0.0;
        profile.key_dist = HYDRARPC_APP_KEY_DIST_ZIPF;
        profile.zipf_theta = HYDRARPC_APP_DEFAULT_ZIPF_THETA;
    } else if (strcmp(name, HYDRARPC_APP_PROFILE_YCSB_C_1K) == 0) {
        profile.name = HYDRARPC_APP_PROFILE_YCSB_C_1K;
        profile.key_size = HYDRARPC_APP_DEFAULT_YCSB_KEY_SIZE;
        profile.value_size = HYDRARPC_APP_DEFAULT_YCSB_VALUE_SIZE;
        profile.read_ratio = 1.0;
        profile.update_ratio = 0.0;
        profile.rmw_ratio = 0.0;
        profile.key_dist = HYDRARPC_APP_KEY_DIST_ZIPF;
        profile.zipf_theta = HYDRARPC_APP_DEFAULT_ZIPF_THETA;
    } else if (strcmp(name, HYDRARPC_APP_PROFILE_YCSB_F_1K) == 0) {
        profile.name = HYDRARPC_APP_PROFILE_YCSB_F_1K;
        profile.key_size = HYDRARPC_APP_DEFAULT_YCSB_KEY_SIZE;
        profile.value_size = HYDRARPC_APP_DEFAULT_YCSB_VALUE_SIZE;
        profile.read_ratio = 0.0;
        profile.update_ratio = 0.0;
        profile.rmw_ratio = 1.0;
        profile.key_dist = HYDRARPC_APP_KEY_DIST_ZIPF;
        profile.zipf_theta = HYDRARPC_APP_DEFAULT_ZIPF_THETA;
    } else if (strcmp(name, HYDRARPC_APP_PROFILE_UDB_RO) == 0) {
        profile.name = HYDRARPC_APP_PROFILE_UDB_RO;
        profile.key_size = HYDRARPC_APP_DEFAULT_UDB_KEY_SIZE;
        profile.value_size = HYDRARPC_APP_DEFAULT_UDB_VALUE_SIZE;
        profile.read_ratio = 1.0;
        profile.update_ratio = 0.0;
        profile.rmw_ratio = 0.0;
        profile.key_dist = HYDRARPC_APP_KEY_DIST_UNIFORM;
        profile.zipf_theta = 0.0;
    } else {
        return -1;
    }

    *out = profile;
    return 0;
}

#endif
