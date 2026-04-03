#ifndef HYDRARPC_APP_RUNTIME_H
#define HYDRARPC_APP_RUNTIME_H

#include <math.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "hydrarpc_app_common.h"

typedef struct {
    uint8_t op;
    uint16_t key_len;
    uint32_t value_len;
    uint64_t key_id;
    uint64_t key_hash;
    uint64_t value_seed;
    uint64_t request_len;
    uint64_t response_len;
} hydrarpc_app_operation_t;

typedef struct {
    const char *profile_name;
    size_t record_count;
    size_t key_size;
    size_t value_size;
    size_t max_key_size;
    size_t max_value_size;
    uint64_t dataset_seed;
    uint8_t *keys;
    uint8_t *values;
    uint16_t *key_lengths;
    uint32_t *value_lengths;
    uint64_t *hashes;
    uint64_t *checksums;
} hydrarpc_app_store_t;

#define HYDRARPC_APPRT_FIXED_UNIFORM_PLAN_LEN 30u

static inline uint64_t
hydrarpc_apprt_next_random_u64(uint64_t *state)
{
    uint64_t x = (state && *state != 0) ? *state : 0xA0761D6478BD642FULL;

    x ^= x >> 12;
    x ^= x << 25;
    x ^= x >> 27;
    x *= 2685821657736338717ULL;
    if (state)
        *state = x;
    return x;
}

static inline double
hydrarpc_apprt_next_random_double(uint64_t *state)
{
    uint64_t raw = hydrarpc_apprt_next_random_u64(state) >> 11;

    return (double)raw * (1.0 / 9007199254740992.0);
}

static int
hydrarpc_apprt_build_zipf_cdf(double theta, uint64_t record_count,
                              double **out_cdf)
{
    double normalizer = 0.0;
    double running = 0.0;
    double *cdf = NULL;
    uint64_t i = 0;

    if (!out_cdf || record_count == 0 || theta <= 0.0)
        return -1;

    cdf = (double *)malloc((size_t)record_count * sizeof(*cdf));
    if (!cdf)
        return -1;

    for (i = 1; i <= record_count; i++)
        normalizer += 1.0 / pow((double)i, theta);
    if (normalizer <= 0.0) {
        free(cdf);
        return -1;
    }

    for (i = 1; i <= record_count; i++) {
        running += (1.0 / pow((double)i, theta)) / normalizer;
        cdf[i - 1] = running;
    }

    cdf[record_count - 1] = 1.0;
    *out_cdf = cdf;
    return 0;
}

static uint64_t
hydrarpc_apprt_sample_zipf_key_id(const double *cdf, uint64_t record_count,
                                  uint64_t *rng_state)
{
    double needle = hydrarpc_apprt_next_random_double(rng_state);
    uint64_t lo = 0;
    uint64_t hi = record_count - 1;

    while (lo < hi) {
        uint64_t mid = lo + ((hi - lo) / 2u);

        if (needle <= cdf[mid])
            hi = mid;
        else
            lo = mid + 1u;
    }

    return lo;
}

static int
hydrarpc_apprt_sample_uniform_key_id(uint64_t record_count, uint64_t *rng_state,
                                     uint64_t *out_key_id)
{
    if (!rng_state || !out_key_id || record_count == 0)
        return -1;

    *out_key_id = hydrarpc_apprt_next_random_u64(rng_state) % record_count;
    return 0;
}

static int
hydrarpc_apprt_uses_fixed_uniform_plan(const char *profile_name,
                                       hydrarpc_app_key_dist_t key_dist)
{
    return key_dist == HYDRARPC_APP_KEY_DIST_UNIFORM &&
           hydrarpc_app_profile_has_variable_layout(profile_name);
}

static uint64_t
hydrarpc_apprt_fixed_uniform_plan_slot(uint64_t client_id, uint64_t req_index)
{
    return (req_index + (client_id % HYDRARPC_APPRT_FIXED_UNIFORM_PLAN_LEN)) %
           HYDRARPC_APPRT_FIXED_UNIFORM_PLAN_LEN;
}

static void
hydrarpc_apprt_fixed_uniform_plan_bins(uint64_t plan_slot,
                                       size_t *out_key_bin,
                                       size_t *out_value_bin)
{
    size_t key_bin = (size_t)(plan_slot % HYDRARPC_APP_UDB_BIN_COUNT);
    size_t block = (size_t)((plan_slot / HYDRARPC_APP_UDB_BIN_COUNT) % 3u);
    size_t value_bin = (key_bin + (block * 3u)) % HYDRARPC_APP_UDB_BIN_COUNT;

    if (out_key_bin)
        *out_key_bin = key_bin;
    if (out_value_bin)
        *out_value_bin = value_bin;
}

static int
hydrarpc_apprt_fixed_uniform_key_used(const hydrarpc_app_operation_t *ops,
                                      uint64_t used_count,
                                      uint64_t key_id)
{
    uint64_t idx = 0;

    if (!ops)
        return 0;

    for (idx = 0; idx < used_count; idx++) {
        if (ops[idx].key_id == key_id)
            return 1;
    }

    return 0;
}

static int
hydrarpc_apprt_find_fixed_uniform_key_id(const hydrarpc_app_operation_t *ops,
                                         uint64_t used_count,
                                         uint64_t record_count,
                                         uint64_t client_id,
                                         uint64_t plan_slot,
                                         uint64_t dataset_seed,
                                         uint64_t *out_key_id)
{
    uint64_t start_key_id = 0;
    uint64_t step = 0;
    size_t target_key_bin = 0;
    size_t target_value_bin = 0;

    if (!ops || !out_key_id || record_count == 0)
        return -1;

    hydrarpc_apprt_fixed_uniform_plan_bins(plan_slot,
                                           &target_key_bin,
                                           &target_value_bin);
    start_key_id = hydrarpc_app_mix64(
        dataset_seed ^
        ((client_id + 1u) * 0x9E3779B185EBCA87ULL) ^
        ((plan_slot + 1u) * 0xD6E8FEB86659FD93ULL)) % record_count;

    for (step = 0; step < record_count; step++) {
        uint64_t key_id = (start_key_id + step) % record_count;

        if (hydrarpc_apprt_fixed_uniform_key_used(ops, used_count, key_id))
            continue;
        if (hydrarpc_app_udb_key_bin_index(dataset_seed, key_id) !=
            target_key_bin) {
            continue;
        }
        if (hydrarpc_app_udb_value_bin_index(dataset_seed, key_id) !=
            target_value_bin) {
            continue;
        }

        *out_key_id = key_id;
        return 0;
    }

    return -1;
}

static int
hydrarpc_apprt_build_operations(hydrarpc_app_operation_t *ops,
                                uint64_t num_requests,
                                const char *profile_name,
                                uint64_t record_count,
                                uint64_t client_id,
                                size_t key_size,
                                size_t value_size,
                                size_t max_key_size,
                                double read_ratio,
                                double update_ratio,
                                double rmw_ratio,
                                hydrarpc_app_key_dist_t key_dist,
                                double zipf_theta,
                                uint64_t dataset_seed,
                                uint64_t workload_seed)
{
    double *cdf = NULL;
    uint8_t *key_buf = NULL;
    uint64_t rng_state = workload_seed;
    uint64_t req_index = 0;

    if (!ops || !profile_name || num_requests == 0 || record_count == 0 ||
        key_size == 0 || value_size == 0 || max_key_size == 0) {
        return -1;
    }
    if (read_ratio < 0.0 || update_ratio < 0.0 || rmw_ratio < 0.0 ||
        fabs((read_ratio + update_ratio + rmw_ratio) - 1.0) > 1e-6) {
        return -1;
    }

    key_buf = (uint8_t *)malloc(max_key_size);
    if (!key_buf)
        return -1;

    if (key_dist == HYDRARPC_APP_KEY_DIST_ZIPF &&
        hydrarpc_apprt_build_zipf_cdf(zipf_theta, record_count, &cdf) != 0) {
        free(key_buf);
        return -1;
    }

    for (req_index = 0; req_index < num_requests; req_index++) {
        size_t record_key_len = 0;
        size_t record_value_len = 0;
        uint64_t key_id = 0;
        uint64_t key_hash = 0;
        double selector = 0.0;

        if (hydrarpc_apprt_uses_fixed_uniform_plan(profile_name, key_dist)) {
            uint64_t plan_slot = hydrarpc_apprt_fixed_uniform_plan_slot(client_id,
                                                                        req_index);

            if (hydrarpc_apprt_find_fixed_uniform_key_id(ops,
                                                         req_index,
                                                         record_count,
                                                         client_id,
                                                         plan_slot,
                                                         dataset_seed,
                                                         &key_id) != 0) {
                free(cdf);
                free(key_buf);
                return -1;
            }
        } else if (key_dist == HYDRARPC_APP_KEY_DIST_ZIPF) {
            key_id = hydrarpc_apprt_sample_zipf_key_id(cdf,
                                                       record_count,
                                                       &rng_state);
        } else if (hydrarpc_apprt_sample_uniform_key_id(record_count,
                                                        &rng_state,
                                                        &key_id) != 0) {
            free(cdf);
            free(key_buf);
            return -1;
        }
        hydrarpc_app_record_layout(profile_name,
                                   dataset_seed,
                                   key_id,
                                   key_size,
                                   value_size,
                                   &record_key_len,
                                   &record_value_len);
        if (record_key_len == 0 || record_key_len > max_key_size ||
            record_key_len > HYDRARPC_APP_MAX_KEY_SIZE ||
            record_value_len == 0) {
            free(cdf);
            free(key_buf);
            return -1;
        }

        hydrarpc_app_fill_key(key_buf, record_key_len, dataset_seed, key_id);
        key_hash = hydrarpc_app_hash_bytes(key_buf, record_key_len);
        selector = hydrarpc_apprt_next_random_double(&rng_state);

        if (selector < read_ratio) {
            ops[req_index].op = HYDRARPC_APP_OP_GET;
        } else if (selector < (read_ratio + update_ratio)) {
            ops[req_index].op = HYDRARPC_APP_OP_PUT;
        } else {
            ops[req_index].op = HYDRARPC_APP_OP_RMW;
        }
        ops[req_index].key_len = (uint16_t)record_key_len;
        ops[req_index].value_len = (uint32_t)record_value_len;
        ops[req_index].key_id = key_id;
        ops[req_index].key_hash = key_hash;
        ops[req_index].value_seed =
            hydrarpc_app_value_seed(workload_seed, key_hash, req_index + 1u);
        ops[req_index].request_len =
            hydrarpc_app_request_wire_size(ops[req_index].op,
                                           ops[req_index].key_len,
                                           ops[req_index].value_len);
        ops[req_index].response_len =
            hydrarpc_app_response_wire_size(HYDRARPC_APP_STATUS_OK,
                                            ops[req_index].op,
                                            ops[req_index].value_len);
    }

    free(cdf);
    free(key_buf);
    return 0;
}

static int
hydrarpc_apprt_store_key_view(const hydrarpc_app_store_t *store,
                              uint64_t key_id,
                              const uint8_t **key_out,
                              size_t *key_len_out)
{
    if (!store || !key_out || !key_len_out || !store->keys ||
        !store->key_lengths || key_id >= store->record_count) {
        return -1;
    }

    *key_out = store->keys + ((size_t)key_id * store->max_key_size);
    *key_len_out = store->key_lengths[key_id];
    return 0;
}

static size_t
hydrarpc_apprt_encode_request(uint8_t *dst, size_t dst_size,
                              const hydrarpc_app_operation_t *op,
                              uint64_t dataset_seed,
                              const hydrarpc_app_store_t *store)
{
    hydrarpc_app_request_hdr_t *hdr = NULL;
    uint8_t *key_ptr = NULL;
    const uint8_t *preloaded_key = NULL;
    size_t preloaded_key_len = 0;
    size_t req_len = 0;

    if (!dst || !op)
        return 0;

    req_len = hydrarpc_app_request_wire_size(
        op->op,
        op->key_len,
        ((op->op == HYDRARPC_APP_OP_PUT || op->op == HYDRARPC_APP_OP_RMW) ?
             op->value_len :
             0u));
    if (req_len > dst_size)
        return 0;

    memset(dst, 0, req_len);
    hdr = (hydrarpc_app_request_hdr_t *)dst;
    hdr->op = op->op;
    hdr->key_len = (uint8_t)op->key_len;
    hdr->reserved0 = 0;
    hdr->value_len =
        (uint32_t)(((op->op == HYDRARPC_APP_OP_PUT ||
                     op->op == HYDRARPC_APP_OP_RMW) ? op->value_len : 0u));
    hdr->key_hash = op->key_hash;

    key_ptr = (uint8_t *)(hdr + 1);
    if (hydrarpc_apprt_store_key_view(store,
                                      op->key_id,
                                      &preloaded_key,
                                      &preloaded_key_len) == 0 &&
        preloaded_key_len == op->key_len) {
        memcpy(key_ptr, preloaded_key, op->key_len);
    } else {
        hydrarpc_app_fill_key(key_ptr, op->key_len, dataset_seed, op->key_id);
    }
    if (op->op == HYDRARPC_APP_OP_PUT || op->op == HYDRARPC_APP_OP_RMW) {
        hydrarpc_app_fill_value(key_ptr + op->key_len,
                                op->value_len,
                                op->value_seed);
    }

    return req_len;
}

static int
hydrarpc_apprt_validate_response(const uint8_t *response_view,
                                 size_t response_len,
                                 const hydrarpc_app_operation_t *expected_op)
{
    const hydrarpc_app_response_hdr_t *resp_hdr = NULL;
    const uint8_t *value_ptr = NULL;
    size_t expected_response_len = 0;

    if (!response_view || !expected_op || response_len < sizeof(*resp_hdr))
        return -1;

    resp_hdr = (const hydrarpc_app_response_hdr_t *)response_view;
    expected_response_len = hydrarpc_app_response_wire_size(resp_hdr->status,
                                                            resp_hdr->op,
                                                            resp_hdr->value_len);
    if (resp_hdr->op != expected_op->op || expected_response_len != response_len)
        return -1;
    if (resp_hdr->status != HYDRARPC_APP_STATUS_OK)
        return -1;

    if (resp_hdr->op == HYDRARPC_APP_OP_GET ||
        resp_hdr->op == HYDRARPC_APP_OP_RMW) {
        if (resp_hdr->value_len != expected_op->value_len)
            return -1;
        value_ptr = (const uint8_t *)(resp_hdr + 1);
        if (hydrarpc_app_checksum_bytes(value_ptr, resp_hdr->value_len) !=
            resp_hdr->value_checksum) {
            return -1;
        }
    } else if (resp_hdr->value_len != 0 || resp_hdr->value_checksum != 0) {
        return -1;
    }

    return 0;
}

static int
hydrarpc_apprt_store_init(hydrarpc_app_store_t *store,
                          const char *profile_name,
                          size_t record_count,
                          size_t key_size,
                          size_t value_size,
                          size_t max_key_size,
                          size_t max_value_size,
                          uint64_t dataset_seed)
{
    if (!store || !profile_name || record_count == 0 || key_size == 0 ||
        value_size == 0 || max_key_size == 0 || max_value_size == 0) {
        return -1;
    }

    memset(store, 0, sizeof(*store));
    store->profile_name = profile_name;
    store->record_count = record_count;
    store->key_size = key_size;
    store->value_size = value_size;
    store->max_key_size = max_key_size;
    store->max_value_size = max_value_size;
    store->dataset_seed = dataset_seed;
    store->keys = (uint8_t *)malloc(record_count * max_key_size);
    store->values = (uint8_t *)malloc(record_count * max_value_size);
    store->key_lengths =
        (uint16_t *)malloc(record_count * sizeof(*store->key_lengths));
    store->value_lengths =
        (uint32_t *)malloc(record_count * sizeof(*store->value_lengths));
    store->hashes = (uint64_t *)malloc(record_count * sizeof(*store->hashes));
    store->checksums =
        (uint64_t *)malloc(record_count * sizeof(*store->checksums));
    if (!store->keys || !store->values || !store->key_lengths ||
        !store->value_lengths || !store->hashes || !store->checksums) {
        return -1;
    }

    return 0;
}

static void
hydrarpc_apprt_store_free(hydrarpc_app_store_t *store)
{
    if (!store)
        return;

    free(store->keys);
    free(store->values);
    free(store->key_lengths);
    free(store->value_lengths);
    free(store->hashes);
    free(store->checksums);
    memset(store, 0, sizeof(*store));
}

static int
hydrarpc_apprt_store_preload(hydrarpc_app_store_t *store)
{
    uint64_t record_index = 0;

    if (!store || !store->keys || !store->values || !store->key_lengths ||
        !store->value_lengths || !store->hashes || !store->checksums) {
        return -1;
    }

    for (record_index = 0; record_index < store->record_count; record_index++) {
        size_t key_len = 0;
        size_t value_len = 0;
        uint8_t *key = store->keys + (record_index * store->max_key_size);
        uint8_t *value =
            store->values + (record_index * store->max_value_size);
        uint64_t hash = 0;
        uint64_t value_seed = 0;

        hydrarpc_app_record_layout(store->profile_name,
                                   store->dataset_seed,
                                   record_index,
                                   store->key_size,
                                   store->value_size,
                                   &key_len,
                                   &value_len);
        if (key_len == 0 || key_len > store->max_key_size ||
            value_len == 0 || value_len > store->max_value_size) {
            return -1;
        }

        store->key_lengths[record_index] = (uint16_t)key_len;
        store->value_lengths[record_index] = (uint32_t)value_len;
        hydrarpc_app_fill_key(key,
                              key_len,
                              store->dataset_seed,
                              record_index);
        hash = hydrarpc_app_hash_bytes(key, key_len);
        store->hashes[record_index] = hash;
        value_seed = hydrarpc_app_value_seed(store->dataset_seed, hash, 0u);
        hydrarpc_app_fill_value(value, value_len, value_seed);
        store->checksums[record_index] =
            hydrarpc_app_checksum_bytes(value, value_len);
    }

    return 0;
}

static int
hydrarpc_apprt_store_find(const hydrarpc_app_store_t *store,
                          const uint8_t *key,
                          size_t key_len,
                          uint64_t key_hash,
                          uint32_t *record_index_out)
{
    uint64_t key_id = 0;
    uint32_t record_index = 0;
    const uint8_t *stored_key = NULL;

    if (!store || !key || key_len < sizeof(uint64_t) ||
        key_len > store->max_key_size || !record_index_out) {
        return 0;
    }

    memcpy(&key_id, key, sizeof(key_id));
    if (key_id >= store->record_count)
        return 0;

    record_index = (uint32_t)key_id;
    if (key_len != store->key_lengths[record_index] ||
        key_hash != store->hashes[record_index]) {
        return 0;
    }

    stored_key = store->keys + ((size_t)record_index * store->max_key_size);
    if (memcmp(key, stored_key, key_len) != 0)
        return 0;

    *record_index_out = record_index;
    return 1;
}

static int
hydrarpc_apprt_store_update_value(hydrarpc_app_store_t *store,
                                  uint32_t record_index,
                                  const uint8_t *value,
                                  size_t value_len)
{
    uint8_t *dst = NULL;

    if (!store || !value || record_index >= store->record_count ||
        value_len != store->value_lengths[record_index]) {
        return -1;
    }

    dst = store->values + ((size_t)record_index * store->max_value_size);
    memcpy(dst, value, value_len);
    store->checksums[record_index] =
        hydrarpc_app_checksum_bytes(dst, value_len);
    return 0;
}

static int
hydrarpc_apprt_store_process_request(hydrarpc_app_store_t *store,
                                     const uint8_t *request_view,
                                     size_t request_len,
                                     uint8_t *response_buf,
                                     size_t response_buf_size,
                                     size_t *response_len_out)
{
    const hydrarpc_app_request_hdr_t *req_hdr = NULL;
    const uint8_t *key_ptr = NULL;
    const uint8_t *value_ptr = NULL;
    hydrarpc_app_response_hdr_t *resp_hdr = NULL;
    uint32_t record_index = 0;
    int found = 0;
    size_t response_len = 0;

    if (!store || !request_view || !response_buf || !response_len_out ||
        request_len < sizeof(*req_hdr) || response_buf_size < sizeof(*resp_hdr)) {
        return -1;
    }

    req_hdr = (const hydrarpc_app_request_hdr_t *)request_view;
    key_ptr = (const uint8_t *)(req_hdr + 1);
    value_ptr = key_ptr + req_hdr->key_len;
    if (req_hdr->key_len == 0 || req_hdr->key_len > store->max_key_size ||
        (req_hdr->op != HYDRARPC_APP_OP_GET &&
         req_hdr->op != HYDRARPC_APP_OP_PUT &&
         req_hdr->op != HYDRARPC_APP_OP_RMW) ||
        hydrarpc_app_request_wire_size(req_hdr->op,
                                       req_hdr->key_len,
                                       req_hdr->value_len) != request_len) {
        return -1;
    }
    if ((req_hdr->op == HYDRARPC_APP_OP_PUT ||
         req_hdr->op == HYDRARPC_APP_OP_RMW) &&
        (req_hdr->value_len == 0 || req_hdr->value_len > store->max_value_size)) {
        return -1;
    }

    found = hydrarpc_apprt_store_find(store,
                                      key_ptr,
                                      req_hdr->key_len,
                                      req_hdr->key_hash,
                                      &record_index);

    memset(response_buf, 0, response_buf_size);
    resp_hdr = (hydrarpc_app_response_hdr_t *)response_buf;
    resp_hdr->status = found ? HYDRARPC_APP_STATUS_OK : HYDRARPC_APP_STATUS_MISS;
    resp_hdr->op = req_hdr->op;
    resp_hdr->reserved0 = 0;
    resp_hdr->value_len = 0;
    resp_hdr->value_checksum = 0;

    if (found && req_hdr->op == HYDRARPC_APP_OP_GET) {
        const uint8_t *src =
            store->values + ((size_t)record_index * store->max_value_size);
        uint8_t *dst = (uint8_t *)(resp_hdr + 1);
        uint32_t actual_value_len = store->value_lengths[record_index];

        if (sizeof(*resp_hdr) + actual_value_len > response_buf_size)
            return -1;
        memcpy(dst, src, actual_value_len);
        resp_hdr->value_len = actual_value_len;
        resp_hdr->value_checksum = store->checksums[record_index];
    } else if (found && req_hdr->op == HYDRARPC_APP_OP_PUT) {
        if (req_hdr->value_len != store->value_lengths[record_index])
            return -1;
        if (hydrarpc_apprt_store_update_value(store,
                                              record_index,
                                              value_ptr,
                                              req_hdr->value_len) != 0) {
            return -1;
        }
    } else if (found && req_hdr->op == HYDRARPC_APP_OP_RMW) {
        const uint8_t *src =
            store->values + ((size_t)record_index * store->max_value_size);
        uint8_t *dst = (uint8_t *)(resp_hdr + 1);
        uint32_t actual_value_len = store->value_lengths[record_index];

        if (req_hdr->value_len != actual_value_len ||
            sizeof(*resp_hdr) + actual_value_len > response_buf_size) {
            return -1;
        }
        memcpy(dst, src, actual_value_len);
        resp_hdr->value_len = actual_value_len;
        resp_hdr->value_checksum = store->checksums[record_index];
        if (hydrarpc_apprt_store_update_value(store,
                                              record_index,
                                              value_ptr,
                                              req_hdr->value_len) != 0) {
            return -1;
        }
    }

    response_len = hydrarpc_app_response_wire_size(resp_hdr->status,
                                                   resp_hdr->op,
                                                   resp_hdr->value_len);
    if (response_len > response_buf_size)
        return -1;

    *response_len_out = response_len;
    return 0;
}

#endif
