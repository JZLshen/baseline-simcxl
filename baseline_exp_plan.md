# Baseline Experiment Plan

This file records the agreed baseline-only experiment set under
`/home/wq/sh/backup/baseline-simcxl-export`.

## Top-Level Driver

Use the top-level driver below to submit the agreed baseline min-set:

- [tools/run_baseline_paper_minset.sh](/home/wq/sh/backup/baseline-simcxl-export/tools/run_baseline_paper_minset.sh)

Examples:

```bash
cd /home/wq/sh/backup/baseline-simcxl-export

# show the full expanded command list only
bash tools/run_baseline_paper_minset.sh --dry-run

# run the whole agreed min-set
bash tools/run_baseline_paper_minset.sh \
  --root-outdir output/paper_baseline_minset_run \
  --parallel-jobs 1

# run only a subset of groups
bash tools/run_baseline_paper_minset.sh \
  --groups "overall coherence" \
  --root-outdir output/paper_baseline_overall_coherence
```

The top-level driver performs:

- one top-level `scons` build
- one image setup per actually needed guest binary
- sub-runner invocation with `--skip-build --skip-image-setup`

## Fixed Defaults

- `count-per-client = 30`
- `slot-count = 1024`
- client counts never include `3`
- sparse experiments keep only `slow32`, not `slow16`
- sparse request counts per slow client:
  - `8` for `25%`
  - `15` for `50%`
  - `30` for `100%`
- sparse `0%` points are reused from the corresponding non-slow baseline runs
- sparse pacing default:
  - `send-mode = greedy`
  - `slow-send-gap-ns = 20000`
- dedicated default transfer mode:
  - `request-transfer-mode = staging`
  - `response-transfer-mode = staging`

## Runner Mapping

- non-coherent dedicated/shared:
  - [tools/run_hydrarpc_multiclient_sweep.sh](/home/wq/sh/backup/baseline-simcxl-export/tools/run_hydrarpc_multiclient_sweep.sh)
- coherent dedicated:
  - [tools/run_hydrarpc_multiclient_coherent_sweep.sh](/home/wq/sh/backup/baseline-simcxl-export/tools/run_hydrarpc_multiclient_coherent_sweep.sh)
- legacy 16-client slowmix helper:
  - [tools/run_hydrarpc_multiclient_16client_slowmix_sweep.sh](/home/wq/sh/backup/baseline-simcxl-export/tools/run_hydrarpc_multiclient_16client_slowmix_sweep.sh)
  - not part of the current paper run list

## Minimal New-Run Set

Total new runs under the current agreed scope: `116`

### 1. Overall Bare RPC

New runs: `36`

Fixed parameters:
- `kinds = "dedicated shared"`
- `client-counts = "1 2 4 8 16 32"`
- `count-per-client = 30`
- `slot-count = 1024`

Workloads:
- `req=64, resp=64`
- `req=1530, resp=315`
- `req=38, resp=230`

Command templates:

```bash
bash tools/run_hydrarpc_multiclient_sweep.sh \
  --root-outdir output/paper_overall_req64_resp64 \
  --kinds "dedicated shared" \
  --client-counts "1 2 4 8 16 32" \
  --count-per-client 30 \
  --slot-count 1024 \
  --req-bytes 64 \
  --resp-bytes 64
```

```bash
bash tools/run_hydrarpc_multiclient_sweep.sh \
  --root-outdir output/paper_overall_req1530_resp315 \
  --kinds "dedicated shared" \
  --client-counts "1 2 4 8 16 32" \
  --count-per-client 30 \
  --slot-count 1024 \
  --req-bytes 1530 \
  --resp-bytes 315
```

```bash
bash tools/run_hydrarpc_multiclient_sweep.sh \
  --root-outdir output/paper_overall_req38_resp230 \
  --kinds "dedicated shared" \
  --client-counts "1 2 4 8 16 32" \
  --count-per-client 30 \
  --slot-count 1024 \
  --req-bytes 38 \
  --resp-bytes 230
```

### 2. Coherence Compare

New runs: `12`

Fixed parameters:
- `client-counts = "1 2 4 8 16 32"`
- `count-per-client = 30`
- `slot-count = 1024`
- `req=64`
- `resp=64`

Series that must be newly run:
- non-coherent dedicated `direct/direct`
- coherent dedicated `direct/direct`

The non-coherent dedicated `staging/staging` series is reused from
`overall req64/resp64 dedicated`.

Command templates:

```bash
bash tools/run_hydrarpc_multiclient_sweep.sh \
  --root-outdir output/paper_coherence_noncc_direct \
  --kinds "dedicated" \
  --client-counts "1 2 4 8 16 32" \
  --count-per-client 30 \
  --slot-count 1024 \
  --req-bytes 64 \
  --resp-bytes 64 \
  --request-transfer-mode direct \
  --response-transfer-mode direct
```

```bash
bash tools/run_hydrarpc_multiclient_coherent_sweep.sh \
  --root-outdir output/paper_coherence_cc_direct \
  --client-counts "1 2 4 8 16 32" \
  --count-per-client 30 \
  --slot-count 1024 \
  --req-bytes 64 \
  --resp-bytes 64 \
  --request-transfer-mode direct \
  --response-transfer-mode direct
```

### 3. Request Size Sensitivity

New runs: `10`

Fixed parameters:
- `kinds = "dedicated shared"`
- `client-counts = "32"`
- `count-per-client = 30`
- `slot-count = 1024`
- `resp=64`

New request sizes:
- `8`
- `256`
- `1024`
- `4096`
- `8192`

The `req=64, resp=64, c32` point is reused from `overall`.

### 4. Response Size Sensitivity

New runs: `10`

Fixed parameters:
- `kinds = "dedicated shared"`
- `client-counts = "32"`
- `count-per-client = 30`
- `slot-count = 1024`
- `req=64`

New response sizes:
- `8`
- `256`
- `1024`
- `4096`
- `8192`

The `req=64, resp=64, c32` point is reused from `overall`.

### 5. Ring Size Sensitivity

New runs: `12`

Fixed parameters:
- `kinds = "dedicated shared"`
- `client-counts = "32"`
- `count-per-client = 30`
- `req=64`
- `resp=64`

New ring sizes:
- `16`
- `32`
- `64`
- `128`
- `256`
- `512`

The `slot-count=1024` point is reused from `overall`.

### 6. Sparse32

New runs: `36`

Fixed parameters:
- `kinds = "dedicated shared"`
- `client-counts = "32"`
- `count-per-client = 30`
- `slot-count = 1024`
- `req=64`
- `resp=64`
- `send-mode = greedy`
- `slow-send-gap-ns = 20000`

Slow client counts:
- `4`
- `8`
- `16`
- `20`
- `24`
- `28`

Slow per-client request counts:
- `8`
- `15`
- `30`

Equivalent meanings:
- `8` -> `25%`
- `15` -> `50%`
- `30` -> `100%`

Command template pattern:

```bash
bash tools/run_hydrarpc_multiclient_sweep.sh \
  --root-outdir output/paper_sparse32_pct25 \
  --kinds "dedicated shared" \
  --client-counts "32" \
  --count-per-client 30 \
  --slot-count 1024 \
  --req-bytes 64 \
  --resp-bytes 64 \
  --slow-client-count 4 \
  --slow-count-per-client 8 \
  --slow-send-gap-ns 20000
```

Repeat the same pattern for:
- `slow-client-count = 8, 16, 20, 24, 28`
- `slow-count-per-client = 8, 15, 30`

## Logical Reuse Notes

- `shared req64 resp64 client=1/2/4/8/16/32` also serves:
  - shared contention motivation
  - `w/o doorbell`
- `dedicated staging/staging req64 resp64 client=1/2/4/8/16/32` also serves:
  - non-coherent staging baseline in coherence comparison
- `dedicated c32 req64 resp{8,256,1024,4096,8192}` also serves:
  - response-size motivation

## Not Run In This Baseline Repo

The following stay in the main project under `/home/wq/sh/SimCXL`:

- server delayed-head sensitivity
- injected CXL latency sensitivity
- DMA lane sensitivity
- `w/o DMA`
- `w/o prefetch`
- application / KV experiments
- the `current` system line in overall comparisons
