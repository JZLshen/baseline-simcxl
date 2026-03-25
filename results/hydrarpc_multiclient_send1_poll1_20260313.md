# HydraRPC Multi-client send1-poll1 Results (2026-03-13)

Root output directory:
- `/home/wq/sh/backup/SimCXL/output/hydrarpc_multiclient_sweep_send1poll1_guestcompile_r20_20260313_032850`

Primary artifacts:
- `summary.csv`
- `run_failures.tsv`

Configuration:
- project: `/home/wq/sh/backup/SimCXL`
- timing source: `m5_rpns`
- requests per client: `20`
- client counts targeted: `1, 2, 4, 8, 16, 32`
- layout 1: `shared` request queue + response queue + req/resp data area
- layout 2: `dedicated` per-client request queue + response queue + req/resp data area
- send/poll policy: `send1-poll1`
- build/launch path: inject C source, compile inside guest during KVM stage, then switch to timing

## Valid measurements

### Shared layout

| clients | total_e2e_ns | avg_latency_ns | median_latency_ns | throughput_mrps | correctness_fail_count |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 164166 | 50661.850 | 46582.000 | 0.121828 | 0 |

### Dedicated layout

| clients | total_e2e_ns | avg_latency_ns | median_latency_ns | throughput_mrps | correctness_fail_count |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 196163 | 59756.200 | 54386.500 | 0.101956 | 0 |
| 2 | 247438 | 83233.800 | 89414.000 | 0.161657 | 0 |
| 4 | 371727 | 155965.362 | 152676.000 | 0.215212 | 0 |
| 8 | 691771 | 266960.275 | 244496.500 | 0.231290 | 0 |

## Failure / non-completion summary

| layout | clients | status | detail |
| --- | ---: | --- | --- |
| shared | 2 | abort | `Packet::setSatisfied(): Assertion '!flags.isSet(SATISFIED)' failed` |
| shared | 4 | abort | `Packet::setSatisfied(): Assertion '!flags.isSet(SATISFIED)' failed` |
| shared | 8 | abort | `BaseCache::handleFill(): Assertion 'pkt->hasData() || pkt->cmd == MemCmd::InvalidateResp' failed` |
| shared | 16 | abort | `BaseCache::handleFill(): Assertion 'pkt->hasData() || pkt->cmd == MemCmd::InvalidateResp' failed` |
| dedicated | 16 | timeout | switched to timing, but produced no request timing records before timeout |
| shared | 32 | timeout | never reached `switching cpus`; stayed in the first event queue / pre-switch stage |
| dedicated | 32 | timeout | same as shared 32 in this run; stayed in the first event queue / pre-switch stage |

## Key observations

1. Under the current classic + type3 path, the shared layout only produces a valid measurement at `client_count=1`.
2. The dedicated layout scales cleanly through `client_count=8`, and aggregate throughput increases monotonically:
   - `c1`: `0.101956 Mrps`
   - `c2`: `0.161657 Mrps`
   - `c4`: `0.215212 Mrps`
   - `c8`: `0.231290 Mrps`
3. `dedicated c16` is not an assertion abort. It entered timing mode but emitted no `client_*_req_*_{start,end}_ns` records before timeout.
4. `shared c32` and `dedicated c32` both failed to even complete the pre-switch stage within the wallclock budget used here. That wallclock timeout is outside the `m5_rpns` critical-path timing window, but it blocks obtaining a valid measurement for `c32`.

## Timing-window reminder

For both current experiment binaries:
- `start_ns` is stamped immediately before the client begins per-request work inside the main send/poll loop.
- The timing includes:
  - random payload generation
  - request packing into local memory
  - pointer/slot calculation inside the loop
  - copy from local memory to remote memory
  - queue token store + `clflushopt` + `sfence`
  - one poll attempt on the response queue
  - response load from remote memory
  - correctness check
  - `end_ns` stamp
- The timing excludes:
  - process creation
  - shared/CXL region allocation
  - NUMA binding
  - fork/wait setup
  - KVM boot
  - guest-side compilation before the timing run starts
