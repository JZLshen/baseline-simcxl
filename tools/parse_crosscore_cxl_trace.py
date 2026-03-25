#!/usr/bin/env python3
"""Summarize a cross-core CXL store/load transaction from CXLMemCtrl trace.

Heuristic:
- Find the latest address that has a write request followed by a read request.
- For that read request, pick the first downstream response events.
"""

from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from pathlib import Path


WRITE_RE = re.compile(
    r"^(\d+): .*cxl_rsp_port: recvTimingReq: (WriteClean|WriteReq) addr (0x[0-9a-fA-F]+)"
)
READ_REQ_RE = re.compile(
    r"^(\d+): .*cxl_rsp_port: recvTimingReq: (ReadCleanReq|ReadReq|ReadExReq) addr (0x[0-9a-fA-F]+)"
)
MEM_RESP_RE = re.compile(
    r"^(\d+): .*mem_req_port: recvTimingResp: (ReadResp|ReadExResp) addr (0x[0-9a-fA-F]+)"
)
RSP_SEND_RE = re.compile(
    r"^(\d+): .*cxl_rsp_port: trySend response addr (0x[0-9a-fA-F]+),"
)
BRIDGE_RESP_RE = re.compile(
    r"^(\d+): .*bridge\.mem_side_port: recvTimingResp: (ReadResp|ReadExResp) addr (0x[0-9a-fA-F]+), when tick(\d+)"
)


@dataclass
class Candidate:
    addr: str
    write_req_tick: int
    read_req_tick: int
    mem_resp_tick: int | None
    rsp_send_tick: int | None
    bridge_when_tick: int | None
    read_cmd: str


def first_ge(values: list[int], lower: int) -> int | None:
    for v in values:
        if v >= lower:
            return v
    return None


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("trace", type=Path)
    args = parser.parse_args()

    if not args.trace.exists():
        print(f"trace file not found: {args.trace}", file=sys.stderr)
        return 1

    writes: dict[str, list[int]] = {}
    reads: dict[str, list[tuple[int, str]]] = {}
    mem_resps: dict[str, list[int]] = {}
    rsp_sends: dict[str, list[int]] = {}
    bridge_whens: dict[str, list[int]] = {}

    with args.trace.open("r", encoding="utf-8", errors="replace") as f:
        for line in f:
            m = WRITE_RE.match(line)
            if m:
                tick = int(m.group(1))
                addr = m.group(3).lower()
                writes.setdefault(addr, []).append(tick)
                continue

            m = READ_REQ_RE.match(line)
            if m:
                tick = int(m.group(1))
                cmd = m.group(2)
                addr = m.group(3).lower()
                reads.setdefault(addr, []).append((tick, cmd))
                continue

            m = MEM_RESP_RE.match(line)
            if m:
                tick = int(m.group(1))
                addr = m.group(3).lower()
                mem_resps.setdefault(addr, []).append(tick)
                continue

            m = RSP_SEND_RE.match(line)
            if m:
                tick = int(m.group(1))
                addr = m.group(2).lower()
                rsp_sends.setdefault(addr, []).append(tick)
                continue

            m = BRIDGE_RESP_RE.match(line)
            if m:
                addr = m.group(3).lower()
                when_tick = int(m.group(4))
                bridge_whens.setdefault(addr, []).append(when_tick)

    candidates: list[Candidate] = []
    for addr, wlist in writes.items():
        rlist = reads.get(addr, [])
        if not rlist:
            continue
        r_ticks = [t for t, _ in rlist]
        for w in wlist:
            read_req_tick = first_ge(r_ticks, w)
            if read_req_tick is None:
                continue
            read_cmd = next(cmd for t, cmd in rlist if t == read_req_tick)
            cand = Candidate(
                addr=addr,
                write_req_tick=w,
                read_req_tick=read_req_tick,
                mem_resp_tick=first_ge(mem_resps.get(addr, []), read_req_tick),
                rsp_send_tick=first_ge(rsp_sends.get(addr, []), read_req_tick),
                bridge_when_tick=first_ge(bridge_whens.get(addr, []), read_req_tick),
                read_cmd=read_cmd,
            )
            candidates.append(cand)

    if not candidates:
        print("trace_summary_status=no_write_read_pair_found")
        return 0

    # Latest write-based pair is most likely the test transaction.
    best = max(candidates, key=lambda c: c.write_req_tick)

    def ns_from_ticks(ticks: int) -> float:
        # gem5 tick is 1 ps in this setup.
        return ticks / 1000.0

    print("trace_summary_status=ok")
    print(f"trace_selected_addr={best.addr}")
    print(f"trace_selected_read_cmd={best.read_cmd}")
    print(f"trace_write_req_tick={best.write_req_tick}")
    print(f"trace_read_req_tick={best.read_req_tick}")
    print(
        f"trace_write_to_read_req_ticks={best.read_req_tick - best.write_req_tick}"
    )
    print(
        "trace_write_to_read_req_ns="
        f"{ns_from_ticks(best.read_req_tick - best.write_req_tick):.3f}"
    )

    if best.mem_resp_tick is not None:
        print(f"trace_mem_resp_tick={best.mem_resp_tick}")
        print(
            f"trace_read_req_to_mem_resp_ticks={best.mem_resp_tick - best.read_req_tick}"
        )
        print(
            "trace_read_req_to_mem_resp_ns="
            f"{ns_from_ticks(best.mem_resp_tick - best.read_req_tick):.3f}"
        )

    if best.rsp_send_tick is not None:
        print(f"trace_rsp_send_tick={best.rsp_send_tick}")
        print(
            f"trace_read_req_to_rsp_send_ticks={best.rsp_send_tick - best.read_req_tick}"
        )
        print(
            "trace_read_req_to_rsp_send_ns="
            f"{ns_from_ticks(best.rsp_send_tick - best.read_req_tick):.3f}"
        )

    if best.bridge_when_tick is not None:
        print(f"trace_bridge_resp_tick={best.bridge_when_tick}")
        print(
            f"trace_read_req_to_bridge_resp_ticks={best.bridge_when_tick - best.read_req_tick}"
        )
        print(
            "trace_read_req_to_bridge_resp_ns="
            f"{ns_from_ticks(best.bridge_when_tick - best.read_req_tick):.3f}"
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
