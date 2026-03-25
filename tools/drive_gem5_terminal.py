#!/usr/bin/env python3
import argparse
import socket
import sys
import time
from pathlib import Path


def parse_args():
    parser = argparse.ArgumentParser(
        description="Drive a gem5 serial terminal over TCP and capture output."
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--log", required=True)
    parser.add_argument("--command-file", default="")
    parser.add_argument("--wait-for", default="")
    parser.add_argument("--expect", default="")
    parser.add_argument("--connect-timeout", type=float, default=120.0)
    parser.add_argument("--pre-send-timeout", type=float, default=60.0)
    parser.add_argument("--post-send-timeout", type=float, default=3600.0)
    parser.add_argument("--expect-idle-timeout", type=float, default=5.0)
    parser.add_argument("--send-delay", type=float, default=1.0)
    return parser.parse_args()


def connect_with_retry(host, port, timeout_s):
    deadline = time.monotonic() + timeout_s
    last_error = None
    while time.monotonic() < deadline:
        try:
            sock = socket.create_connection((host, port), timeout=2.0)
            sock.settimeout(0.5)
            return sock
        except OSError as exc:
            last_error = exc
            time.sleep(0.5)
    raise RuntimeError(
        f"failed to connect to {host}:{port} within {timeout_s:.1f}s: {last_error}"
    )


def main():
    args = parse_args()
    command = ""
    should_send = bool(args.command_file)
    if should_send:
        command = Path(args.command_file).read_text(encoding="utf-8")
        if not command.endswith("\n"):
            command += "\n"

    sock = connect_with_retry(args.host, args.port, args.connect_timeout)
    log_path = Path(args.log)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    sent = False
    seen_expect = False
    wait_deadline = time.monotonic() + args.pre_send_timeout
    overall_deadline = time.monotonic() + args.post_send_timeout
    idle_deadline = None
    tail = b""

    with log_path.open("wb") as log_file:
        while True:
            now = time.monotonic()
            if should_send and not sent and now >= wait_deadline:
                sock.sendall(b"\n")
                time.sleep(args.send_delay)
                sock.sendall(command.encode("utf-8"))
                sent = True
                continue

            if now >= overall_deadline:
                raise RuntimeError(
                    f"terminal command did not finish within {args.post_send_timeout:.1f}s"
                )

            if idle_deadline is not None and now >= idle_deadline:
                return 0

            try:
                data = sock.recv(4096)
            except socket.timeout:
                continue

            if not data:
                return 0

            log_file.write(data)
            log_file.flush()

            tail = (tail + data)[-16384:]
            decoded_tail = tail.decode("utf-8", errors="ignore")

            if should_send and not sent:
                if args.wait_for:
                    if args.wait_for in decoded_tail:
                        sock.sendall(b"\n")
                        time.sleep(args.send_delay)
                        sock.sendall(command.encode("utf-8"))
                        sent = True
                else:
                    sock.sendall(b"\n")
                    time.sleep(args.send_delay)
                    sock.sendall(command.encode("utf-8"))
                    sent = True

            if should_send and args.expect and not seen_expect and args.expect in decoded_tail:
                seen_expect = True
                idle_deadline = time.monotonic() + args.expect_idle_timeout


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"drive_gem5_terminal.py: {exc}", file=sys.stderr)
        raise SystemExit(1)
