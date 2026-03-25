#!/usr/bin/env python3
import argparse
import socket
import sys
import time
from pathlib import Path


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Drive a gem5 serial terminal from the host. The tool waits until "
            "a shell starts consuming input, sends a command file, and keeps "
            "recording terminal output until the socket closes."
        )
    )
    parser.add_argument("--host", default="127.0.0.1", help="Terminal host")
    parser.add_argument("--port", type=int, required=True, help="Terminal TCP port")
    parser.add_argument(
        "--command-file",
        required=True,
        help="Host file whose contents are sent to the guest shell",
    )
    parser.add_argument(
        "--transcript",
        default="",
        help="Optional host file capturing the full terminal transcript",
    )
    parser.add_argument(
        "--connect-timeout",
        type=float,
        default=120.0,
        help="Seconds to wait for the terminal socket to accept connections",
    )
    parser.add_argument(
        "--shell-timeout",
        type=float,
        default=120.0,
        help="Seconds to wait until the guest shell executes probe commands",
    )
    parser.add_argument(
        "--disconnect-timeout",
        type=float,
        default=600.0,
        help="Seconds to wait for the socket to close after commands are sent",
    )
    parser.add_argument(
        "--probe-interval",
        type=float,
        default=1.0,
        help="Seconds between shell probe attempts",
    )
    return parser.parse_args()


def open_socket(host, port, timeout_s):
    deadline = time.monotonic() + timeout_s
    last_error = None
    while time.monotonic() < deadline:
        try:
            sock = socket.create_connection((host, port), timeout=1.0)
            sock.settimeout(0.2)
            return sock
        except OSError as exc:
            last_error = exc
            time.sleep(0.2)
    raise TimeoutError(
        f"timed out connecting to {host}:{port}: {last_error}"
    ) from last_error


def read_available(sock):
    chunks = []
    while True:
        try:
            chunk = sock.recv(4096)
        except socket.timeout:
            break
        if not chunk:
            return b"".join(chunks), True
        chunks.append(chunk)
    return b"".join(chunks), False


def append_transcript(output_path, chunk):
    if not output_path or not chunk:
        return
    with open(output_path, "ab") as fh:
        fh.write(chunk)


def normalize_commands(path):
    data = Path(path).read_text(encoding="utf-8")
    data = data.replace("\r\n", "\n").replace("\r", "\n")
    if not data.endswith("\n"):
        data += "\n"
    return data.replace("\n", "\r").encode("utf-8")


def wait_for_shell(sock, args):
    marker = f"__SERIAL_READY_{int(time.time() * 1000)}__".encode("utf-8")
    marker_patterns = (
        b"\r\n" + marker + b"\r\n",
        b"\n" + marker + b"\r\n",
        b"\n" + marker + b"\n",
    )
    deadline = time.monotonic() + args.shell_timeout
    last_probe = 0.0
    recent = bytearray()
    while time.monotonic() < deadline:
        chunk, closed = read_available(sock)
        if chunk:
            append_transcript(args.transcript, chunk)
            recent.extend(chunk)
            if len(recent) > 65536:
                del recent[:-65536]
            if any(pattern in recent for pattern in marker_patterns):
                return
        if closed:
            raise RuntimeError("terminal closed before the guest shell became ready")

        now = time.monotonic()
        if now - last_probe >= args.probe_interval:
            sock.sendall(b"\r")
            sock.sendall(b"printf '")
            sock.sendall(marker)
            sock.sendall(b"\\n'\r")
            last_probe = now
        time.sleep(0.1)

    raise TimeoutError("guest shell never acknowledged the serial probe")


def run_commands(sock, args):
    payload = normalize_commands(args.command_file)
    sock.sendall(payload)

    deadline = time.monotonic() + args.disconnect_timeout
    while time.monotonic() < deadline:
        chunk, closed = read_available(sock)
        if chunk:
            append_transcript(args.transcript, chunk)
        if closed:
            return
        time.sleep(0.1)

    raise TimeoutError("terminal stayed open past disconnect timeout")


def main():
    args = parse_args()
    if args.transcript:
        Path(args.transcript).parent.mkdir(parents=True, exist_ok=True)
        Path(args.transcript).write_bytes(b"")

    try:
        sock = open_socket(args.host, args.port, args.connect_timeout)
        try:
            chunk, closed = read_available(sock)
            if chunk:
                append_transcript(args.transcript, chunk)
            if closed:
                raise RuntimeError("terminal closed immediately after connect")
            wait_for_shell(sock, args)
            run_commands(sock, args)
        finally:
            sock.close()
    except Exception as exc:
        print(f"drive_guest_serial.py: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
