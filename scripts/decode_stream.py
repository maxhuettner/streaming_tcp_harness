#!/usr/bin/env python3
"""
Decode a stream of MessagePack-encoded events to human-readable JSON.

Supports two input modes:
- stdin: pipe from `nc HOST PORT | ...` (default)
- direct connect: `--connect HOST:PORT`

Supports two framing modes:
- raw msgpack stream (default, matches current tcp_source)
- length-prefixed frames: `--length-prefixed` (matches tcp_sink)

Schema field orders:
- bid     (7): [auction, bidder, price, channel, url, dateTime_ms, extra]
- auction (10): [id, itemName, description, initialBid, reserve, dateTime_ms, expires_ms, seller, category, extra]
- person  (8): [id, name, emailAddress, creditCard, city, state, dateTime_ms, extra]
"""

import argparse
import datetime as dt
import json
import socket
import sys
from typing import BinaryIO, Iterable, Optional

try:
    import msgpack
except ModuleNotFoundError as e:
    sys.stderr.write("Missing dependency: msgpack. Install with: pip install msgpack\n")
    raise


FIELDS = {
    "bid": [
        "auction",
        "bidder",
        "price",
        "channel",
        "url",
        "dateTime_ms",
        "extra",
    ],
    "auction": [
        "id",
        "itemName",
        "description",
        "initialBid",
        "reserve",
        "dateTime_ms",
        "expires_ms",
        "seller",
        "category",
        "extra",
    ],
    "person": [
        "id",
        "name",
        "emailAddress",
        "creditCard",
        "city",
        "state",
        "dateTime_ms",
        "extra",
    ],
}


def ms_to_iso(ms: int) -> str:
    try:
        return dt.datetime.utcfromtimestamp(ms / 1000).isoformat(timespec="milliseconds") + "Z"
    except Exception:
        return ""


def map_record(obj, schema: str, add_iso: bool) -> object:
    # If it's a list with the expected arity, map to dict; otherwise pass through as-is.
    fields = FIELDS[schema]
    if isinstance(obj, list) and len(obj) == len(fields):
        rec = dict(zip(fields, obj))
        if add_iso:
            if "dateTime_ms" in rec and isinstance(rec["dateTime_ms"], int):
                rec["dateTime"] = ms_to_iso(rec["dateTime_ms"]) or rec["dateTime_ms"]
            if "expires_ms" in rec and isinstance(rec.get("expires_ms"), int):
                rec["expires"] = ms_to_iso(rec["expires_ms"]) or rec["expires_ms"]
        return rec
    return obj


def iter_raw_unpack(stream: BinaryIO) -> Iterable[object]:
    unpacker = msgpack.Unpacker(stream, raw=False)
    for obj in unpacker:
        yield obj


def iter_len_prefixed(stream: BinaryIO) -> Iterable[object]:
    read = stream.read
    while True:
        hdr = read(4)
        if not hdr or len(hdr) < 4:
            return
        length = int.from_bytes(hdr, "little")
        if length <= 0:
            continue
        buf = bytearray()
        remaining = length
        while remaining:
            chunk = read(remaining)
            if not chunk:
                return
            buf.extend(chunk)
            remaining -= len(chunk)
        try:
            yield msgpack.unpackb(bytes(buf), raw=False)
        except Exception:
            # Skip malformed payloads
            continue


def open_socket_stream(connect: Optional[str]) -> Optional[BinaryIO]:
    if not connect:
        return None
    host, port_s = connect.split(":", 1)
    port = int(port_s)
    s = socket.create_connection((host, port))
    return s.makefile("rb")


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--schema", choices=FIELDS.keys(), default="bid", help="Schema mapping for arrays")
    p.add_argument("--connect", metavar="HOST:PORT", help="Connect directly instead of reading stdin")
    p.add_argument("--length-prefixed", action="store_true", help="Read 4-byte LE length-prefixed frames")
    p.add_argument("--count", type=int, default=10, help="Max records to print (0 = unlimited)")
    p.add_argument("--pretty", action="store_true", help="Pretty-print JSON")
    p.add_argument("--iso-time", action="store_true", help="Add ISO timestamps derived from *_ms fields")
    args = p.parse_args()

    stream = open_socket_stream(args.connect) or sys.stdin.buffer

    iterator = iter_len_prefixed(stream) if args.length_prefixed else iter_raw_unpack(stream)

    printed = 0
    for obj in iterator:
        mapped = map_record(obj, args.schema, add_iso=args.iso_time)
        if args.pretty:
            print(json.dumps(mapped, ensure_ascii=False, indent=2))
        else:
            print(json.dumps(mapped, ensure_ascii=False))
        printed += 1
        if args.count and printed >= args.count:
            break

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

