#!/usr/bin/env python3
"""
sequencer-feed-monitor.py -- pick, and keep, the fast WebSocket connections to a
Cloudflare-fronted feed.

WHY THIS EXISTS
===============
On a Cloudflare-fronted feed, *which connection you happen to get* matters more
than anything you can buy. Three independent lotteries are drawn at connect time
and then FIXED for the life of that connection:

  1. THE RETURN PATH. A large fraction of connections take a route two hops
     longer on the return leg. Measured on one path: +6.3 ms, ~45-50% of
     connections, selected by a hash of the connection 4-tuple. The source port
     is the only part of that tuple a client controls -- so the path IS
     selectable: bind a port, time the handshake, keep it if fast, discard it if
     slow.

  2. THE ORIGIN. Cloudflare Load Balancing pins each connection to one of a small
     number of origin instances, named in the `__cflb` cookie on the upgrade
     response. Measured arrival-lag spread between the best and worst origin:
     ~9.4 ms -- LARGER than the return-path defect, and free to avoid, because
     the cookie tells you which one you got.

  3. THE COLO. Cloudflare's anycast decides which edge serves you; `cf-ray`
     reports it. From a fixed location you do not control this, so this file only
     records it.

So the strategy is: open more connections than you need, measure all three, keep
the winners, drop the rest -- then keep scoring the survivors, because a
connection that was fast at 09:00 can fall behind by 11:00 and you want to know.

WHAT WILL FOOL YOU IF YOU WRITE THIS YOURSELF
=============================================
Every one of these produced a confident, plausible, WRONG number before it was
fixed. They are the reason this file is longer than it looks like it should be.

  * A REPLAYED BACKLOG IS NOT A RACE. The feed replays history to every new
    connection -- in one measurement 85% of all messages arrived in the first ten
    seconds, at ~275/s against a live rate of ~9.5/s. A connection scored during
    its own replay "wins" every message by construction. Admit a new connection
    at the pool's current high-water sequence (`admit_above`), and discard its
    samples until it has actually caught up.

  * CATCHING UP MAKES IT LOOK SLOW, NOT FAST. While a new connection streams
    history, the live messages are arriving too, and it delivers those behind
    everyone else. Charged naively, challengers scored 17-244 ms against
    incumbents at 3-6 ms -- which reads as "aged connections are better, never
    redial", the exact opposite of the truth.

  * WINNING IS NOT A LAG OF ZERO. Score a connection only on races it LOSES. If
    a win writes 0.0 into its own sample list, anything winning more than half
    its races reports a median lag of exactly 0.000 forever.

  * "FAST" IS A DIFFERENCE, SO IT MUST BE RELATIVE. The defect is +6.3 ms
    *versus the other connections in the same cohort*. An absolute threshold
    marks every connection from a distant host as slow, and the column you wanted
    to stratify everything else by is uniformly false.

  * DO NOT PROBE-THEN-RECONNECT ON THE SAME 4-TUPLE. Reusing a 4-tuple straight
    after an RST gets the SYN dropped and the kernel waits out its retransmit
    timer: one measured handshake took 1034 ms instead of 1.3 ms and was recorded
    as a return-path measurement. Measure on the socket you intend to keep.

  * DISCARD A LOSING PROBE WITH RST, NOT FIN. A clean close parks the 4-tuple in
    TIME_WAIT, and the next probe in the port walk gets handed that same port.

  * ICMP CANNOT SEE ANY OF THIS. ping and default mtr carry one fixed flow tuple,
    so they draw one path and stay on it. On one host, two VIPs gave two OPPOSITE
    wrong pictures -- one looked flawless, one uniformly slow, neither showed a
    split. Only TCP with pinned source ports sees it.

USAGE
=====
    # open 12 candidates, keep the best 4, print what you drew
    python3 sequencer-feed-monitor.py race --keep 4 --probe 12

    # keep them open and print a live scoreboard of which is leading
    python3 sequencer-feed-monitor.py watch --keep 4 --probe 12 --seconds 300

    # a different feed / message-id field
    python3 sequencer-feed-monitor.py race --host feed.example.com --seq-field '"seq":\\s*(\\d+)'

Standard library only. Python 3.9+.
"""

from __future__ import annotations

import argparse
import base64
import errno
import hashlib
import json
import os
import re
import socket
import ssl
import struct
import zlib
import sys
import time
from dataclasses import dataclass, field

# ---------------------------------------------------------------------------
# Tunables
# ---------------------------------------------------------------------------

DEFAULT_HOST = "feed.mainnet.chain.robinhood.com"

# A connection is on the fast path if its handshake is within this many ms of the
# BEST in its own cohort. Relative, never absolute -- see the notes above.
FAST_PATH_MARGIN_MS = 4.0

# Leading chars of the __cflb cookie that identify the origin instance.
ORIGIN_PREFIX = 24

# ⛔ MEASURED 2026-09-09, and it corrects an earlier belief that shaped this file.
#
# The feed does NOT rate-limit upgrades over time. It caps how many websockets one
# SOURCE ADDRESS may HOLD AT ONCE. Measured against the live endpoint:
#
#     6 upgrades sequentially, each closed before the next  -> 6/6 accepted
#     6 upgrades concurrently, held open                    -> 2 accepted, then 429
#     release the two, retry immediately                    -> accepted (NO cooldown)
#     a different address while the first sat refusing      -> accepted
#
# So: sequential probing is unlimited, concurrency is not, the cap is per address,
# and there is no penalty window. An earlier note in this file claimed "~175
# upgrades in five minutes earned a 429 for the next half hour" -- that describes a
# rate limit, and no rate limit of that shape exists.
#
# CONSEQUENCE FOR THIS TOOL: you cannot hold more than the cap from one address, and
# because the __cflb origin is drawn AT UPGRADE TIME, you cannot score a connection,
# close it, and re-open "the same" one -- a re-open is a fresh draw. The only way to
# keep a good origin is to never close it. So the strategy is: hold up to the cap,
# score them, drop the worst, redraw, repeat. To hold N connections you need
# ceil(N / cap) distinct source addresses.
#
# The cap is DISCOVERED at runtime rather than hardcoded, because it has moved
# before: it was recorded as 5 per IP earlier in 2026 and measured as 2 tonight.
# ⛔ No fallback constant. The cap is ONLY ever learned from a real 429; a
# hardcoded default is exactly what this endpoint punishes, because the number
# moves — 2, 3, 4 and 5 have all been measured on one address.
CLOSE_SETTLE_S = 0.6             # measured: 0.25 s was too short, the reopen
                                 # still raced the close and lost draws
MAX_PROBES_PER_SOURCE = 64       # sequential probes are cheap; this is just a bound

# A message more than this far below the pool high-water mark is replayed
# history, not a late arrival.
CATCHUP_TOLERANCE = 10

# ⛔ A HEADLINE NUMBER MUST NOT BE SET BY A CONNECTION THAT BARELY RACED.
# Below this many races a connection is not ranked and cannot be recommended.
# Without it, a short run where only one connection happened to score reports
# that connection at 100% and recommends it -- even when it is on the SLOW path.
# Observed exactly that: a 25 s run recommended an 8.77 ms slow-path connection
# at "100.0%" off 237 races while the fast one had raced 0 times.
MIN_LAG_SAMPLES = 5              # below this a "median" lag is just one observation
MIN_RACE_SAMPLES = 10

# Keep this many lag samples per connection. Enough for a stable median.
LAG_WINDOW = 4000

# Remember this many recent sequences to decide "who got it first".
SEEN_WINDOW = 5000

RECV_BYTES = 262144
MAX_READS_PER_POLL = 16          # bounded, so one chatty peer cannot starve others
MAX_FRAME_BYTES = 8 * 1024 * 1024

WS_GUID = b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11"          # RFC6455 §1.3


# ---------------------------------------------------------------------------
# THE ONE PERCENTILE CONVENTION. This helper is byte-for-byte identical in every
# tool we publish, and it must stay that way: two tools under the same name that
# disagree about how to compute a median is a defect in the product, not a
# footnote. If you change it here, change it everywhere.
#
# Sort ascending and return the observed value at index floor(p * (n-1)),
# zero-based, clamped to n-1. NOTHING IS INTERPOLATED, which matters more than
# it sounds: the textbook median averages the two middle values on an even n and
# therefore reports a latency that no connection ever produced. Every number
# these tools print is a measurement that actually happened.
# ---------------------------------------------------------------------------
def pctl(values, p):
    """The observed value at floor(p * (n-1)). No interpolation. None if empty."""
    if not values:
        return None
    v = sorted(values)
    return v[min(len(v) - 1, int(p * (len(v) - 1)))]

CFLB_RE = re.compile(rb"(?im)^set-cookie:\s*__cflb=([^;]+)")
RAY_RE = re.compile(rb"(?im)^cf-ray:\s*([^\r\n]+)")
ACCEPT_RE = re.compile(rb"(?im)^sec-websocket-accept:\s*([^\r\n]+)")
DEFAULT_SEQ_RE = rb'"sequenceNumber":\s*(\d+)'


# ---------------------------------------------------------------------------
# One connection
# ---------------------------------------------------------------------------

@dataclass
class Conn:
    """One connection, and everything we know about which lotteries it won."""
    sock: ssl.SSLSocket
    sport: int
    handshake_ms: float
    origin: str
    ray: str
    colo: str
    opened_at: float
    dead: bool = False              # peer closed: readable forever, recv gives b""
    buf: bytearray = field(default_factory=bytearray)

    # Set once the whole cohort's handshakes are known. None = no cohort.
    path_is_fast: bool | None = None

    # Messages at or below this are THIS connection's replay backlog, not races.
    admit_above: int = 0
    backlog_skipped: int = 0
    caught_up: bool = False
    catchup_skipped: int = 0
    relapses: int = 0

    lags_ms: list[float] = field(default_factory=list)
    firsts: int = 0                 # races won (recorded as a count, NOT as 0.0 lag)
    races: int = 0
    last_seq: int = 0

    @property
    def fast_path(self) -> bool:
        if self.path_is_fast is not None:
            return self.path_is_fast
        return self.handshake_ms < FAST_PATH_MARGIN_MS

    @property
    def median_lag_ms(self) -> float | None:
        # Was: v[n//2] on odd n, (v[n//2-1] + v[n//2]) / 2 on even n. That second
        # branch AVERAGES two samples and so reports a lag no message ever had.
        #
        # On two samples pctl(v, .5) is just the SMALLER of them. Printing that to
        # three decimals under a column headed median_lag_ms invites a decision it
        # cannot support, so below MIN_LAG_SAMPLES this reports nothing.
        if len(self.lags_ms) < MIN_LAG_SAMPLES:
            return None
        return pctl(self.lags_ms, 0.5)

    def __str__(self) -> str:
        return (f"sport={self.sport} hs={self.handshake_ms:6.3f}ms "
                f"{'FAST' if self.fast_path else 'slow'} "
                f"origin={origin_label(self.origin)} colo={self.colo}")


_ORIGIN_LABELS: dict[str, str] = {}


def origin_label(full: str) -> str:
    """Stable short label (A, B, C...) for an origin id.

    The raw `__cflb` value is long and its distinguishing characters are NOT at
    the front -- printing a truncated prefix rendered two DIFFERENT origins
    identically while the summary line correctly said they differed. A label
    cannot do that.
    """
    if full not in _ORIGIN_LABELS:
        n = len(_ORIGIN_LABELS)
        _ORIGIN_LABELS[full] = chr(ord("A") + n) if n < 26 else f"O{n}"
    return _ORIGIN_LABELS[full]


def origin_legend() -> str:
    return "  ".join(f"{lbl}={full}" for full, lbl in sorted(_ORIGIN_LABELS.items(),
                                                             key=lambda kv: kv[1]))


def _hard_close(sock) -> None:
    """RST, not FIN -- a FIN parks the 4-tuple in TIME_WAIT and poisons the port walk."""
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
    except OSError:
        pass
    try:
        sock.close()
    except OSError:
        pass


def _ws_key() -> str:
    return base64.b64encode(os.urandom(16)).decode()


def _ws_accept(key: str) -> bytes:
    return base64.b64encode(hashlib.sha1(key.encode() + WS_GUID).digest())


def open_one(host: str, ip: str, sport: int, *, path: str = "/",
             timeout: float = 8.0, src_ip: str | None = None) -> Conn:
    """
    Open ONE connection and record which lotteries it won.

    The TCP handshake time IS the return-path measurement. It is taken on the
    socket we intend to keep -- never probe-then-reconnect on the same 4-tuple.

    `timeout` is a deadline for the WHOLE upgrade, not per-recv: checking it only
    at the top of the loop while each recv carries the full timeout lets a peer
    that dribbles one byte buy another entire timeout.
    """
    family = socket.AF_INET6 if ":" in ip else socket.AF_INET
    s = socket.socket(family, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    # The cap is PER SOURCE ADDRESS, so choosing the source address is the whole
    # remedy this tool recommends. It used to print that advice and offer no flag.
    bind_ip = src_ip or ""
    s.bind((bind_ip, sport) if family == socket.AF_INET else (bind_ip, sport, 0, 0))
    s.settimeout(timeout)

    t0 = time.monotonic()
    s.connect((ip, 443))
    handshake_ms = (time.monotonic() - t0) * 1000.0

    try:
        ctx = ssl.create_default_context()
        tls = ctx.wrap_socket(s, server_hostname=host)
    except BaseException:
        _hard_close(s)
        raise

    # ONE exit for every post-connect failure, and it goes through _hard_close.
    try:
        key = _ws_key()
        # ⛔ permessage-deflate IS MANDATORY ON THIS ENDPOINT SINCE 2026-09-17.
        # The feed went compressed-only on that date. Without this header the server
        # answers 400 Bad Request and no stream ever starts -- which looks exactly
        # like an outage and is not one. MEASURED 2026-09-19: without it 400, with it
        # "HTTP/1.1 101 Switching Protocols" and
        # "sec-websocket-extensions: permessage-deflate; server_no_context_takeover;
        #  client_no_context_takeover".
        # ⭐ We REQUEST no_context_takeover in both directions on purpose: it makes every
        # message independently compressed, so a decoder needs no cross-message state and
        # a dropped/late frame cannot corrupt the ones after it.
        req = (f"GET {path} HTTP/1.1\r\n"
               f"Host: {host}\r\n"
               f"Upgrade: websocket\r\n"
               f"Connection: Upgrade\r\n"
               f"Sec-WebSocket-Version: 13\r\n"
               f"Sec-WebSocket-Extensions: permessage-deflate; "
               f"client_no_context_takeover; server_no_context_takeover\r\n"
               f"Sec-WebSocket-Key: {key}\r\n\r\n")
        tls.sendall(req.encode())

        head = b""
        deadline = time.monotonic() + timeout
        while b"\r\n\r\n" not in head:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("upgrade timed out")
            tls.settimeout(remaining)
            chunk = tls.recv(65536)
            if not chunk:
                raise ConnectionError("closed during upgrade")
            head += chunk
        tls.settimeout(timeout)

        status = head.split(b"\r\n", 1)[0].decode(errors="replace")
        # The STATUS CODE, not a substring. `"101" in status` also accepts
        # "HTTP/1.1 503 Service Unavailable ray 101ab" -- and that connection then
        # occupies a slot for the whole run and never produces a message.
        parts = status.split()
        if len(parts) < 2 or parts[1] != "101":
            raise ConnectionError(f"upgrade refused: {status}")

        # RFC6455 requires the server to echo a key-derived accept value. Checking
        # it is the protocol's own identity test -- cheap, and it catches a
        # middlebox answering on the server's behalf.
        m = ACCEPT_RE.search(head)
        if not m or m.group(1).strip() != _ws_accept(key):
            raise ConnectionError("bad Sec-WebSocket-Accept")

        mo = CFLB_RE.search(head)
        origin = mo.group(1).decode()[:ORIGIN_PREFIX] if mo else "unknown"
        mr = RAY_RE.search(head)
        ray = mr.group(1).decode().strip() if mr else "?"
        colo = ray.rsplit("-", 1)[-1] if "-" in ray else "?"

        head_rest = head.split(b"\r\n\r\n", 1)[1]
        c = Conn(sock=tls, sport=sport, handshake_ms=handshake_ms, origin=origin,
                 ray=ray, colo=colo, opened_at=time.monotonic())
        c.buf.extend(head_rest)
        return c
    except BaseException:
        _hard_close(tls)
        raise


# ---------------------------------------------------------------------------
# The race
# ---------------------------------------------------------------------------

def race_connections(host: str, ip: str, want: int, probe: int, *,
                     sport_base: int = 61000, path: str = "/", log=print,
                     src_ip: str | None = None) -> list[Conn]:
    """Keep the best `want` connections, drawing up to `probe` times.

    ⛔ You cannot open `probe` connections at once and pick from them: one source
    address holds only a couple, and the number moves. And because the
    origin is drawn AT UPGRADE TIME, a connection you close is gone -- re-opening
    gives a fresh draw, not the one you liked.

    So this holds up to the cap, scores what it holds, drops the worst, and draws a
    replacement, `probe` draws in total. What survives is genuinely the best of
    `probe` draws, and it was never closed.

    To hold more than the cap, run one instance per source address -- ceil(N / cap)
    of them for N connections. That, and not any rate limit, is what the IPv6
    allocation is for.
    """
    probe = min(probe, MAX_PROBES_PER_SOURCE)
    got: list[Conn] = []
    cap = None            # learned from the first refusal; None means "not yet known"
    refusals = 0
    port = sport_base
    ceiling = sport_base + probe * 20        # bounded, so a systemic failure still ends

    # `probe` counts SUCCESSFUL draws, not attempts. A refusal at the cap is not a
    # draw -- it is the server telling us to wait -- and counting it as one silently
    # shrinks the sample, which on a lottery means fewer origins seen. Attempts are
    # bounded separately so a systemic failure still terminates.
    attempts = 0
    max_attempts = probe * 4
    i = 0
    while i < probe and attempts < max_attempts:
        attempts += 1
        # The cap is LEARNED, not probed for. A dedicated discovery phase costs a
        # full cap's worth of slots and then the real draws race its own closes --
        # measured 6 of 8 draws lost that way. A 429 already tells us we are at the
        # cap, and it is the cheapest possible signal.
        if cap is not None and len(got) >= cap:
            worst = max(got, key=lambda c: c.handshake_ms)
            got.remove(worst)
            # ⛔ NOT worst.close(): Conn is a dataclass with no close(), so that
            # raised AttributeError into a bare except and the socket stayed OPEN --
            # off the list, still holding its server slot, leaked for the life of
            # the process. It also has to leave with RST, not FIN.
            _hard_close(worst.sock)
            # Let the far side release the slot before asking for it back. Without
            # this the replacement draw races the close and is refused: measured
            # 3 of 8 draws wasted at 0 ms, 0 of 8 at 250 ms. The wasted draws are
            # not free -- each one is an origin you never got to see.
            time.sleep(CLOSE_SETTLE_S)

        # Walk forward past unusable ports. A port left over from a previous run is
        # not a measurement failure -- treating it as one silently shrinks the
        # cohort, which is how a cohort of four becomes zero unnoticed.
        # Errnos are SYMBOLIC: EADDRINUSE is 98 on Linux and 48 on macOS.
        while port < ceiling:
            try:
                c = open_one(host, ip, port, path=path, src_ip=src_ip)
                got.append(c)
                log(f"  draw {i}: {c}")
                port += 1
                i += 1
                break
            except ConnectionError as exc:
                if "429" in str(exc):
                    # ⛔ Record the cap ONCE, on the first refusal, and never move it.
                    # An earlier version re-derived it on every 429 -- but by then a
                    # slot had already been freed, so it read one lower each time and
                    # ratcheted 4 -> 3 -> 2 -> 1 -> keep nothing. The cap is the number
                    # we were holding the FIRST time we were told no.
                    if cap is None:
                        cap = max(1, len(got))
                        log(f"  draw {i}: at cap — this address holds {cap} "
                            f"concurrent websocket(s)")
                        if want > cap:
                            log(f"  ! asked to keep {want}; keeping {cap}. To hold "
                                f"{want}, run {-(-want // cap)} instances, one per "
                                f"source address.")
                    else:
                        log(f"  draw {i}: refused at cap {cap}")
                    refusals += 1
                    port += 1
                    time.sleep(CLOSE_SETTLE_S)   # do NOT advance i: retry this draw
                    break
                log(f"  draw {i}: {type(exc).__name__}: {exc}")
                port += 1
                i += 1
                break
            except OSError as exc:
                if exc.errno in (errno.EADDRINUSE, errno.EADDRNOTAVAIL, errno.EACCES):
                    port += 1
                    continue
                log(f"  draw {i}: {type(exc).__name__}: {exc}")
                port += 1
                i += 1
                break
            except Exception as exc:                              # noqa: BLE001
                log(f"  draw {i}: {type(exc).__name__}: {exc}")
                port += 1
                i += 1
                break
        time.sleep(0.15)                                          # gentle on the feed

    if cap is not None and want > cap:
        want = cap
    if refusals:
        log(f"  ({refusals} draw(s) refused at the cap; that is expected and not an error)")
    return select_best(got, want, log=log)


def select_best(got: list[Conn], want: int, log=print) -> list[Conn]:
    """
    Keep the best `want`: fast path first, then SPREAD ACROSS ORIGINS, then backfill.

    Diversifying across origins matters more than it looks. If every connection
    lands on the same origin and that origin is the slow one, no number of
    connections helps you.
    """
    if got:
        # The return-path lottery is a DIFFERENCE, so classify against the cohort
        # it was drawn with, never against an absolute threshold.
        best_ms = min(c.handshake_ms for c in got)
        for c in got:
            c.path_is_fast = (c.handshake_ms - best_ms) < FAST_PATH_MARGIN_MS

    got.sort(key=lambda c: (not c.fast_path, c.handshake_ms))
    keep: list[Conn] = []
    kept_ids: set[int] = set()
    seen_origins: set[str] = set()

    for c in got:
        if len(keep) >= want:
            break
        if c.origin in seen_origins and len(seen_origins) < 4:
            continue                                              # hold out for variety
        keep.append(c); kept_ids.add(id(c)); seen_origins.add(c.origin)

    for c in got:                                                 # backfill if variety ran out
        if len(keep) >= want:
            break
        if id(c) not in kept_ids:
            keep.append(c); kept_ids.add(id(c))

    for c in got:
        if id(c) not in kept_ids:
            _hard_close(c.sock)                                   # discarded probe: RST
    log(f"  keeping {len(keep)} of {len(got)}: "
        f"{sum(c.fast_path for c in keep)} fast-path, "
        f"{len(set(c.origin for c in keep))} distinct origin(s) "
        f"[{', '.join(sorted(origin_label(c.origin) for c in keep))}]")
    return keep


# ---------------------------------------------------------------------------
# Reading frames
# ---------------------------------------------------------------------------

def iter_frames(c: Conn):
    """Yield complete (opcode, payload) already buffered on this connection."""
    while True:
        b = c.buf
        if len(b) < 2:
            return
        b1 = b[1]
        ln = b1 & 0x7F
        off = 2
        if ln == 126:
            if len(b) < 4:
                return
            ln = struct.unpack(">H", b[2:4])[0]; off = 4
        elif ln == 127:
            if len(b) < 10:
                return
            ln = struct.unpack(">Q", b[2:10])[0]; off = 10
        if ln > MAX_FRAME_BYTES:
            # ⛔ DRAIN BEFORE RAISING. Leaving the bytes in the buffer meant the same
            # error raised on every subsequent poll, forever, while the buffer grew
            # without bound -- a hot log-spam loop that never dropped the connection.
            # A desync we cannot resynchronise is a dead connection.
            b.clear()
            raise ConnectionError(f"frame too large: {ln} — desynchronised")
        if b1 & 0x80:                                              # server frames must not be masked
            off += 4
        if len(b) < off + ln:
            return
        op = b[0] & 0x0F
        rsv1 = bool(b[0] & 0x40)          # permessage-deflate marks compressed frames here
        payload = bytes(b[off:off + ln])
        del b[:off + ln]
        if rsv1 and op in (1, 2):
            # ⛔ SINCE 2026-09-17 THE FEED IS COMPRESSED-ONLY, so every data frame arrives
            # DEFLATE'd and MUST be inflated before anything can parse a sequence number
            # out of it. Offering the extension without doing this is WORSE than not
            # offering it: the upgrade succeeds, the connection looks healthy, and the
            # run silently scores ZERO races. Measured exactly that way on 2026-09-19
            # ("admitted above sequence 0", races 0) before this was added.
            # ⭐ We negotiate server_no_context_takeover, so each message is compressed
            # independently and a FRESH decompressor per message is correct -- no state
            # carries between messages, so one bad frame cannot corrupt the next.
            # The 4-byte tail is the empty deflate block RFC 7692 §7.2.2 requires callers
            # to append before inflating a permessage-deflate payload.
            try:
                payload = zlib.decompressobj(-zlib.MAX_WBITS).decompress(
                    payload + b"\x00\x00\xff\xff")
            except zlib.error as e:
                raise ConnectionError(f"deflate failed: {e} — desynchronised") from None
        yield op, payload


# ---------------------------------------------------------------------------
# The pool
# ---------------------------------------------------------------------------

class Pool:
    """Holds the kept connections and scores which of them is actually leading."""

    def __init__(self, conns: list[Conn], seq_re: bytes = DEFAULT_SEQ_RE, log=print):
        self.conns = conns
        self.seq_re = re.compile(seq_re)
        self.log = log
        self.seen: dict[int, float] = {}        # seq -> monotonic time of FIRST arrival
        self.high_water = 0

    def _admit(self, c: Conn) -> None:
        c.admit_above = self.high_water

    def poll(self, timeout: float = 0.25) -> int:
        """Read what is available; score arrivals. Returns messages processed."""
        import select as _select
        for c in [c for c in self.conns if c.dead]:
            self.log(f"  {c.sport}: peer closed — dropping it from the pool")
            _hard_close(c.sock)
            self.conns.remove(c)
        socks = [c.sock for c in self.conns if c.sock and not c.dead]
        if not socks:
            return 0
        ready, _, _ = _select.select(socks, [], [], timeout)
        by_sock = {c.sock: c for c in self.conns}
        processed = 0
        now = time.monotonic()

        for s in ready:
            c = by_sock.get(s)
            if c is None:
                continue
            for _ in range(MAX_READS_PER_POLL):
                try:
                    data = s.recv(RECV_BYTES)
                except (BlockingIOError, ssl.SSLWantReadError):
                    break
                except OSError:
                    data = b""
                    c.dead = True
                if not data:
                    # A peer-closed socket stays PERMANENTLY readable and recv returns
                    # b"" instantly, so leaving it in the poll set spins the loop at
                    # ~290,000 iterations/second -- burning a core inside the process
                    # whose whole job is resolving millisecond differences between the
                    # connections that are still alive.
                    if not c.buf:
                        c.dead = True
                    break
                c.buf.extend(data)
                if not getattr(s, "pending", lambda: 0)():
                    break

            # ⛔ ONE CLOCK FOR THE WHOLE BATCH -- see where `now` is taken above.
            # Sampled per connection inside this loop, select's input order (fastest
            # handshake first) charged everything the earlier connections cost to the
            # later ones as lag, deciding every near-tie in favour of the very
            # hypothesis the tool exists to test.
            try:
                for op, payload in iter_frames(c):
                    if op == 0x9:                                  # PING -> PONG, keep it alive
                        # RFC 6455 §5.5.3: a Pong MUST carry the Ping's payload. We
                        # sent an empty one; a server that enforces this closes the
                        # connection, which then busy-spins the poll loop.
                        try:
                            mask = os.urandom(4)
                            body = bytes(x ^ mask[i % 4] for i, x in enumerate(payload))
                            s.sendall(bytes([0x8A, 0x80 | len(payload)]) + mask + body)
                        except OSError:
                            pass
                        continue
                    if op not in (0x1, 0x2):
                        continue
                    for m in self.seq_re.finditer(payload):
                        processed += self._score(c, int(m.group(1)), now)
            except ConnectionError as exc:
                self.log(f"  {c.sport}: {exc}")
                c.dead = True
            except (ValueError, IndexError) as exc:
                # A --seq-field whose group is missing or non-numeric used to escape
                # poll() and traceback out of main() mid-run.
                self.log(f"  {c.sport}: --seq-field did not yield a number ({exc})")
                c.dead = True
        return processed

    def _score(self, c: Conn, seq: int, now: float) -> int:
        c.last_seq = seq

        # A REPLAYED BACKLOG IS NOT A RACE.
        if seq <= c.admit_above:
            c.backlog_skipped += 1
            return 0

        # A CONNECTION STILL CATCHING UP IS NOT RACING. Until it delivers a
        # sequence at or above what the pool already knew, its samples are its own
        # warm-up, and charging them reads as "aged connections are far better".
        if not c.caught_up:
            if seq >= self.high_water - CATCHUP_TOLERANCE:
                c.caught_up = True
            else:
                c.catchup_skipped += 1
                return 0
        elif seq < self.high_water - CATCHUP_TOLERANCE:
            # It caught up and fell behind again. That is failing to keep up, not
            # losing a lottery; do not charge it as latency.
            # ⛔ IT IS STILL A RACE IT LOST. Returning without counting it deleted a
            # connection's WORST behaviour from its own denominator while keeping
            # every win, so won% -- the column this tool tells you to act on --
            # inverted: a connection that froze repeatedly reported 96.6% when it had
            # actually delivered 43.6% of messages first. No LAG sample (the delay is
            # its own backlog, not the network), but the race counts.
            c.relapses += 1
            c.caught_up = False
            c.catchup_skipped += 1
            c.races += 1
            return 0

        first = self.seen.get(seq)
        if first is None:
            self.seen[seq] = now
            self.high_water = max(self.high_water, seq)
            c.firsts += 1
            c.races += 1
            # ⛔ NO 0.0 SAMPLE HERE. A win is a count, not a lag of zero.
            if len(self.seen) > SEEN_WINDOW:
                for k in sorted(self.seen)[:len(self.seen) - SEEN_WINDOW]:
                    del self.seen[k]
            return 1

        c.races += 1
        c.lags_ms.append((now - first) * 1000.0)
        if len(c.lags_ms) > LAG_WINDOW:
            del c.lags_ms[:len(c.lags_ms) - LAG_WINDOW]
        return 1

    def report(self) -> str:
        rows = []
        rows.append(f"{'sport':>6} {'hs_ms':>7} {'path':>5} {'origin':<14} {'colo':>5} "
                    f"{'races':>7} {'won':>6} {'won%':>6} {'median_lag_ms':>14} "
                    f"{'relapse':>8} {'skipped':>8}")
        # ⛔ RANK ON win%, NOT on median lag. A connection is scored only on the
        # races it LOSES, so the leader's lag samples come from the small minority
        # of messages it did not get first -- and it can therefore show the WORST
        # median lag in the table while being the best connection you have.
        # Observed: the connection winning 73% of races ranked mid-table by lag.
        # Sorting by lag puts your best connection in the middle.
        # Under-sampled connections sort LAST and are never recommended.
        ranked = sorted(self.conns,
                        key=lambda x: (x.races < MIN_RACE_SAMPLES,
                                       -(x.firsts / x.races) if x.races else 0.0,
                                       x.median_lag_ms if x.median_lag_ms is not None else 1e9))
        for c in ranked:
            ml = c.median_lag_ms
            thin = c.races < MIN_RACE_SAMPLES
            rows.append(
                f"{c.sport:>6} {c.handshake_ms:>7.3f} {'FAST' if c.fast_path else 'slow':>5} "
                f"{origin_label(c.origin):<14} {c.colo:>5} {c.races:>7} {c.firsts:>6} "
                f"{(100.0 * c.firsts / c.races if c.races else 0.0):>5.1f}% "
                f"{('n/a' if ml is None else f'{ml:.3f}'):>14} {c.relapses:>8} "
                f"{c.backlog_skipped + c.catchup_skipped:>8}"
                f"{'   <- too few races to rank' if thin else ''}")
        rows.append("")
        eligible = [c for c in ranked if c.races >= MIN_RACE_SAMPLES]
        if len(eligible) >= 2:
            b = eligible[0]
            rows.append(f"  >> KEEP: sport {b.sport} (origin {origin_label(b.origin)}, "
                        f"{100.0 * b.firsts / b.races:.1f}% of messages first)")
        elif len(eligible) == 1:
            rows.append(f"  >> NO RECOMMENDATION: only 1 connection reached "
                        f"{MIN_RACE_SAMPLES} races, so there was no contest to win. "
                        f"Run longer, or with more connections.")
        else:
            rows.append(f"  >> NO RECOMMENDATION: no connection reached {MIN_RACE_SAMPLES} "
                        f"races yet. Run longer (--seconds).")
        rows.append("")
        rows.append("  skipped = messages not scored: its own replay backlog, plus any")
        rows.append("            stretch where it had fallen behind. A LARGE NUMBER HERE MEANS")
        rows.append("            won% IS COMPUTED OVER LESS OF THE FEED THAN YOU THINK.")
        rows.append("  RANKED BY won% -- the fraction of messages this connection delivered FIRST.")
        rows.append("  That is the number to act on.")
        rows.append("  median_lag_ms is how far behind it was on the races it LOST, and it is a")
        rows.append("  DIAGNOSTIC, not a ranking: the leader loses rarely, so its few samples can")
        rows.append("  be the worst in the table. Do not sort by it.")
        rows.append("  relapse = times it caught up and then fell behind again. A connection with")
        rows.append("  relapses is failing to keep up, not losing a lottery -- consider redialling it.")
        rows.append(f"  A connection with fewer than {MIN_RACE_SAMPLES} races is not ranked: with too")
        rows.append("  few samples, whoever happened to score reports 100% and means nothing.")
        rows.append("")
        rows.append(f"  origins: {origin_legend()}")
        return "\n".join(rows)

    def snapshot(self, note: str = "") -> dict:
        """One JSON row describing the pool right now."""
        return {
            "utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "note": note,
            "conns": [{"sport": c.sport, "handshake_ms": c.handshake_ms,
                       "fast_path": c.fast_path, "origin": c.origin,
                       "origin_label": origin_label(c.origin), "colo": c.colo,
                       "races": c.races, "firsts": c.firsts,
                       "won_pct": (100.0 * c.firsts / c.races) if c.races else None,
                       "median_lag_ms": c.median_lag_ms,
                       "relapses": c.relapses} for c in self.conns],
        }

    def close(self) -> None:
        for c in self.conns:
            _hard_close(c.sock)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def write_row(path: str | None, row: dict, counter: list[int]) -> None:
    """Append one NDJSON row and count it.

    ⛔ Called from EVERY report AND from the final block. An earlier version wrote
    only inside the interval branch, so `race` mode, any run shorter than
    --every, and any Ctrl-C all produced NO FILE AT ALL and said nothing about it.
    """
    if not path:
        return
    with open(path, "a") as fh:
        fh.write(json.dumps(row) + "\n")
    counter[0] += 1


def resolve(host: str, want_v6: bool) -> str:
    fam = socket.AF_INET6 if want_v6 else socket.AF_INET
    infos = socket.getaddrinfo(host, 443, fam, socket.SOCK_STREAM)
    return infos[0][4][0]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("mode", choices=["race", "watch"])
    ap.add_argument("--host", default=DEFAULT_HOST)
    ap.add_argument("--ip", help="pin a specific edge address (default: resolve --host)")
    ap.add_argument("--v6", action="store_true", help="use IPv6")
    ap.add_argument("--path", default="/")
    ap.add_argument("--keep", type=int, default=4)
    ap.add_argument("--probe", type=int, default=12)
    ap.add_argument("--sport-base", type=int, default=61000)
    ap.add_argument("--src-ip", default=None,
                    help="bind this LOCAL source address. The concurrency cap is per "
                         "source address, so this is how you hold more than one "
                         "address's worth: run one instance per address.")
    ap.add_argument("--seconds", type=float, default=300.0, help="watch mode duration")
    ap.add_argument("--every", type=float, default=30.0, help="watch mode report interval")
    ap.add_argument("--seq-field", default=None,
                    help='regex with one capture group for the message id, e.g. '
                         '\'"seq":\\s*(\\d+)\'')
    ap.add_argument("--json", metavar="FILE",
                    help="append NDJSON here: the cohort in race mode, plus one row per "
                         "report and a final row in watch mode. Path is relative to your "
                         "CURRENT directory unless absolute.")
    a = ap.parse_args()

    ip = a.ip or resolve(a.host, a.v6)
    rows = [0]
    json_path = os.path.abspath(a.json) if a.json else None
    print(f"host={a.host} ip={ip} keep={a.keep} probe={a.probe}")
    if json_path:
        print(f"json  -> {json_path}")
    print("racing:")
    conns = race_connections(a.host, ip, a.keep, a.probe, src_ip=a.src_ip,
                             sport_base=a.sport_base, path=a.path)
    if not conns:
        print("no connections survived the race", file=sys.stderr)
        return 1

    print("\nkept:")
    for c in conns:
        print(f"  {c}")

    if a.mode == "race":
        write_row(json_path, {"utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                              "note": "race", "host": a.host, "ip": ip,
                              "conns": [{"sport": c.sport, "handshake_ms": c.handshake_ms,
                                         "fast_path": c.fast_path, "origin": c.origin,
                                         "origin_label": origin_label(c.origin),
                                         "colo": c.colo} for c in conns]}, rows)
        for c in conns:
            _hard_close(c.sock)
        if json_path:
            print(f"\nwrote {rows[0]} row(s) to {json_path}")
        print("\nRace only -- connections closed. Use `watch` to hold them and score them.")
        return 0

    seq_re = a.seq_field.encode() if a.seq_field else DEFAULT_SEQ_RE
    pool = Pool(conns, seq_re=seq_re)

    # Admit every connection at the pool's high-water mark AFTER a short settle, so
    # the opening replay is never scored. Without this the newest connection
    # "wins" every message by construction.
    print("\nsettling (discarding the replay backlog)...")
    settle_end = time.monotonic() + 12.0
    while time.monotonic() < settle_end:
        pool.poll(0.25)
    for c in pool.conns:
        pool._admit(c)
        c.caught_up = True
        c.lags_ms.clear(); c.firsts = 0; c.races = 0
    print(f"  admitted above sequence {pool.high_water}\n")

    end = time.monotonic() + a.seconds
    nxt = time.monotonic() + a.every
    try:
        while time.monotonic() < end:
            pool.poll(0.25)
            if time.monotonic() >= nxt:
                print(f"--- {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())} ---")
                print(pool.report())
                print()
                write_row(json_path, pool.snapshot("interval"), rows)
                nxt = time.monotonic() + a.every
    except KeyboardInterrupt:
        pass
    finally:
        print("final:")
        print(pool.report())
        # ALWAYS write a final row -- this is the one that makes a short run or a
        # Ctrl-C still produce a usable file.
        write_row(json_path, pool.snapshot("final"), rows)
        if json_path:
            print(f"\nwrote {rows[0]} row(s) to {json_path}")
        pool.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
