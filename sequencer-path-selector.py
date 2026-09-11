#!/usr/bin/env python3
"""
sequencer-path-selector.py -- is there a lottery to win on the SUBMIT leg?

WHY THIS EXISTS, AND WHY IT IS NOT sequencer-feed-monitor.py
===========================================================
`sequencer-feed-monitor.py` plays three lotteries that exist because the feed
sits behind Cloudflare: the return path, the `__cflb` origin, and the anycast
colo. NONE of those apply to `sequencer.mainnet.chain.robinhood.com`, which is
plain EC2 in us-east-2 with no CDN in front of it. So the question has to be
asked again from scratch for the submit leg, and the answer may well be "there is
no lottery here" -- which is a result, not a failure.

WHAT A CLIENT ACTUALLY CONTROLS ON THE SUBMIT LEG
-------------------------------------------------
  1. WHICH DESTINATION IP. The hostname publishes three A records and a client
     that just resolves the name draws one at random per connection.
  2. THE SOURCE PORT. ECMP inside a carrier or inside AWS hashes the 4-tuple, so
     the source port can select a path. This is the dimension that has never been
     tested against the sequencer -- it is the whole reason this tool exists.
  3. The source address, if the host has several. Reported, not swept, because a
     VM usually has one.
  4. Nothing else. There is no AAAA record, so IPv6 is not on the menu.

THE TWO THINGS THAT MAKE THIS HONEST
------------------------------------
  * INTERLEAVING. Every (dst, sport) pair is sampled round-robin. Measuring one
    pair to completion and then the next conflates "which pair" with "when", and
    on this project that mistake has produced confident wrong answers more than
    once.
  * A DETERMINISM RE-TEST. A lottery is only worth playing if the draw STICKS.
    If a "fast" port is not still fast a minute later, there is nothing to
    select and the spread was just noise wearing a costume. This tool re-tests
    its own winners and losers and will tell you they did not hold.

It refuses to name a winner unless the between-pair spread is larger than the
within-pair spread.
"""
from __future__ import annotations

import argparse
import json
import socket
import statistics as st
import struct
import sys
import time

LINGER_RST = struct.pack("ii", 1, 0)
SEQ_HOST = "sequencer.mainnet.chain.robinhood.com"

# ⛔ DO NOT hardcode an address list here. The endpoint's A records are not a
# contract: they can be redeployed at any time, and a stale list silently measures
# the wrong thing while looking like it worked. Resolve at run time, every run.
# (An earlier version of this file shipped the three addresses observed on
# 2026-09-09 as its default, which contradicted the advice published beside it.)
def resolve_default(host: str = SEQ_HOST) -> str:
    """Every A/AAAA record the endpoint publishes right now, comma-separated."""
    import socket
    out: list[str] = []
    try:
        for _, _, _, _, sa in socket.getaddrinfo(host, 443, 0, socket.SOCK_STREAM):
            if sa[0] not in out:
                out.append(sa[0])
    except socket.gaierror as exc:
        print(f"# could not resolve {host}: {exc}", file=sys.stderr)
    return ",".join(out)


def sample(dst: str, port: int, sport: int | None, src_ip: str | None,
           timeout: float) -> float | None:
    """One connect(). Milliseconds, or None on failure.

    SO_LINGER(0) makes close() emit RST rather than FIN: thousands of samples
    would otherwise pile into TIME_WAIT and later binds would fail, and those
    failures are a SELECTION BIAS (the sample skews toward whichever ports
    happened to be free), not merely lost data.
    """
    # Family follows the destination literal. The sequencer publishes no AAAA, but
    # this same sweep is the right instrument for the feed, which does -- so the
    # family must not be assumed.
    fam = socket.AF_INET6 if ":" in dst else socket.AF_INET
    s = socket.socket(fam, socket.SOCK_STREAM)
    try:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, LINGER_RST)
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        if sport is not None or src_ip is not None:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            # An unspecified v6 bind address is "::", not "" -- binding "" on an
            # AF_INET6 socket raises, which would have silently dropped every
            # pinned-port sample on IPv6 and left the sweep looking empty.
            any_addr = "::" if fam == socket.AF_INET6 else ""
            s.bind((src_ip or any_addr, sport or 0))
        s.settimeout(timeout)
        t0 = time.perf_counter()
        s.connect((dst, port))
        return (time.perf_counter() - t0) * 1000.0
    except (OSError, socket.timeout):
        return None
    finally:
        s.close()


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


def q(v, p):                      # kept as a short alias; same convention
    return pctl(v, p)


def sweep(pairs, port, reps, interval, timeout, src_ip):
    """Round-robin over every (dst, sport) pair, `reps` times."""
    got = {p: [] for p in pairs}
    for _ in range(reps):
        for pr in pairs:
            ms = sample(pr[0], port, pr[1], src_ip, timeout)
            if ms is not None:
                got[pr].append(ms)
            time.sleep(interval)
    return got


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--targets", default=None,
                    help="comma-separated addresses; default: resolve "
                         f"{SEQ_HOST} live")
    ap.add_argument("--port", type=int, default=443)
    ap.add_argument("--ports", type=int, default=16, help="source ports per target")
    ap.add_argument("--sport-base", type=int, default=24000)
    ap.add_argument("--reps", type=int, default=25, help="samples per (dst,sport) pair")
    ap.add_argument("--interval", type=float, default=0.01)
    ap.add_argument("--timeout", type=float, default=3.0)
    ap.add_argument("--recheck-after", type=float, default=45.0,
                    help="seconds to wait before the determinism re-test")
    ap.add_argument("--src-ip", default=None)
    ap.add_argument("--json", default="")
    a = ap.parse_args()
    if not a.targets:
        a.targets = resolve_default()
        if not a.targets:
            ap.error('could not resolve the endpoint and no --targets given')
        print(f'# resolved {SEQ_HOST} -> {a.targets}')

    dsts = [t.strip() for t in a.targets.split(",") if t.strip()]
    if a.sport_base < 1024 or a.sport_base + a.ports - 1 > 65535:
        ap.error("source port range outside 1024-65535")
    # These reach socket calls as OverflowError / ValueError, which are NOT OSError,
    # so they escaped sample()'s except clause and tracebacked out of a live run.
    if not 1 <= a.port <= 65535:
        ap.error("--port must be 1-65535")
    if a.timeout <= 0:
        ap.error("--timeout must be positive")
    if a.interval < 0:
        ap.error("--interval must be >= 0")
    if a.ports < 2:
        ap.error("--ports must be at least 2: a source-port effect needs two ports to compare")
    if a.reps < 2:
        ap.error("--reps must be at least 2: a spread cannot be separated from noise on one sample")
    if a.recheck_after < 5:
        ap.error("--recheck-after must be >= 5s: re-testing inside the same network moment "
                 "makes the ordering reproduce for the trivial reason")
    # ⛔ Linux keeps this in /proc; macOS and the BSDs do NOT, and swallowing the
    # FileNotFoundError made this warning a dead branch on the platform a lot of
    # people will run it from — silently landing --sport-base in the ephemeral range.
    lo = hi = None
    try:
        lo, hi = (int(x) for x in
                  open("/proc/sys/net/ipv4/ip_local_port_range").read().split())
    except (OSError, ValueError):
        try:
            import subprocess
            lo = int(subprocess.run(["sysctl", "-n", "net.inet.ip.portrange.hifirst"],
                                    capture_output=True, text=True, timeout=5).stdout.strip())
            hi = int(subprocess.run(["sysctl", "-n", "net.inet.ip.portrange.hilast"],
                                    capture_output=True, text=True, timeout=5).stdout.strip())
        except Exception:                                          # noqa: BLE001
            print("# NOTE: could not read this system's ephemeral port range; make sure "
                  f"--sport-base {a.sport_base} does not overlap it", file=sys.stderr)
    if lo is not None and hi is not None:
        if lo <= a.sport_base + a.ports - 1 and a.sport_base <= hi:
            print(f"# WARNING: source ports overlap the ephemeral range {lo}-{hi}; "
                  f"expect bind failures and a biased sample", file=sys.stderr)

    pairs = [(d, a.sport_base + i) for d in dsts for i in range(a.ports)]
    print(f"# sequencer lottery: {len(dsts)} destinations x {a.ports} source ports "
          f"= {len(pairs)} pairs, {a.reps} samples each, interleaved")

    for d in dsts:                                   # warm route cache; discarded
        for _ in range(3):
            sample(d, a.port, None, a.src_ip, a.timeout)

    got = sweep(pairs, a.port, a.reps, a.interval, a.timeout, a.src_ip)
    med = {p: q(v, .5) for p, v in got.items() if v}
    if not med:
        print("ALL SAMPLES FAILED"); return 1

    # ---- dimension 1: destination IP -------------------------------------
    print(f"\n{'destination':<18}{'n':>7}{'min':>9}{'p50':>9}{'p90':>9}{'sd':>8}")
    per_dst, dead = {}, []
    for d in dsts:
        v = [x for p, xs in got.items() if p[0] == d for x in xs]
        if not v:
            # ⛔ One drained or ACL-blocked address is EXACTLY what this tool exists
            # to find, and min() on its empty list used to traceback instead.
            dead.append(d)
            print(f"{d:<18}{0:>7}{'—':>9}{'—':>9}{'—':>9}{'—':>8}   NO SAMPLES")
            continue
        per_dst[d] = v
        print(f"{d:<18}{len(v):>7}{min(v):>9.3f}{q(v,.5):>9.3f}{q(v,.9):>9.3f}{st.pstdev(v):>8.3f}")
    if dead:
        print(f"  ⛔ {len(dead)} of {len(dsts)} destinations answered nothing: {', '.join(dead)}")
    if not per_dst:
        print("ALL SAMPLES FAILED"); return 1
    dsts = [d for d in dsts if d in per_dst]
    best_d = min(per_dst, key=lambda d: q(per_dst[d], .5))
    worst_d = max(per_dst, key=lambda d: q(per_dst[d], .5))
    dst_gain = q(per_dst[worst_d], .5) - q(per_dst[best_d], .5)
    # The destination dimension needs a noise yardstick too, or its recommendation
    # is gated on a bare magic number while the port dimension is gated on a ratio.
    dst_noise = st.fmean([st.pstdev(v) for v in per_dst.values() if len(v) > 1]) \
        if any(len(v) > 1 for v in per_dst.values()) else 0.0
    print(f"  -> destination choice is worth {dst_gain:.3f} ms at p50 "
          f"({best_d} best, {worst_d} worst)")

    # ---- dimension 2: source port ----------------------------------------
    print(f"\n{'source-port effect, within each destination':<52}")
    port_gain = {}
    for d in dsts:
        # ⛔ len(v) > 1, NOT just `v`. A pair with a single surviving sample used to
        # set the spread (numerator) while contributing nothing to the within-pair sd
        # (denominator) — one starved pair could manufacture a ratio of 1184.
        ps = {p[1]: q(v, .5) for p, v in got.items() if p[0] == d and len(v) > 1}
        sds = [st.pstdev(v) for p, v in got.items() if p[0] == d and len(v) > 1]
        if len(ps) < 2 or not sds:
            print(f"  {d:<16} fewer than two source ports produced usable samples — "
                  f"no port effect can be measured here")
            port_gain[d] = (0.0, 0.0, ps)
            continue
        spread = max(ps.values()) - min(ps.values())
        within = st.fmean(sds)
        port_gain[d] = (spread, within, ps)
        fast = sorted(ps, key=ps.get)[:3]; slow = sorted(ps, key=ps.get)[-3:]
        print(f"  {d:<16} port-median spread {spread:6.3f} ms | within-pair sd {within:6.3f} "
              f"| ratio {spread/within if within else float('nan'):.2f}")
        print(f"      fastest ports {fast} -> {[round(ps[x],3) for x in fast]}")
        print(f"      slowest ports {slow} -> {[round(ps[x],3) for x in slow]}")

    # ---- determinism: does a fast port STAY fast? ------------------------
    print(f"\n# determinism re-test in {a.recheck_after:.0f}s "
          f"(a lottery you cannot re-draw is not selectable)")
    time.sleep(a.recheck_after)
    checked = []
    for d in dsts:
        ps = port_gain[d][2]
        if len(ps) < 2:
            continue
        order = sorted(ps, key=ps.get)
        # ⛔ Only the fastest and slowest are ever read. order[1] and order[-2] used
        # to be sampled too — 2 x reps extra connects per destination, for data that
        # reached nothing but the --json blob — and on --ports 1/2/3 they either
        # raised IndexError or silently duplicated a pair, which double-sampled it
        # and broke the interleaving this file calls its core principle.
        checked += [(d, order[0]), (d, order[-1])]
    # The destination ordering was never re-tested either, yet it carried a
    # recommendation. Re-draw best vs worst on EPHEMERAL ports, so this measures the
    # destination and not a port.
    if len(per_dst) > 1:
        checked += [(best_d, None), (worst_d, None)]
    checked = list(dict.fromkeys(checked))
    got2 = sweep(checked, a.port, a.reps, a.interval, a.timeout, a.src_ip)
    print(f"{'pair':<26}{'run1 p50':>10}{'run2 p50':>10}{'delta':>9}   rank held?")
    held = tot = 0
    verified = {}                      # destination -> did its port ordering hold?
    for d in dsts:
        ps = port_gain[d][2]
        if len(ps) < 2:
            continue
        order = sorted(ps, key=ps.get)
        fastp, slowp = order[0], order[-1]
        f1, s1 = ps[fastp], ps[slowp]
        f2 = q(got2[(d, fastp)], .5) if got2.get((d, fastp)) else None
        s2 = q(got2[(d, slowp)], .5) if got2.get((d, slowp)) else None
        if f2 is None or s2 is None:
            continue
        tot += 1
        ok = f2 < s2
        held += ok
        verified[d] = ok
        print(f"{d+' :'+str(fastp)+' (fast)':<26}{f1:>10.3f}{f2:>10.3f}{f2-f1:>+9.3f}")
        print(f"{d+' :'+str(slowp)+' (slow)':<26}{s1:>10.3f}{s2:>10.3f}{s2-s1:>+9.3f}   "
              f"{'YES' if ok else 'NO — order flipped'}")

    # ---- did the DESTINATION ordering reproduce? -------------------------
    dst_ok = None
    if len(per_dst) > 1:
        b2 = q(got2[(best_d, None)], .5) if got2.get((best_d, None)) else None
        w2 = q(got2[(worst_d, None)], .5) if got2.get((worst_d, None)) else None
        if b2 is not None and w2 is not None:
            dst_ok = b2 < w2
            print(f"{best_d+' (best dst)':<26}{q(per_dst[best_d],.5):>10.3f}{b2:>10.3f}"
                  f"{b2-q(per_dst[best_d],.5):>+9.3f}")
            print(f"{worst_d+' (worst dst)':<26}{q(per_dst[worst_d],.5):>10.3f}{w2:>10.3f}"
                  f"{w2-q(per_dst[worst_d],.5):>+9.3f}   "
                  f"{'YES' if dst_ok else 'NO — order flipped'}")

    # ---- verdict ---------------------------------------------------------
    print("\n" + "=" * 72)
    # TWO INDEPENDENT GATES, both must pass before anything is called worth playing.
    #
    #   1. EFFECT SIZE. spread > sd is far too weak: at ratio 1.09 the "effect" is
    #      noise, and an earlier version of this tool duly declared a lottery on it.
    #      Checked against every out-of-sample result we have, a bar of 2.0 keeps
    #      each pick that DELIVERED (3.18 -> +0.646 ms) and rejects each that failed
    #      (1.51 -> -0.495 ms, 1.76 -> nothing, 1.09 -> noise).
    #   2. DETERMINISM, below: did the ordering reproduce?
    #
    # They are independent, and on our data they agree. Requiring both is cheap
    # insurance against a sweep that got lucky in one dimension.
    MIN_EFFECT_RATIO = 2.0
    MIN_EFFECT_MS = 0.05     # below this, the effect is under the timer's own resolution
    worth, rejected = [], []
    # ⛔ WHAT THIS TOOL ENDORSES, recorded as data rather than left for a consumer to
    # re-derive. A downstream `min()` over every pair swept is NOT this tool's
    # recommendation: it ignores both gates and it mixes the destination dimension
    # into what reads as a source-port pick. That mistake is already documented in
    # this repo for phaseC-confirm.sh, and analyse.py was making it too.
    dst_endorsed = False
    endorsed_ports: dict[str, int] = {}

    # DESTINATION. It used to be recommended on `dst_gain > 0.05` alone — a bare
    # magic number, no noise comparison, and an ordering nothing ever re-drew. Both
    # gates now apply to it, exactly as they do to source ports.
    dst_ratio = (dst_gain / dst_noise) if dst_noise else 0.0
    if len(per_dst) > 1:
        if dst_gain < MIN_EFFECT_MS:
            rejected.append(f"destination choice is only {dst_gain:.3f} ms — below the "
                            f"{MIN_EFFECT_MS} ms floor")
        elif dst_ratio < MIN_EFFECT_RATIO:
            rejected.append(f"destination choice {dst_gain:.3f} ms is only {dst_ratio:.1f}x "
                            f"the {dst_noise:.3f} ms spread inside each destination")
        elif dst_ok is None:
            rejected.append("destination ordering could not be re-tested")
        elif not dst_ok:
            rejected.append("destination ordering did NOT reproduce")
        else:
            dst_endorsed = True
            worth.append(f"pinning destination {best_d} (worth {dst_gain:.3f} ms at p50, "
                         f"{dst_ratio:.1f}x noise, ordering reproduced)")

    # SOURCE PORTS, per destination, and ONLY for destinations that were verified.
    # A destination skipped from the re-test used to keep its recommendation while
    # the printed held/tot ratio read as unanimous.
    for d, (spread, within, ps) in port_gain.items():
        if len(ps) < 2:
            continue
        ratio = (spread / within) if within else 0.0
        if spread < MIN_EFFECT_MS:
            continue
        if ratio < MIN_EFFECT_RATIO:
            if within and spread > within:
                rejected.append(f"{d} port spread {spread:.3f} is only {ratio:.1f}x its sd "
                                f"{within:.3f} — below the {MIN_EFFECT_RATIO}x bar")
            continue
        if d not in verified:
            rejected.append(f"{d} showed a {ratio:.1f}x port effect that could NOT be "
                            f"re-tested — no verdict on it")
        elif not verified[d]:
            rejected.append(f"{d} showed a {ratio:.1f}x port effect whose ordering did "
                            f"NOT reproduce")
        else:
            endorsed_ports[d] = min(ps, key=ps.get)
            worth.append(f"source-port selection on {d} "
                         f"(spread {spread:.3f} = {ratio:.1f}x the sd of {within:.3f})")

    # The single (dst, sport) this tool recommends, if any. Chosen only from the
    # destinations whose port effect cleared BOTH gates; a bare port number means
    # "any ephemeral port" and is only offered when the destination dimension alone
    # was endorsed.
    selected = None
    if endorsed_ports:
        d_sel = min(endorsed_ports, key=lambda d: q(got[(d, endorsed_ports[d])], .5))
        selected = [d_sel, endorsed_ports[d_sel]]
    elif dst_endorsed:
        selected = [best_d, None]

    for r in rejected:
        print(f"  rejected: {r}")
    print(f"determinism: fast/slow order held on {held}/{tot} destination(s)"
          + ("" if dst_ok is None else f"; destination ordering {'held' if dst_ok else 'FLIPPED'}"))

    # ⛔ tot == 0 USED TO PASS. `held < tot` is `0 < 0` -> False, so a run where every
    # single re-test came back empty fell through to "worth playing" — the gate
    # inverted precisely when the evidence was weakest.
    # ⭐ ONE verdict variable drives BOTH the printed text and the JSON artifact.
    # They used to be separate: the console said "NO LOTTERY WORTH PLAYING" while
    # the JSON carried only held/total — and held == total reads as a clean pass to
    # anything downstream. analyse.py duly published a lottery gain, with a tidy
    # "2/2", on a leg this tool had just rejected on both gates.
    if tot == 0 and dst_ok is None:
        verdict = "cannot_verify"
        selected = None
        print("VERDICT: CANNOT VERIFY — nothing was re-tested successfully, so no claim\n"
              "         here has been checked against fresh samples. Re-run.")
    elif not worth:
        verdict = "no_lottery"
        selected = None
        print("VERDICT: NO LOTTERY WORTH PLAYING on this leg — nothing cleared both the\n"
              "         effect-size bar and the re-test.")
    else:
        verdict = "worth_playing"
        print("VERDICT: worth playing —\n         " + "\n         ".join(worth))

    if a.json:
        json.dump({"pairs": {f"{d}|{p}": v for (d, p), v in got.items()},
                   "recheck": {f"{d}|{p}": v for (d, p), v in got2.items()},
                   "dst_gain_ms": dst_gain, "best_dst": best_d,
                   "held": held, "total": tot,
                   # Everything the verdict was made of, so no consumer ever has to
                   # re-implement a gate. A second implementation of a gate is a
                   # second chance for it to disagree with this one.
                   "schema": 2,
                   "verdict": verdict,
                   "selected": selected,
                   "worth": worth,
                   "rejected": rejected,
                   "dst_order_held": dst_ok,
                   "dst_noise_ms": dst_noise,
                   "dst_ratio": dst_ratio,
                   "port_order_held": verified,
                   }, open(a.json, "w"))
        print(f"wrote {a.json}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
