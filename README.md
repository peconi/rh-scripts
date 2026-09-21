# rh-scripts

Two small, dependency-free tools for measuring and improving how a client reaches
**Robinhood Chain**'s public endpoints.

They exist because, on both legs of this chain, *which connection you happen to get*
turns out to matter more than anything you can buy — and both legs are measurable from
the client side, with nothing but the Python standard library.

| script | leg | one-line purpose |
|---|---|---|
| [`sequencer-feed-monitor.py`](sequencer-feed-monitor.py) | **read** — `feed.mainnet.chain.robinhood.com` | open more WebSocket connections than you need, keep the fast ones, and keep scoring them |
| [`sequencer-path-selector.py`](sequencer-path-selector.py) | **submit** — `sequencer.mainnet.chain.robinhood.com` | find the fastest (destination, source port) pair — and refuse to recommend one that does not reproduce |
| [`rh-feed-client.py`](rh-feed-client.py) | **read** — any feed endpoint | consume a sequencer feed over ONE connection, or race several sources and take whichever delivers first |

**Requirements:** Python 3.9+ (tested on 3.9 and 3.12). No third-party packages, no build step, no configuration file.
Both scripts resolve the endpoint live on every run, because the published address set is
not a contract and a hardcoded list silently measures the wrong thing.

**One percentile convention, in both tools.** Each script carries a `pctl()` helper that is
byte-for-byte identical to the other's: sort ascending, take the observed value at index
`floor(p × (n−1))`, **interpolate nothing**. That last part is the one that matters — the
textbook median averages the two middle values on an even sample count and so reports a
latency that no connection ever produced. Every number these tools print is a measurement
that actually happened.

All output below is from real runs of the code in this repository. Nothing is illustrative.

---

## 1. `sequencer-feed-monitor.py` — the read leg

### TL;DR

The feed sits behind Cloudflare. **Two independent lotteries are drawn when you connect
and then fixed for the life of that connection:**

1. **The origin.** Cloudflare Load Balancing pins each connection to one origin instance,
   named in the `__cflb` cookie on the upgrade response. The arrival-lag spread between the
   best and worst origin is large — and it is free to avoid, because the cookie tells you
   which one you drew.
2. **The colo.** Anycast decides which Cloudflare edge serves you. From a fixed location you
   do not control this, so the script only records it.

The strategy is therefore: **open more connections than you need, measure both, keep
the winners, and keep scoring the survivors** — because a connection that leads at 09:00 can
fall behind by 11:00, and you want to know when it does.

### Usage

```bash
# open 4 candidates, keep the best 2, print what you drew, then exit
python3 sequencer-feed-monitor.py race --keep 2 --probe 4

# same, but hold them open and print a live scoreboard
python3 sequencer-feed-monitor.py watch --keep 2 --probe 4 --seconds 90 --every 30

# a different feed, with your own sequence field for the race scoring
python3 sequencer-feed-monitor.py race --host feed.example.com --seq-field '"seq":\s*(\d+)'
```

### Example — `race`

```
$ python3 sequencer-feed-monitor.py race --keep 2 --probe 6
host=feed.mainnet.chain.robinhood.com ip=172.66.147.70 keep=2 probe=6
racing:
  draw 0: sport=61000 hs= 0.807ms origin=A colo=CMH
  draw 1: sport=61001 hs= 0.881ms origin=A colo=CMH
  draw 2: at cap — this address holds 2 concurrent websocket(s)
  draw 2: sport=61003 hs= 0.815ms origin=A colo=CMH
  draw 3: sport=61004 hs= 0.764ms origin=A colo=CMH
  draw 4: sport=61005 hs= 0.580ms origin=A colo=CMH
  draw 5: sport=61006 hs= 0.674ms origin=A colo=CMH
  (1 draw(s) refused at the cap; that is expected and not an error)
  keeping 2 of 2: 1 distinct origin(s) [A, A]

kept:
  sport=61005 hs= 0.580ms origin=A colo=CMH
  sport=61006 hs= 0.674ms origin=A colo=CMH
```

Note `draw 2`. **The feed caps how many WebSocket connections one source address may hold at
once** — see [Concurrency](#a-note-on-concurrency) below. The script discovers that cap from
the first refusal rather than assuming a number, then drops its worst-handshake connection to
free a slot before drawing again. A draw refused at the cap does not consume one of your
`--probe` draws; other failures do.

To hold more than one address's worth, run one instance per source address with `--src-ip`.

### Example — `watch`

```
$ python3 sequencer-feed-monitor.py watch --keep 2 --probe 6 --seconds 45 --every 45
  [ ... race output as above ... ]

settling (discarding the replay backlog)...
  admitted above sequence 67331739

--- 2026-09-19T19:27:21Z ---
 sport   hs_ms origin          colo   races    won   won%  median_lag_ms  relapse  skipped
 61016   0.603 B                CMH     447    258  57.7%         15.272        0        2
 61005   0.721 A                CMH     448    190  42.4%         17.611        1        4

  >> KEEP: sport 61016 (origin B, 57.7% of messages first)

  [ ... the run then prints a legend for each column; omitted here ... ]

  origins: A=0H28vuXLJ6LQPaB5R1EAZ6ww  B=02DiuJ4sK723JTf1BpD2qK81
```

**Read the `won%` column and nothing else.** It is the fraction of messages this connection
delivered *first*, and it is the number to act on.

- `median_lag_ms` is how far behind a connection was **on the races it lost**. It is a
  diagnostic, not a ranking — the leader loses rarely, so its handful of losing samples can
  easily be the worst in the table. **Do not sort by it.**
- `relapse` counts the times a connection caught up and then fell behind again. Relapses mean
  it is failing to keep up rather than losing a lottery; consider redialling it.
- ⭐ `skipped` is the count of messages **not scored** for that connection — its own replay
  backlog, plus any stretch where it had fallen behind. **Read it as the denominator's
  honesty.** In the run above, `61006` skipped 3,182 messages against `61005`'s 4: its 8.4%
  is computed over a small slice of the feed, and a connection that skips heavily is one that
  cannot keep up, whatever its percentage says.
- A connection with fewer than 10 races is not ranked at all, and a `median_lag_ms` computed
  from fewer than 5 losses is reported as `n/a` rather than printed to three decimals. With
  too few samples, whoever happened to score reports 100% and means nothing.

### What will fool you if you write this yourself

Every one of these produced a confident, plausible, wrong number before it was fixed. They are
why the file is longer than it looks like it should be.

- **A replayed backlog is not a race.** The feed replays history to every new connection — in
  one measurement 85% of all messages arrived in the first ten seconds, at roughly 275/s
  against a live rate of about 9.5/s. A connection scored during its own replay "wins"
  everything by construction. New connections are admitted at the pool's current high-water
  sequence and their samples discarded until they have caught up.
- **Catching up makes a connection look slow, not fast.** Scored naively, challengers came in
  at 17–244 ms against incumbents at 3–6 ms — which reads as *"aged connections are better,
  never redial"*, the exact opposite of the truth.
- **Winning is not a lag of zero.** Score a connection only on the races it *loses*. If a win
  writes 0.0 into its own samples, anything winning more than half its races reports a median
  lag of exactly 0.000 forever.
- **Do not probe and then reconnect on the same 4-tuple.** Reusing it straight after an RST
  gets the SYN dropped and the kernel waits out its retransmit timer — one measured handshake
  took 1034 ms instead of 1.3 ms and was recorded as a genuine handshake. Measure on the
  socket you intend to keep.
- **Discard a losing probe with RST, not FIN.** A clean close parks the 4-tuple in `TIME_WAIT`
  and the next probe in the port walk is handed the same port back.
- **ICMP cannot see any of this.** `ping` and a default `mtr` carry one fixed flow tuple, so
  they sample one path and stay on it. Both lotteries above are drawn **per connection**, so a
  tool that only ever makes *one* connection cannot see either — it reports whichever draw it
  happened to get as though it were the network. On one host, two addresses of the same service
  gave two *opposite* wrong pictures — one looked flawless, one uniformly slow, and neither
  matched what clients actually got.

---

## 3. `rh-feed-client.py` — reading a feed, from one connection or several

A minimal, dependency-free reader. It exists to be **read and copied**, not just run:
everything it does is what your own client has to do.

```bash
# straight from Robinhood
python3 rh-feed-client.py --url wss://feed.mainnet.chain.robinhood.com --seconds 60

# race two sources and see which delivers first
python3 rh-feed-client.py \
    --url wss://feed.mainnet.chain.robinhood.com \
    --url ws://your-relay:9642 \
    --seconds 60
```

Repeat `--url` to consume several sources at once. Every message is counted **once**,
credited to whichever source delivered it first, and the summary tells you the split —
which is the honest way to compare two feeds, because it never compares clocks on
different machines.

### What it handles that a naive client does not

- **`permessage-deflate` is mandatory** on the public endpoint since 2026-09-17. Offer it
  or you get `400 Bad Request` and no stream at all. ⚠️ It compresses the **message**, not
  the frame: RSV1 rides on the first frame only and you inflate after reassembly.
  Inflating per frame earns a 101 and then silently yields nothing on anything fragmented.
- **The feed replays backlog to every new connection.** In one of our measurements 85% of
  all messages arrived in the first ten seconds. Anything you time during that window is
  history, not live delivery — `--settle` discards it.
- **PING must be answered** or the server drops you.
- **Sequence gaps are reported**, so a silent hole cannot pass as a clean run.

### One thing worth knowing before you scale it

The endpoint limits **concurrency per source address**, not request rate. Opening more
sockets from one address does not buy you more of the feed — each connection is also
pinned to one backend origin for its lifetime, and origins do not deliver at the same
time. That is why this script can race several sources: the useful unit is *distinct
source*, not *more sockets*.

## 2. `sequencer-path-selector.py` — the submit leg

### TL;DR

The sequencer is plain EC2 with no CDN in front of it, so none of the Cloudflare lotteries
apply and the question has to be asked again from scratch. What a client actually controls
here is:

1. **Which destination address** — the hostname publishes several A records, and a client
   that just resolves the name draws one at random per connection.
2. **The source port** — ECMP hashes the 4-tuple, so the source port can select a path.
3. Nothing else. There is no AAAA record, so IPv6 is not on the menu.

Two things make this honest, and they are the whole point of the script:

- **Interleaving.** Every (destination, source port) pair is sampled round-robin. Measuring
  one pair to completion and then the next conflates *which pair* with *when*.
- **A determinism re-test.** A lottery is only worth playing if the draw sticks. If a "fast"
  port is not still fast a minute later, there was nothing to select and the spread was noise
  wearing a costume. **The script re-tests its own winners and will tell you they did not
  hold.** That verdict is the result.

It reports one of three things: a stable selection, **no material difference**, or **a ranking
that did not reproduce** — and it refuses to name a winner in the latter two cases.

### Usage

```bash
# defaults: resolve live, 16 source ports per target, 25 samples each
python3 sequencer-path-selector.py

# quicker sweep, shorter wait before the determinism re-test
python3 sequencer-path-selector.py --ports 8 --reps 12 --recheck-after 30

# pin your own targets and save the raw samples
python3 sequencer-path-selector.py --targets 3.141.111.43,3.142.9.34 --json out.json

# bind a specific source address on a multi-homed host
python3 sequencer-path-selector.py --src-ip 203.0.113.10
```

### Example

```
$ python3 sequencer-path-selector.py --ports 8 --reps 12 --recheck-after 30
# resolved sequencer.mainnet.chain.robinhood.com -> 3.136.74.196,3.141.111.43,3.142.9.34
# sequencer lottery: 3 destinations x 8 source ports = 24 pairs, 12 samples each, interleaved

destination             n      min      p50      p90      sd
3.136.74.196           96    1.572    2.835    3.553   1.987
3.141.111.43           96    1.339    2.152    2.844   0.487
3.142.9.34             96    1.811    2.482    3.117   0.685
  -> destination choice is worth 0.683 ms at p50 (3.141.111.43 best, 3.136.74.196 worst)

source-port effect, within each destination
  3.136.74.196     port-median spread  0.883 ms | within-pair sd  1.263 | ratio 0.70
      fastest ports [24001, 24000, 24003] -> [2.6, 2.682, 2.752]
      slowest ports [24006, 24004, 24005] -> [2.9, 3.157, 3.483]
  3.141.111.43     port-median spread  0.925 ms | within-pair sd  0.338 | ratio 2.74
      fastest ports [24007, 24005, 24006] -> [1.809, 1.927, 1.978]
      slowest ports [24002, 24000, 24004] -> [2.584, 2.641, 2.734]
  3.142.9.34       port-median spread  0.577 ms | within-pair sd  0.486 | ratio 1.19
      fastest ports [24002, 24000, 24007] -> [2.132, 2.302, 2.404]
      slowest ports [24005, 24003, 24001] -> [2.61, 2.649, 2.709]

# determinism re-test in 30s (a lottery you cannot re-draw is not selectable)
pair                        run1 p50  run2 p50    delta   rank held?
3.136.74.196 :24001 (fast)     2.600     2.923   +0.324
3.136.74.196 :24005 (slow)     3.483     3.623   +0.140   YES
3.141.111.43 :24007 (fast)     1.809     2.112   +0.303
3.141.111.43 :24004 (slow)     2.734     2.798   +0.064   YES
3.142.9.34 :24002 (fast)       2.132     2.039   -0.093
3.142.9.34 :24001 (slow)       2.709     2.800   +0.091   YES
3.141.111.43 (best dst)        2.152     2.171   +0.019
3.136.74.196 (worst dst)       2.835     2.820   -0.016   YES

========================================================================
  rejected: destination choice 0.683 ms is only 0.6x the 1.053 ms spread inside each destination
  rejected: 3.142.9.34 port spread 0.577 is only 1.2x its sd 0.486 — below the 2.0x bar
determinism: fast/slow order held on 3/3 destination(s); destination ordering held
VERDICT: worth playing —
         source-port selection on 3.141.111.43 (spread 0.925 = 2.7x the sd of 0.338)
```

Four things in that output are worth pointing at:

- **Two of the three candidate findings were rejected, and it says why.** `3.142.9.34`'s port
  spread was 1.2× its own noise; the *destination* effect — a real-looking 0.683 ms — was only
  0.6× the spread inside each destination, because `3.136.74.196` was unusually noisy in that
  window. Both are printed as `rejected:` lines rather than quietly dropped.
- **Every claim faces the same two gates**, the destination dimension included: an effect at
  least 2× the noise it sits in, *and* an ordering that reproduced on fresh samples. The
  destination ordering is re-drawn on **ephemeral** ports so it measures the destination and
  not a port.
- **A skipped gate is not a pass.** A destination whose re-test returns nothing is reported as
  unverified and is not recommended, and a run where *nothing* could be re-tested returns
  `CANNOT VERIFY` rather than a verdict.
- **A pick that fails the re-test can be worse than not choosing at all.** Playing a lottery
  without verifying reproducibility is not a neutral act.

---

## All flags

`sequencer-feed-monitor.py <race|watch>`

| flag | default | what it does |
|---|---|---|
| `--host` | the Robinhood feed | hostname to dial and send as SNI |
| `--ip` | resolve `--host` | pin one **destination** edge address |
| `--v6` | off | use IPv6 |
| `--src-ip` | kernel default | bind this **local source** address — one instance per address is how you exceed the per-address cap |
| `--path` | `/` | WebSocket request path |
| `--keep` / `--probe` | 4 / 12 | how many to keep, how many to draw |
| `--sport-base` | 61000 | first source port in the walk |
| `--seconds` / `--every` | 300 / 30 | `watch` duration and report interval |
| `--seq-field` | feed's own | regex with one capture group for the message id |
| `--json FILE` | off | append NDJSON: the cohort, each report, and a final row |

`sequencer-path-selector.py`

| flag | default | what it does |
|---|---|---|
| `--targets` | resolve live | comma-separated destination addresses |
| `--port` | 443 | destination port |
| `--ports` / `--sport-base` | 16 / 24000 | how many source ports, starting where |
| `--reps` | 25 | samples per (destination, source port) pair |
| `--interval` / `--timeout` | 0.01 / 3.0 | pause between samples; per-connect timeout |
| `--recheck-after` | 45 | seconds to wait before the determinism re-test |
| `--src-ip` | kernel default | bind this local source address |
| `--json FILE` | off | write the raw per-pair samples |

⚠️ `watch` adds a fixed ~12 second settling period, during which the replay backlog is
discarded, and that is **not** counted in `--seconds`.

---

## What `--json` writes (path selector)

A consumer should read the tool's **verdict**, not re-derive one from the raw counters.
That mistake — a downstream analyser inferring "worth playing" from the determinism
counts alone, blind to the effect-size gate — is why these fields exist:

| key | meaning |
|---|---|
| `schema` | `2`. **Treat a file without this as withheld, not as a pass.** |
| `verdict` | `worth_playing`, `no_lottery`, or `not_reproducible` — the tool's own decision |
| `selected` | `[destination, source_port]` the tool endorses, or absent |
| `best_dst` / `dst_gain_ms` | best destination by p50 and what choosing it is worth |
| `dst_order_held` / `port_order_held` | did each ordering reproduce on fresh samples |
| `held` / `total` | destinations whose port ordering held, over those actually re-tested |
| `pairs` / `recheck` | raw per-sample lists, keyed `"<dst>|<sport>"` |

⛔ `held`/`total` counts only destinations that produced re-test samples. A skipped
gate is in neither the numerator nor the denominator — so `2/2` does not mean two of
two destinations, it means two of the two that could be tested.

## A note on concurrency

Neither script tries to open an unlimited number of connections, because the feed will not
allow it — and the shape of the limit is not what most people assume.

**It is a concurrency cap, not a rate limit.** You may open as many connections as you like
over time; what you may not do is hold many open at once. There is no penalty window — the
moment you release a slot, you may use it again.

The cap is **per source address** and it is small — **two to five observed so far** — and it **moves**,
so neither script hardcodes a number. `sequencer-feed-monitor.py` learns it from the first
refusal and backs off.

Two consequences worth knowing before you design around it:

- **To hold N connections you need roughly N/2 source addresses.**
- **The bucket is the address, not the prefix.** Two addresses inside one IPv6 `/64` carry
  independent budgets, so a `/64` — which holds 2⁶⁴ addresses — is already more than any
  design can consume. A larger allocation such as a `/56` is useful for subnetting; it buys
  **no additional concurrency**.

Bare TCP connections do not count against the cap. They terminate at the CDN edge — only the
WebSocket **upgrade** consumes a slot, which is why the source-port sweep is essentially free
while holding sessions is rationed.

---

## License

MIT — see [LICENSE](LICENSE).
