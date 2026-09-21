#!/usr/bin/env python3
"""
rh-feed-client.py -- read a Robinhood Chain sequencer feed.

Works UNCHANGED against either endpoint. That is the point:

    # straight from Robinhood, through Cloudflare
    rh-feed-client.py --url wss://feed.mainnet.chain.robinhood.com

    # from a Cloud Propeller relay -- same script, same output, one word changed
    rh-feed-client.py --url ws://<relay>:9642

WHAT THE RELAY CHANGES FOR YOU
==============================
The public endpoint caps concurrent websockets PER SOURCE ADDRESS (~2-3, and it
jitters -- discover it, never hard-code it). Each connection is also pinned at
connect time to one Cloudflare backend origin, named in the `__cflb` cookie, and
origins do not deliver at the same time. So to see every message as early as it
is available you need many connections across many addresses, and you must keep
scoring them because the leader changes.

A relay does that work upstream and serves you the merged, deduplicated result.
ONE connection from you, no address juggling, no origin bookkeeping.

⛔ TWO THINGS THAT WILL BITE YOU IF YOU WRITE THIS YOURSELF
  * The public endpoint has MANDATED permessage-deflate since 2026-09-17. Offer
    it or you get `400 Bad Request` and no stream at all. This client offers it
    and inflates RSV1 frames. A relay usually serves plain frames; both work
    here because compression is negotiated, not assumed.
  * The feed REPLAYS backlog to every new connection. In one of our
    measurements 85% of all messages arrived in the first ten seconds. Anything
    you time during that window is history, not live delivery -- `--settle`
    discards it.

Stdlib only. MIT.
"""
import argparse, base64, json, os, socket, ssl, struct, sys, time, zlib


def ws_key():
    return base64.b64encode(os.urandom(16)).decode()


def connect(url, timeout=10.0, source_addr=None):
    """Open one feed connection. Returns (sock, deflate_negotiated, headers)."""
    if "://" not in url:
        url = "ws://" + url
    scheme, rest = url.split("://", 1)
    hostport, _, path = rest.partition("/")
    path = "/" + path
    if hostport.startswith("["):                      # [v6]:port
        host, _, port = hostport[1:].partition("]")
        port = int(port.lstrip(":") or (443 if scheme == "wss" else 80))
    else:
        host, _, p = hostport.partition(":")
        port = int(p) if p else (443 if scheme == "wss" else 80)

    ai = socket.getaddrinfo(host, port, 0, socket.SOCK_STREAM)[0]
    s = socket.socket(ai[0], socket.SOCK_STREAM)
    s.settimeout(timeout)
    if source_addr:
        s.bind((source_addr, 0))
    s.connect(ai[4])
    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    if scheme == "wss":
        s = ssl.create_default_context().wrap_socket(s, server_hostname=host)

    key = ws_key()
    req = (f"GET {path} HTTP/1.1\r\n"
           f"Host: {host}\r\n"
           f"Upgrade: websocket\r\n"
           f"Connection: Upgrade\r\n"
           f"Sec-WebSocket-Version: 13\r\n"
           f"Sec-WebSocket-Key: {key}\r\n"
           # MANDATORY on the public endpoint; harmless if the relay declines it.
           f"Sec-WebSocket-Extensions: permessage-deflate; "
           f"client_no_context_takeover; server_no_context_takeover\r\n\r\n")
    s.sendall(req.encode())

    head = b""
    while b"\r\n\r\n" not in head:
        chunk = s.recv(4096)
        if not chunk:
            raise ConnectionError("connection closed during upgrade")
        head += chunk
    head, _, tail = head.partition(b"\r\n\r\n")
    status = head.split(b"\r\n", 1)[0].decode(errors="replace")
    if " 101" not in status:
        raise ConnectionError(f"upgrade refused: {status}")
    hdrs = {}
    for line in head.split(b"\r\n")[1:]:
        k, _, v = line.decode(errors="replace").partition(":")
        hdrs[k.strip().lower()] = v.strip()
    deflate = "permessage-deflate" in hdrs.get("sec-websocket-extensions", "").lower()
    return s, deflate, hdrs, tail


class Reader:
    """Incremental WebSocket reader. Feed it bytes, get back whole MESSAGES.

    ⛔ The earlier version was a blocking generator, which CANNOT be driven by a
    selector: on a non-blocking socket it raises on the first short read and the
    caller sees zero messages with no error. Parsing must be incremental to race
    several sockets at once.
    """
    def __init__(self, sock, deflate, initial=b""):
        self.sock=sock; self.deflate=deflate
        self.buf=bytearray(initial); self.frag_op=None; self.frag=bytearray(); self.frag_z=False

    @staticmethod
    def _inflate(b):
        return zlib.decompressobj(-zlib.MAX_WBITS).decompress(b + b"\x00\x00\xff\xff")

    def pump(self):
        """Read what is available, return a list of complete message payloads."""
        try:
            while True:
                d=self.sock.recv(262144)
                if not d: break
                self.buf+=d
                if len(d)<262144: break
            
        except (BlockingIOError, ssl.SSLWantReadError):
            pass
        except OSError:
            pass
        out=[]
        while True:
            b=self.buf
            if len(b)<2: break
            fin=bool(b[0]&0x80); rsv1=bool(b[0]&0x40); op=b[0]&0x0F
            ln=b[1]&0x7F; off=2
            if ln==126:
                if len(b)<4: break
                ln=struct.unpack("!H",b[2:4])[0]; off=4
            elif ln==127:
                if len(b)<10: break
                ln=struct.unpack("!Q",b[2:10])[0]; off=10
            if len(b)<off+ln: break
            payload=bytes(b[off:off+ln]); del self.buf[:off+ln]
            if op>=0x8:
                if op==0x9:                                   # PING -> PONG or we get dropped
                    m=os.urandom(4)
                    try:
                        self.sock.sendall(bytes([0x8A,0x80|len(payload)])+m+
                                          bytes(c^m[i&3] for i,c in enumerate(payload)))
                    except OSError: pass
                continue
            if op==0x0:
                self.frag+=payload
                if fin:
                    msg=bytes(self.frag)
                    out.append(self._inflate(msg) if self.frag_z else msg)
                    self.frag_op=None; self.frag=bytearray(); self.frag_z=False
                continue
            if fin:
                out.append(self._inflate(payload) if rsv1 else payload)
            else:
                self.frag_op=op; self.frag=bytearray(payload); self.frag_z=rsv1
        return out


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--url", required=True, action="append", metavar="URL",
                    help="ws://relay:9642 or wss://feed.mainnet.chain.robinhood.com. "
                         "REPEAT IT to race several sources: every message is counted ONCE, "
                         "credited to whichever source delivered it first.")
    ap.add_argument("--seconds", type=float, default=30.0)
    ap.add_argument("--settle", type=float, default=5.0,
                    help="discard this many seconds of replay backlog first")
    ap.add_argument("--source-addr", default=None, help="bind this local source address")
    ap.add_argument("--print-every", type=int, default=25)
    ap.add_argument("--json", metavar="FILE", help="append one JSON line per message")
    a = ap.parse_args()

    import selectors
    srcs=[]
    for u in a.url:
        t0=time.monotonic()
        try:
            sock,deflate,hdrs,tail = connect(u, source_addr=a.source_addr)
        except Exception as e:
            print(f"cannot connect to {u}: {e}"); continue
        print(f"connected to {u} in {(time.monotonic()-t0)*1000:.3f} ms "
              f"[{'permessage-deflate' if deflate else 'plain frames'}]")
        sock.setblocking(False)
        srcs.append({"url":u,"sock":sock,"rd":Reader(sock,deflate,tail),"first":0,"saw":0})
    if not srcs:
        print("no sources connected"); return 1
    if len(srcs)>1:
        print(f"racing {len(srcs)} sources; each message counted once, credited to "
              f"whichever delivered it first\n")

    sel=selectors.DefaultSelector()
    for i,s_ in enumerate(srcs): sel.register(s_["sock"], selectors.EVENT_READ, i)

    jf = open(a.json,"a") if a.json else None
    settle_until=time.monotonic()+a.settle
    deadline=time.monotonic()+a.seconds+a.settle
    n=skipped=gaps=0; first_seq=last_seq=None; seen=set()
    try:
        while time.monotonic()<=deadline:
            for key,_ in sel.select(0.4):
                s_=srcs[key.data]
                payloads=s_["rd"].pump()
                for payload in payloads:
                    try: obj=json.loads(payload)
                    except Exception: continue
                    for m in obj.get("messages",[obj]):
                        seq=m.get("sequenceNumber")
                        if seq is None: continue
                        s_["saw"]+=1
                        if time.monotonic()<settle_until:
                            skipped+=1; last_seq=seq; seen.add(seq); continue
                        if seq in seen: continue         # already had it from a faster source
                        seen.add(seq); s_["first"]+=1
                        if first_seq is None: first_seq=seq
                        elif last_seq is not None and seq>last_seq+1: gaps+=seq-last_seq-1
                        last_seq=seq; n+=1
                        if jf: jf.write(json.dumps({"utc":time.time(),"seq":seq,"src":s_["url"]})+"\n")
                        if a.print_every and n%a.print_every==0:
                            print(f"  {n:>6} messages   seq={seq}   "
                                  f"{n/max(1e-9,time.monotonic()-settle_until):.1f}/s")
    except KeyboardInterrupt:
        pass
    finally:
        if jf: jf.close()
        for s_ in srcs:
            try: s_["sock"].close()
            except Exception: pass
    if len(srcs)>1:
        print("\nwho delivered first:")
        for s_ in srcs:
            pct = 100.0*s_["first"]/n if n else 0.0
            print(f"  {s_['url']:<46} {s_['first']:>6} first ({pct:5.1f}%)   {s_['saw']:>6} seen")
    dur = max(1e-9, time.monotonic() - settle_until)
    print(f"\n{n} messages in {dur:.1f}s ({n/dur:.1f}/s), {skipped} discarded as replay backlog")
    print(f"sequence {first_seq} -> {last_seq}, gaps: {gaps}")
    return 0 if n > 0 and gaps == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
