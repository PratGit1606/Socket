#!/usr/bin/env python3
"""
DHT Peer — Full Project
Supports: register, setup-dht, query-dht, leave-dht,
          join-dht, deregister, teardown-dht
All peer-to-peer messages handled in a background listener thread.
"""
import socket, sys, threading, csv, time, random, re

if len(sys.argv) != 3:
    print("Usage: python peer.py <manager-ip> <manager-port>")
    sys.exit(1)

MGR_IP   = sys.argv[1]
MGR_PORT = int(sys.argv[2])
BUFSIZE  = 65535
NAME  = None
MPORT = None
PPORT = None

my_id        = None
ring_n       = None
peer_tuples  = []          
local_table  = {}          
record_count = 0

mgr_sock  = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
peer_sock = None

_query_event   = threading.Event()
_query_result  = {}         

_rebuild_event = threading.Event()   
_teardown_done = threading.Event()   

state_lock = threading.Lock()

def send_to_manager(msg):
    mgr_sock.sendto(msg.encode(), (MGR_IP, MGR_PORT))

def send_to_peer(ip, port, msg):
    peer_sock.sendto(msg.encode(), (ip, int(port)))

def handle_mgr_receive_blocking(timeout=5.0):
    mgr_sock.settimeout(timeout)
    try:
        data, _ = mgr_sock.recvfrom(BUFSIZE)
        return data.decode().strip()
    except socket.timeout:
        return None
    finally:
        mgr_sock.settimeout(None)

def right_neighbor():
    """Return (name, ip, pport) of this node's right neighbour."""
    idx = (my_id + 1) % ring_n
    return peer_tuples[idx]

def first_prime_above(x):
    def isprime(p):
        if p < 2: return False
        if p % 2 == 0: return p == 2
        for i in range(3, int(p**0.5) + 1, 2):
            if p % i == 0: return False
        return True
    y = x + 1
    while not isprime(y):
        y += 1
    return y

def compute_pos_and_id(event_id, s, n):
    pos = event_id % s
    tid = pos % n
    return pos, tid


def peer_listener():
    global my_id, ring_n, peer_tuples, local_table, record_count

    while True:
        try:
            data, addr = peer_sock.recvfrom(BUFSIZE)
        except socket.timeout:
            continue
        except Exception as e:
            print(f"[{NAME}] peer_listener error: {e}")
            continue

        s     = data.decode().strip()
        parts = s.split('|')
        cmd   = parts[0]

        if cmd == "SET-ID":
            _, id_s, n_s, tuple_list = parts[0], parts[1], parts[2], parts[3]
            with state_lock:
                my_id  = int(id_s)
                ring_n = int(n_s)
                tuples = [t for t in tuple_list.split(';') if t]
                peer_tuples = []
                for t in tuples:
                    nm, ip, pp = t.split(',')
                    peer_tuples.append((nm, ip, int(pp)))
            rname = peer_tuples[(my_id + 1) % ring_n][0]
            print(f"[{NAME}] SET-ID id={my_id} n={ring_n} right={rname}")

        elif cmd == "STORE":
            if len(parts) < 4:
                continue
            target_id  = int(parts[1])
            pos        = int(parts[2])
            record     = "|".join(parts[3:])
            if my_id is None:
                print(f"[{NAME}] STORE before SET-ID; discarding")
                continue
            if target_id == my_id:
                with state_lock:
                    local_table.setdefault(pos, []).append(record)
                    record_count += 1
                print(f"[{NAME}] STORED pos={pos} total={record_count}")
            else:
                nm, ip, pp = right_neighbor()
                send_to_peer(ip, pp, s)
                print(f"[{NAME}] FORWARD STORE -> {nm} target={target_id}")

        elif cmd == "COUNT-REQ":
            reqid = parts[1] if len(parts) > 1 else "0"
            reply = f"COUNT-REPLY|{reqid}|{NAME}|{record_count}"
            peer_sock.sendto(reply.encode(), addr)
            print(f"[{NAME}] COUNT-REQ answered reqid={reqid} count={record_count}")

        elif cmd == "FIND-EVENT":
            if len(parts) < 9:
                print(f"[{NAME}] malformed FIND-EVENT: {s}")
                continue
            ev_id   = int(parts[1])
            hs      = int(parts[2])
            hn      = int(parts[3])
            pos     = int(parts[4])
            id_seq  = parts[5]         
            src_nm  = parts[6]
            src_ip  = parts[7]
            src_pp  = int(parts[8])

            visited = set(int(x) for x in id_seq.split(',') if x != "")

            if my_id is None:
                print(f"[{NAME}] FIND-EVENT before SET-ID; discarding")
                continue

            found_record = None
            if pos in local_table:
                for rec in local_table[pos]:
                    try:
                        if int(rec.split(',')[0]) == ev_id:
                            found_record = rec
                            break
                    except Exception:
                        pass

            new_seq = id_seq + ("," if id_seq else "") + str(my_id)

            if found_record is not None:
                reply = f"FIND-REPLY|SUCCESS|{new_seq}|{found_record}"
                peer_sock.sendto(reply.encode(), (src_ip, src_pp))
                print(f"[{NAME}] FIND-EVENT found ev={ev_id} -> replying to {src_nm}")
            else:
                visited.add(my_id)
                all_ids  = set(range(hn))
                unvisited = all_ids - visited
                if not unvisited:
                    reply = f"FIND-REPLY|FAILURE|{new_seq}|"
                    peer_sock.sendto(reply.encode(), (src_ip, src_pp))
                    print(f"[{NAME}] FIND-EVENT ev={ev_id} not found after all nodes")
                else:
                    next_id  = random.choice(list(unvisited))
                    next_nm, next_ip, next_pp = peer_tuples[next_id]
                    fwd = f"FIND-EVENT|{ev_id}|{hs}|{hn}|{pos}|{new_seq}|{src_nm}|{src_ip}|{src_pp}"
                    send_to_peer(next_ip, next_pp, fwd)
                    print(f"[{NAME}] FIND-EVENT hot potato ev={ev_id} -> {next_nm} (id={next_id})")

        elif cmd == "FIND-REPLY":
            _query_result['raw'] = s
            _query_event.set()
            print(f"[{NAME}] FIND-REPLY received: {s[:80]}")

        elif cmd == "TEARDOWN":

            orig = parts[1] if len(parts) > 1 else ""
            with state_lock:
                local_table.clear()
                record_count = 0
            print(f"[{NAME}] TEARDOWN: cleared local table")
            if orig == NAME:
                print(f"[{NAME}] TEARDOWN circled back to originator")
                _teardown_done.set()
            else:
                nm, ip, pp = right_neighbor()
                send_to_peer(ip, pp, s)
                print(f"[{NAME}] TEARDOWN forwarded -> {nm}")

        elif cmd == "RESET-ID":
            if len(parts) < 8:
                print(f"[{NAME}] malformed RESET-ID: {s}")
                continue
            new_id    = int(parts[1])
            new_n     = int(parts[2])
            tlist     = parts[3]
            leave_nm  = parts[4]
            orig_nm   = parts[5]
            orig_ip   = parts[6]
            orig_pp   = int(parts[7])

            with state_lock:
                my_id  = new_id
                ring_n = new_n
                tuples_raw = [t for t in tlist.split(';') if t]
                peer_tuples = []
                for t in tuples_raw:
                    nm, ip, pp = t.split(',')
                    peer_tuples.append((nm, ip, int(pp)))

            print(f"[{NAME}] RESET-ID new_id={my_id} new_n={ring_n}")

            if NAME == orig_nm:
                print(f"[{NAME}] RESET-ID returned to leave originator")
                _rebuild_event.set()
            else:
                nm2, ip2, pp2 = right_neighbor()
                fwd = f"RESET-ID|{new_id + 1}|{new_n}|{tlist}|{leave_nm}|{orig_nm}|{orig_ip}|{orig_pp}"
                send_to_peer(ip2, pp2, fwd)
                print(f"[{NAME}] RESET-ID forwarded -> {nm2}")

        elif cmd == "REBUILD-DHT":
            if len(parts) < 5:
                print(f"[{NAME}] malformed REBUILD-DHT: {s}")
                continue
            yyyy    = parts[1]
            orig_nm = parts[2]
            orig_ip = parts[3]
            orig_pp = int(parts[4])
            print(f"[{NAME}] REBUILD-DHT received — I am new leader, rebuilding ring")
            t = threading.Thread(
                target=rebuild_ring,
                args=(yyyy, orig_nm, orig_ip, orig_pp),
                daemon=True
            )
            t.start()

        elif cmd == "REBUILD-DONE":
            print(f"[{NAME}] REBUILD-DONE received — rebuilding complete")
            _rebuild_event.set()

        elif cmd == "JOIN-RING":
            if len(parts) < 4:
                continue
            join_nm  = parts[1]
            join_ip  = parts[2]
            join_pp  = int(parts[3])
            print(f"[{NAME}] JOIN-RING from {join_nm}")
            t = threading.Thread(
                target=leader_handle_join,
                args=(join_nm, join_ip, join_pp),
                daemon=True
            )
            t.start()

        else:
            print(f"[{NAME}] unknown peer cmd: {s[:80]}")



def inject_stores(yyyy, ptuples):
    """Read CSV and inject STORE messages into the ring (to right neighbour)."""
    n = len(ptuples)
    fname = f"details-{yyyy}.csv"
    try:
        with open(fname, newline='', encoding='utf-8', errors='ignore') as f:
            reader = list(csv.reader(f))
    except FileNotFoundError:
        print(f"[{NAME}] dataset {fname} not found.")
        return 0
    if len(reader) <= 1:
        print(f"[{NAME}] dataset empty")
        return 0

    l = len(reader) - 1
    s_val = first_prime_above(2 * l)
    print(f"[{NAME}] rows={l}  hash-size={s_val}  ring-size={n}")

    right_nm, right_ip, right_pp = ptuples[1 % n]
    for row in reader[1:]:
        try:
            ev_id = int(row[0])
        except Exception:
            ev_id = abs(hash(",".join(row))) % (10**9)
        pos, tid = compute_pos_and_id(ev_id, s_val, n)
        rec = ",".join(row)
        msg = f"STORE|{tid}|{pos}|{rec}"
        send_to_peer(right_ip, right_pp, msg)

    print(f"[{NAME}] injected all STORE messages")
    return s_val


def collect_counts(ptuples, timeout=5.0):
    """Send COUNT-REQ to all ring peers and collect COUNT-REPLY."""
    reqid   = str(int(time.time()))
    replies = {}
    expected = set(nm for nm, _, _ in ptuples)

    tmp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    tmp.bind(("", 0))
    tmp.settimeout(timeout)

    for nm, ip, pp in ptuples:
        tmp.sendto(f"COUNT-REQ|{reqid}".encode(), (ip, pp))

    deadline = time.time() + timeout
    while expected and time.time() < deadline:
        try:
            data, _ = tmp.recvfrom(BUFSIZE)
            p = data.decode().strip().split('|')
            if p[0] == "COUNT-REPLY" and len(p) >= 4 and p[1] == reqid:
                replies[p[2]] = int(p[3])
                expected.discard(p[2])
        except socket.timeout:
            break
    tmp.close()

    for nm, _, _ in ptuples:
        if nm not in replies:
            replies[nm] = 0
    return replies


def leader_build_dht(tuples_str, yyyy):
    """Called after SETUP-DHT SUCCESS. Builds the ring from scratch."""
    global my_id, ring_n, peer_tuples, local_table, record_count

    tuples_raw = [t for t in tuples_str.split(';') if t]
    peer_list  = []
    for t in tuples_raw:
        nm, ip, pp = t.split(',')
        peer_list.append((nm, ip, int(pp)))

    n = len(peer_list)
    tuple_list_full = ";".join(f"{nm},{ip},{pp}" for nm, ip, pp in peer_list)

    with state_lock:
        peer_tuples = peer_list
        my_id       = 0
        ring_n      = n

    for i, (nm, ip, pp) in enumerate(peer_list):
        msg = f"SET-ID|{i}|{n}|{tuple_list_full}"
        send_to_peer(ip, pp, msg)
        print(f"[leader] SET-ID -> {nm} id={i}")

    time.sleep(0.3)

    s_val = inject_stores(yyyy, peer_list)
    time.sleep(0.5)

    counts = collect_counts(peer_list)
    print("[leader] per-node stored counts:")
    for nm, _, _ in peer_list:
        print(f"  {nm}: {counts.get(nm, 0)}")

    send_to_manager(f"DHT-COMPLETE|{NAME}")
    resp = handle_mgr_receive_blocking(timeout=8.0)
    print("[leader] manager reply:", resp)


def rebuild_ring(yyyy, orig_nm, orig_ip, orig_pp):
    """
    Called on the new leader after a leave or join causes a rebuild.
    Repopulates the ring and notifies the leave/join initiator.
    """
    global local_table, record_count

    with state_lock:
        ptuples = list(peer_tuples)
        n       = ring_n

    tuple_list_full = ";".join(f"{nm},{ip},{pp}" for nm, ip, pp in ptuples)
    for i, (nm, ip, pp) in enumerate(ptuples):
        msg = f"SET-ID|{i}|{n}|{tuple_list_full}"
        send_to_peer(ip, pp, msg)
        print(f"[{NAME}] REBUILD SET-ID -> {nm} id={i}")

    time.sleep(0.3)

    s_val = inject_stores(yyyy, ptuples)
    time.sleep(0.5)

    counts = collect_counts(ptuples)
    print(f"[{NAME}] REBUILD per-node counts:")
    for nm, _, _ in ptuples:
        print(f"  {nm}: {counts.get(nm, 0)}")

    send_to_peer(orig_ip, orig_pp, "REBUILD-DONE")
    print(f"[{NAME}] REBUILD-DONE sent to {orig_nm}")


def leader_handle_join(join_nm, join_ip, join_pp):
    """
    Called on the current leader when a JOIN-RING message arrives.
    Appends the new peer to the ring, re-numbers, re-populates.
    The current leader stays leader (id=0).
    """
    global ring_n, peer_tuples

    with state_lock:
        old_tuples = list(peer_tuples)

    new_tuples = old_tuples + [(join_nm, join_ip, join_pp)]
    new_n      = len(new_tuples)

    with state_lock:
        peer_tuples = new_tuples
        ring_n      = new_n

    yyyy = _join_yyyy  

    tuple_list_full = ";".join(f"{nm},{ip},{pp}" for nm, ip, pp in new_tuples)
    for i, (nm, ip, pp) in enumerate(new_tuples):
        msg = f"SET-ID|{i}|{new_n}|{tuple_list_full}"
        send_to_peer(ip, pp, msg)
        print(f"[{NAME}] JOIN SET-ID -> {nm} id={i}")

    time.sleep(0.3)
    s_val = inject_stores(yyyy, new_tuples)
    time.sleep(0.5)

    counts = collect_counts(new_tuples)
    print(f"[{NAME}] JOIN per-node counts:")
    for nm, _, _ in new_tuples:
        print(f"  {nm}: {counts.get(nm, 0)}")

    send_to_peer(join_ip, join_pp, "REBUILD-DONE")
    print(f"[{NAME}] JOIN rebuild done, REBUILD-DONE -> {join_nm}")


def do_query_dht(event_id_str):
    global my_id, ring_n, peer_tuples

    try:
        ev_id = int(event_id_str)
    except Exception:
        print("bad event-id")
        return

    send_to_manager(f"QUERY-DHT|{NAME}")
    reply = handle_mgr_receive_blocking(timeout=5.0)
    print("[peer] manager reply:", reply)
    if not reply or "SUCCESS" not in reply:
        print("query-dht failed")
        return

    extra = reply.split('|', 3)[3]
    parts = extra.split(',')
    if len(parts) != 3:
        print("bad manager reply format")
        return
    start_nm, start_ip, start_pp = parts[0], parts[1], int(parts[2])
    with state_lock:
        hn      = ring_n if ring_n else 1
        ptuples = list(peer_tuples)

    with state_lock:
        hs = _dht_hash_size if _dht_hash_size else 0

    if hs == 0:
        hs = _dht_hash_size

    pos = ev_id % hs if hs else 0

    _query_event.clear()
    _query_result.clear()

    msg = (f"FIND-EVENT|{ev_id}|{hs}|{hn}|{pos}||"
           f"{NAME}|127.0.0.1|{PPORT}")
    my_ip = _my_ip if _my_ip else "127.0.0.1"
    msg = (f"FIND-EVENT|{ev_id}|{hs}|{hn}|{pos}||"
           f"{NAME}|{my_ip}|{PPORT}")

    peer_sock.sendto(msg.encode(), (start_ip, start_pp))
    print(f"[peer] FIND-EVENT ev={ev_id} -> {start_nm} ({start_ip}:{start_pp})")

    got = _query_event.wait(timeout=10.0)
    if not got or 'raw' not in _query_result:
        print(f"Storm event {ev_id}: no reply received (timeout)")
        return

    raw    = _query_result['raw']
    rparts = raw.split('|', 3)
    if len(rparts) < 4:
        print("malformed FIND-REPLY")
        return

    status  = rparts[1]
    id_seq  = rparts[2]
    payload = rparts[3]

    FIELDS = [
        "event_id", "state", "year", "month_name", "event_type",
        "cz_type", "cz_name", "injuries_direct", "injuries_indirect",
        "deaths_direct", "deaths_indirect", "damage_property",
        "damage_crops", "tor_f_scale"
    ]

    if status == "SUCCESS":
        print(f"\n--- Storm event {ev_id} FOUND ---")
        print(f"Nodes probed (id-seq): {id_seq}")
        cols = payload.split(',')
        for i, field in enumerate(FIELDS):
            val = cols[i] if i < len(cols) else ""
            print(f"  {field}: {val}")
        print()
    else:
        print(f"Storm event {ev_id} not found in the DHT.")
        print(f"Nodes probed (id-seq): {id_seq}")

def do_leave_dht(yyyy):
    """
    Steps (per spec §1.2.3):
    1. Teardown local tables via ring propagation.
    2. Renumber: send RESET-ID around ring (skip ourselves).
    3. Our right neighbour becomes new leader; it sends REBUILD-DHT.
    4. We wait for REBUILD-DONE, then send DHT-REBUILT to manager.
    """
    global my_id, ring_n, peer_tuples, local_table, record_count

    _teardown_done.clear()
    nm, ip, pp = right_neighbor()
    send_to_peer(ip, pp, f"TEARDOWN|{NAME}")
    print(f"[{NAME}] TEARDOWN sent -> {nm}. Waiting for it to circle back...")

    got = _teardown_done.wait(timeout=15.0)
    if not got:
        print(f"[{NAME}] WARNING: TEARDOWN did not return in time")

    with state_lock:
        old_tuples = list(peer_tuples)
        old_n      = ring_n

    new_tuples = [t for t in old_tuples if t[0] != NAME]
    new_n      = len(new_tuples)
    tuple_list = ";".join(f"{nm2},{ip2},{pp2}" for nm2, ip2, pp2 in new_tuples)

    right_nm, right_ip, right_pp = old_tuples[(my_id + 1) % old_n][0], \
                                   old_tuples[(my_id + 1) % old_n][1], \
                                   old_tuples[(my_id + 1) % old_n][2]

    my_ip = _my_ip if _my_ip else "127.0.0.1"
    _rebuild_event.clear()

    msg = (f"RESET-ID|0|{new_n}|{tuple_list}|{NAME}|{NAME}|{my_ip}|{PPORT}")
    send_to_peer(right_ip, right_pp, msg)
    print(f"[{NAME}] RESET-ID sent -> {right_nm}. Waiting for circle-back...")

    got = _rebuild_event.wait(timeout=15.0)
    if not got:
        print(f"[{NAME}] WARNING: RESET-ID did not return in time")

    with state_lock:
        my_id        = None
        ring_n       = None
        peer_tuples  = []
        local_table  = {}
        record_count = 0

    _rebuild_event.clear()
    msg = f"REBUILD-DHT|{yyyy}|{NAME}|{my_ip}|{PPORT}"
    send_to_peer(right_ip, right_pp, msg)
    print(f"[{NAME}] REBUILD-DHT sent -> {right_nm}. Waiting for REBUILD-DONE...")

    got = _rebuild_event.wait(timeout=30.0)
    if not got:
        print(f"[{NAME}] WARNING: REBUILD-DONE not received in time")

    new_leader = right_nm
    send_to_manager(f"DHT-REBUILT|{NAME}|{new_leader}")
    resp = handle_mgr_receive_blocking(timeout=8.0)
    print(f"[{NAME}] DHT-REBUILT manager reply:", resp)


_join_yyyy = None  

def do_join_dht(yyyy):
    global _join_yyyy, my_id, ring_n, peer_tuples, local_table, record_count

    _join_yyyy = yyyy

    send_to_manager(f"JOIN-DHT|{NAME}")
    reply = handle_mgr_receive_blocking(timeout=5.0)
    print("[peer] manager reply:", reply)
    if not reply or "SUCCESS" not in reply:
        print("join-dht failed")
        return

    extra = reply.split('|', 3)[3]
    parts = extra.split(',')
    if len(parts) != 3:
        print("bad manager reply format")
        return
    leader_nm, leader_ip, leader_pp = parts[0], parts[1], int(parts[2])

    my_ip = _my_ip if _my_ip else "127.0.0.1"

    _rebuild_event.clear()

    msg = f"JOIN-RING|{NAME}|{my_ip}|{PPORT}"
    peer_sock.sendto(msg.encode(), (leader_ip, leader_pp))
    print(f"[{NAME}] JOIN-RING -> {leader_nm}. Waiting for REBUILD-DONE...")

    got = _rebuild_event.wait(timeout=30.0)
    if not got:
        print(f"[{NAME}] WARNING: REBUILD-DONE not received in time")
        return

    send_to_manager(f"DHT-REBUILT|{NAME}|{leader_nm}")
    resp = handle_mgr_receive_blocking(timeout=8.0)
    print(f"[{NAME}] DHT-REBUILT manager reply:", resp)


def do_teardown_dht():
    """Leader tears down the DHT."""
    send_to_manager(f"TEARDOWN-DHT|{NAME}")
    reply = handle_mgr_receive_blocking(timeout=5.0)
    print("[peer] manager reply:", reply)
    if not reply or "SUCCESS" not in reply:
        print("teardown-dht failed")
        return

    _teardown_done.clear()
    nm, ip, pp = right_neighbor()
    send_to_peer(ip, pp, f"TEARDOWN|{NAME}")
    print(f"[{NAME}] TEARDOWN sent -> {nm}. Waiting to circle back...")

    got = _teardown_done.wait(timeout=15.0)
    if not got:
        print(f"[{NAME}] WARNING: TEARDOWN did not return in time")

    with state_lock:
        local_table.clear()
        record_count = 0

    send_to_manager(f"TEARDOWN-COMPLETE|{NAME}")
    resp = handle_mgr_receive_blocking(timeout=8.0)
    print("[peer] manager reply:", resp)
    print(f"[{NAME}] DHT torn down successfully.")
def do_deregister():
    send_to_manager(f"DEREGISTER|{NAME}")
    reply = handle_mgr_receive_blocking(timeout=5.0)
    print("[peer] manager reply:", reply)
    if reply and "SUCCESS" in reply:
        print(f"[{NAME}] deregistered. Exiting.")
        sys.exit(0)
    else:
        print("deregister failed (are you still in the DHT?)")

_my_ip        = None
_dht_hash_size = 0   

print("peer started. Type 'help' for commands.")

while True:
    try:
        cmdline = input("cmd> ").strip()
    except EOFError:
        break
    if not cmdline:
        continue
    parts = cmdline.split()
    cmd   = parts[0].lower()

    if cmd == "help":
        print("Commands: register | setup-dht <n> <YYYY> | query-dht <event_id>")
        print("          leave-dht <YYYY> | join-dht <YYYY> | teardown-dht")
        print("          deregister")

    elif cmd == "register":
        if NAME is not None:
            print("already registered")
            continue
        name  = input("peer-name (alpha <=15 chars): ").strip()
        ip    = input("ip (your local IP): ").strip()
        try:
            mport = int(input("m-port: ").strip())
            pport = int(input("p-port: ").strip())
        except Exception:
            print("bad ports"); continue

        try:
            mgr_sock.close()
            mgr_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            mgr_sock.bind(("", mport))
            peer_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            peer_sock.bind(("", pport))
        except Exception as e:
            print("socket bind error:", e); continue

        NAME   = name
        MPORT  = mport
        PPORT  = pport
        _my_ip = ip
        globals().update({"NAME": name, "MPORT": mport, "PPORT": pport,
                          "_my_ip": ip, "mgr_sock": mgr_sock,
                          "peer_sock": peer_sock})

        msg = f"REGISTER|{NAME}|{ip}|{MPORT}|{PPORT}"
        send_to_manager(msg)
        print("[peer] REGISTER sent, waiting...")
        reply = handle_mgr_receive_blocking(timeout=5.0)
        print("[peer] manager reply:", reply)
        if reply and "SUCCESS" in reply:
            t = threading.Thread(target=peer_listener, daemon=True)
            t.start()
            print(f"[{NAME}] registered and listener started.")
        else:
            print("register failed")
            NAME = None

    elif cmd == "setup-dht":
        if NAME is None:
            print("must register first"); continue
        if len(parts) != 3:
            print("Usage: setup-dht <n> <YYYY>"); continue
        n    = parts[1]
        yyyy = parts[2]
        send_to_manager(f"SETUP-DHT|{NAME}|{n}|{yyyy}")
        print("[peer] SETUP-DHT sent, waiting...")
        reply = handle_mgr_receive_blocking(timeout=10.0)
        print("[peer] manager reply:", reply)
        if reply and reply.startswith("MANAGER-REPLY|SETUP-DHT|SUCCESS|"):
            tuples_str = reply.split('|', 3)[3]
            leader_build_dht(tuples_str, yyyy)
            try:
                import csv as _csv
                with open(f"details-{yyyy}.csv", newline='',
                          encoding='utf-8', errors='ignore') as _f:
                    _l = sum(1 for _ in _f) - 1
                globals()['_dht_hash_size'] = first_prime_above(2 * _l)
            except Exception:
                globals()['_dht_hash_size'] = 0
        else:
            print("setup-dht failed or not leader")

    elif cmd == "query-dht":
        if NAME is None:
            print("must register first"); continue
        if len(parts) != 2:
            print("Usage: query-dht <event_id>"); continue
        do_query_dht(parts[1])

    elif cmd == "leave-dht":
        if NAME is None:
            print("must register first"); continue
        if len(parts) != 2:
            print("Usage: leave-dht <YYYY>"); continue
        do_leave_dht(parts[1])
    elif cmd == "join-dht":
        if NAME is None:
            print("must register first"); continue
        if len(parts) != 2:
            print("Usage: join-dht <YYYY>"); continue
        do_join_dht(parts[1])

    elif cmd == "teardown-dht":
        if NAME is None:
            print("must register first"); continue
        do_teardown_dht()

    elif cmd == "deregister":
        if NAME is None:
            print("must register first"); continue
        do_deregister()

    else:
        print("Unknown command. Type 'help' for list of commands.")