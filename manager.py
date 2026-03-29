#!/usr/bin/env python3
import socket, sys, random, re

if len(sys.argv) != 2:
    print("Usage: python manager.py <port>")
    sys.exit(1)

GROUP_NUM = None

MGR_PORT = int(sys.argv[1])
BUFSIZE  = 65535

peers = {}

dht_state = {
    "exists":         False,
    "peers":          [],
    "leader":         None,
    "pending_action": None,
    "churn_peer":     None,
}

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.bind(("", MGR_PORT))
print(f"[manager] listening on UDP port {MGR_PORT}")

def send_reply(addr, cmd, code, extra=""):
    msg = f"MANAGER-REPLY|{cmd}|{code}|{extra}"
    sock.sendto(msg.encode(), addr)

def port_range_for_group(g):
    if g % 2 == 1:
        lo = (g // 2) * 1000 + 500
        hi = (g // 2) * 1000 + 999
    else:
        lo = (g // 2) * 1000 + 1000
        hi = (g // 2) * 1000 + 1499
    return lo, hi

def valid_peer_name(name):
    return bool(re.fullmatch(r"[A-Za-z]{1,15}", name))

def handle_register(fields, addr):
    if len(fields) != 5:
        send_reply(addr, "REGISTER", "FAILURE", "bad-format")
        return
    _, name, ip, mport_s, pport_s = fields
    try:
        mport = int(mport_s); pport = int(pport_s)
    except Exception:
        send_reply(addr, "REGISTER", "FAILURE", "bad-ports")
        return
    if not valid_peer_name(name):
        send_reply(addr, "REGISTER", "FAILURE", "bad-name")
        return
    if GROUP_NUM is not None:
        lo, hi = port_range_for_group(GROUP_NUM)
        if not (lo <= mport <= hi and lo <= pport <= hi):
            send_reply(addr, "REGISTER", "FAILURE", "port-out-of-range")
            return
    if name in peers:
        send_reply(addr, "REGISTER", "FAILURE", "name-duplicate")
        return
    for p in peers.values():
        if p['mport'] == mport or p['pport'] == pport:
            send_reply(addr, "REGISTER", "FAILURE", "port-duplicate")
            return
    peers[name] = {"ip": ip, "mport": mport, "pport": pport, "state": "Free"}
    send_reply(addr, "REGISTER", "SUCCESS", "")
    print(f"[manager] REGISTERED {name} ip={ip} mport={mport} pport={pport}")

def handle_setup_dht(fields, addr):
    if len(fields) != 4:
        send_reply(addr, "SETUP-DHT", "FAILURE", "bad-format")
        return
    _, name, n_str, yyyy = fields
    if name not in peers:
        send_reply(addr, "SETUP-DHT", "FAILURE", "not-registered")
        return
    try:
        n = int(n_str)
    except Exception:
        send_reply(addr, "SETUP-DHT", "FAILURE", "bad-n")
        return
    if n < 3:
        send_reply(addr, "SETUP-DHT", "FAILURE", "n-too-small")
        return
    if dht_state["exists"]:
        send_reply(addr, "SETUP-DHT", "FAILURE", "dht-already-exists")
        return
    free = [nm for nm, p in peers.items() if p['state'] == "Free"]
    if name not in free:
        send_reply(addr, "SETUP-DHT", "FAILURE", "leader-not-free")
        return
    if len(free) < n:
        send_reply(addr, "SETUP-DHT", "FAILURE", "not-enough-peers")
        return
    free.remove(name)
    selected = [name] + random.sample(free, n - 1)
    tuples = []
    for nm in selected:
        p = peers[nm]
        tuples.append(f"{nm},{p['ip']},{p['pport']}")
        peers[nm]['state'] = "InDHT"
    peers[name]['state'] = "Leader"
    dht_state["exists"]         = True
    dht_state["peers"]          = selected[:]
    dht_state["leader"]         = name
    dht_state["pending_action"] = "setup-wait-dht-complete"
    extra = ";".join(tuples)
    send_reply(addr, "SETUP-DHT", "SUCCESS", extra)
    print(f"[manager] SETUP-DHT leader={name} peers={selected}")

def handle_dht_complete(fields, addr):
    if len(fields) != 2:
        send_reply(addr, "DHT-COMPLETE", "FAILURE", "bad-format")
        return
    _, name = fields
    if dht_state["leader"] != name:
        send_reply(addr, "DHT-COMPLETE", "FAILURE", "not-leader")
        return
    dht_state["pending_action"] = None
    send_reply(addr, "DHT-COMPLETE", "SUCCESS", "")
    print(f"[manager] DHT-COMPLETE from {name}. Manager ready.")

def handle_query_dht(fields, addr):
    if len(fields) != 2:
        send_reply(addr, "QUERY-DHT", "FAILURE", "bad-format")
        return
    _, name = fields
    if name not in peers:
        send_reply(addr, "QUERY-DHT", "FAILURE", "not-registered")
        return
    if not dht_state["exists"]:
        send_reply(addr, "QUERY-DHT", "FAILURE", "no-dht")
        return
    if peers[name]['state'] != "Free":
        send_reply(addr, "QUERY-DHT", "FAILURE", "peer-not-free")
        return
    chosen = random.choice(dht_state["peers"])
    p = peers[chosen]
    extra = f"{chosen},{p['ip']},{p['pport']}"
    send_reply(addr, "QUERY-DHT", "SUCCESS", extra)
    print(f"[manager] QUERY-DHT by {name} -> directed to {chosen}")

def handle_leave_dht(fields, addr):
    if len(fields) != 2:
        send_reply(addr, "LEAVE-DHT", "FAILURE", "bad-format")
        return
    _, name = fields
    if not dht_state["exists"]:
        send_reply(addr, "LEAVE-DHT", "FAILURE", "no-dht")
        return
    if name not in peers or peers[name]['state'] not in ("InDHT", "Leader"):
        send_reply(addr, "LEAVE-DHT", "FAILURE", "peer-not-in-dht")
        return
    dht_state["pending_action"] = "leave-wait-dht-rebuilt"
    dht_state["churn_peer"]     = name
    send_reply(addr, "LEAVE-DHT", "SUCCESS", "")
    print(f"[manager] LEAVE-DHT from {name}. Waiting for dht-rebuilt.")

def handle_join_dht(fields, addr):
    if len(fields) != 2:
        send_reply(addr, "JOIN-DHT", "FAILURE", "bad-format")
        return
    _, name = fields
    if not dht_state["exists"]:
        send_reply(addr, "JOIN-DHT", "FAILURE", "no-dht")
        return
    if name not in peers or peers[name]['state'] != "Free":
        send_reply(addr, "JOIN-DHT", "FAILURE", "peer-not-free")
        return
    leader = dht_state["leader"]
    p = peers[leader]
    extra = f"{leader},{p['ip']},{p['pport']}"
    dht_state["pending_action"] = "join-wait-dht-rebuilt"
    dht_state["churn_peer"]     = name
    peers[name]['state']        = "InDHT"
    send_reply(addr, "JOIN-DHT", "SUCCESS", extra)
    print(f"[manager] JOIN-DHT from {name}. Leader={leader}. Waiting for dht-rebuilt.")

def handle_dht_rebuilt(fields, addr):
    if len(fields) != 3:
        send_reply(addr, "DHT-REBUILT", "FAILURE", "bad-format")
        return
    _, name, new_leader = fields
    if dht_state["churn_peer"] != name:
        send_reply(addr, "DHT-REBUILT", "FAILURE", "wrong-peer")
        return

    action = dht_state["pending_action"]

    if action == "leave-wait-dht-rebuilt":
        leaving = name
        if leaving in dht_state["peers"]:
            dht_state["peers"].remove(leaving)
        peers[leaving]['state'] = "Free"

    elif action == "join-wait-dht-rebuilt":
        joining = name
        if joining not in dht_state["peers"]:
            dht_state["peers"].append(joining)

    old_leader = dht_state["leader"]
    if old_leader and old_leader in peers and old_leader != new_leader:
        if peers[old_leader]['state'] == "Leader":
            peers[old_leader]['state'] = "InDHT"
    dht_state["leader"] = new_leader
    if new_leader in peers:
        peers[new_leader]['state'] = "Leader"

    dht_state["pending_action"] = None
    dht_state["churn_peer"]     = None
    send_reply(addr, "DHT-REBUILT", "SUCCESS", "")
    print(f"[manager] DHT-REBUILT by {name}. New leader={new_leader}. Peers={dht_state['peers']}")

def handle_deregister(fields, addr):
    if len(fields) != 2:
        send_reply(addr, "DEREGISTER", "FAILURE", "bad-format")
        return
    _, name = fields
    if name not in peers:
        send_reply(addr, "DEREGISTER", "FAILURE", "not-registered")
        return
    if peers[name]['state'] in ("InDHT", "Leader"):
        send_reply(addr, "DEREGISTER", "FAILURE", "peer-in-dht")
        return
    del peers[name]
    send_reply(addr, "DEREGISTER", "SUCCESS", "")
    print(f"[manager] DEREGISTERED {name}")

def handle_teardown_dht(fields, addr):
    if len(fields) != 2:
        send_reply(addr, "TEARDOWN-DHT", "FAILURE", "bad-format")
        return
    _, name = fields
    if not dht_state["exists"]:
        send_reply(addr, "TEARDOWN-DHT", "FAILURE", "no-dht")
        return
    if dht_state["leader"] != name:
        send_reply(addr, "TEARDOWN-DHT", "FAILURE", "not-leader")
        return
    dht_state["pending_action"] = "teardown-wait-teardown-complete"
    send_reply(addr, "TEARDOWN-DHT", "SUCCESS", "")
    print(f"[manager] TEARDOWN-DHT from leader={name}. Waiting for teardown-complete.")

def handle_teardown_complete(fields, addr):
    if len(fields) != 2:
        send_reply(addr, "TEARDOWN-COMPLETE", "FAILURE", "bad-format")
        return
    _, name = fields
    if dht_state["leader"] != name:
        send_reply(addr, "TEARDOWN-COMPLETE", "FAILURE", "not-leader")
        return
    for nm in dht_state["peers"]:
        if nm in peers:
            peers[nm]['state'] = "Free"
    dht_state["exists"]         = False
    dht_state["peers"]          = []
    dht_state["leader"]         = None
    dht_state["pending_action"] = None
    dht_state["churn_peer"]     = None
    send_reply(addr, "TEARDOWN-COMPLETE", "SUCCESS", "")
    print(f"[manager] TEARDOWN-COMPLETE. DHT destroyed. All peers now Free.")

ALWAYS_ALLOWED = {"REGISTER"}

while True:
    data, addr = sock.recvfrom(BUFSIZE)
    s      = data.decode().strip()
    fields = s.split('|')
    cmd    = fields[0].upper()

    if dht_state["pending_action"] is not None and cmd not in ALWAYS_ALLOWED:
        action = dht_state["pending_action"]
        allowed = None
        if action == "setup-wait-dht-complete":
            allowed = ("DHT-COMPLETE", dht_state["leader"])
        elif action in ("leave-wait-dht-rebuilt", "join-wait-dht-rebuilt"):
            allowed = ("DHT-REBUILT", dht_state["churn_peer"])
        elif action == "teardown-wait-teardown-complete":
            allowed = ("TEARDOWN-COMPLETE", dht_state["leader"])

        if allowed:
            exp_cmd, exp_peer = allowed
            if not (cmd == exp_cmd and len(fields) >= 2 and fields[1] == exp_peer):
                send_reply(addr, cmd, "FAILURE", "manager-busy")
                continue

    if   cmd == "REGISTER":          handle_register(fields, addr)
    elif cmd == "SETUP-DHT":         handle_setup_dht(fields, addr)
    elif cmd == "DHT-COMPLETE":      handle_dht_complete(fields, addr)
    elif cmd == "QUERY-DHT":         handle_query_dht(fields, addr)
    elif cmd == "LEAVE-DHT":         handle_leave_dht(fields, addr)
    elif cmd == "JOIN-DHT":          handle_join_dht(fields, addr)
    elif cmd == "DHT-REBUILT":       handle_dht_rebuilt(fields, addr)
    elif cmd == "DEREGISTER":        handle_deregister(fields, addr)
    elif cmd == "TEARDOWN-DHT":      handle_teardown_dht(fields, addr)
    elif cmd == "TEARDOWN-COMPLETE": handle_teardown_complete(fields, addr)
    else:
        send_reply(addr, "UNKNOWN", "FAILURE", "unknown-command")