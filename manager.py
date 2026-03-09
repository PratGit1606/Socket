#!/usr/bin/env python3
import socket, sys, random, re

if len(sys.argv) != 2:
    print("Usage: python manager.py <port>")
    sys.exit(1)

GROUP_NUM = None

MGR_PORT = int(sys.argv[1])
BUFSIZE = 65535

peers = {} 
dht_state = {"exists": False, "peers": [], "leader": None, "pending_action": None}

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.bind(("", MGR_PORT))
print(f"[manager] listening on UDP port {MGR_PORT}")

def send_reply(addr, cmd, code, extra=""):
    msg = f"MANAGER-REPLY|{cmd}|{code}|{extra}"
    sock.sendto(msg.encode(), addr)

def port_range_for_group(g):
    if g % 2 == 1:
        lo = (g//2) * 1000 + 500
        hi = (g//2) * 1000 + 999
    else:
        lo = (g//2) * 1000 + 1000
        hi = (g//2) * 1000 + 1499
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
    except:
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
    print(f"[manager] registered {name} {ip} mport={mport} pport={pport}")

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
    except:
        send_reply(addr, "SETUP-DHT", "FAILURE", "bad-n")
        return
    if n < 3:
        send_reply(addr, "SETUP-DHT", "FAILURE", "n-too-small")
        return
    free = [nm for nm,p in peers.items() if p['state'] == "Free"]
    if name not in free:
        send_reply(addr, "SETUP-DHT", "FAILURE", "leader-not-free")
        return
    if len(free) < n:
        send_reply(addr, "SETUP-DHT", "FAILURE", "not-enough-peers")
        return
    if dht_state["exists"]:
        send_reply(addr, "SETUP-DHT", "FAILURE", "dht-already-exists")
        return
    free.remove(name)
    selected = [name] + random.sample(free, n-1)
    tuples = []
    for nm in selected:
        p = peers[nm]
        tuples.append(f"{nm},{p['ip']},{p['pport']}")
        peers[nm]['state'] = "InDHT"
    peers[name]['state'] = "Leader"
    dht_state["exists"] = True
    dht_state["peers"] = selected.copy()
    dht_state["leader"] = name
    dht_state["pending_action"] = "setup-wait-dht-complete"
    extra = ";".join(tuples)
    send_reply(addr, "SETUP-DHT", "SUCCESS", extra)
    print(f"[manager] SETUP-DHT: leader={name} peers={selected}")

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
    print(f"[manager] DHT-COMPLETE received from {name}. manager now accepts commands.")

while True:
    data, addr = sock.recvfrom(BUFSIZE)
    s = data.decode().strip()
    fields = s.split('|')
    cmd = fields[0].upper()
    if dht_state["pending_action"] is not None:
        if not (cmd == "DHT-COMPLETE" and len(fields) >= 2 and fields[1] == dht_state["leader"]):
            # send manager-busy for any incoming other message
            send_reply(addr, cmd, "FAILURE", "manager-busy")
            continue
    if cmd == "REGISTER":
        handle_register(fields, addr)
    elif cmd == "SETUP-DHT":
        handle_setup_dht(fields, addr)
    elif cmd == "DHT-COMPLETE":
        handle_dht_complete(fields, addr)
    else:
        send_reply(addr, "UNKNOWN", "FAILURE", "unknown-command")