#!/usr/bin/env python3
import socket, sys, threading, csv, time, re

if len(sys.argv) != 3:
    print("Usage: python peer.py <manager-ip> <manager-port>")
    sys.exit(1)

MGR_IP = sys.argv[1]
MGR_PORT = int(sys.argv[2])
BUFSIZE = 65535

NAME = None
MPORT = None
PPORT = None

my_id = None
ring_n = None
peer_tuples = []
local_table = {}
record_count = 0

mgr_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
peer_sock = None

def send_to_manager(msg):
    mgr_sock.sendto(msg.encode(), (MGR_IP, MGR_PORT))

def send_to_peer(ip, port, msg):
    peer_sock.sendto(msg.encode(), (ip, int(port)))

def handle_mgr_receive_blocking(expected_cmd=None, timeout=5.0):
    mgr_sock.settimeout(timeout)
    try:
        data, _ = mgr_sock.recvfrom(BUFSIZE)
        return data.decode().strip()
    except socket.timeout:
        return None
    finally:
        mgr_sock.settimeout(None)

def peer_listener():
    global my_id, ring_n, peer_tuples, local_table, record_count
    while True:
        try:
            data, addr = peer_sock.recvfrom(BUFSIZE)
        except socket.timeout:
            continue
        except Exception as e:
            print(f"[{NAME}] peer_listener exception: {e}")
            continue
        s = data.decode().strip()
        parts = s.split('|')
        cmd = parts[0]
        if cmd == "SET-ID":
            _, id_s, n_s, tuple_list = parts
            my_id = int(id_s); ring_n = int(n_s)
            tuples = [t for t in tuple_list.split(';') if t]
            peer_tuples = []
            for t in tuples:
                nm, ip, pport = t.split(',')
                peer_tuples.append((nm, ip, int(pport)))
            print(f"[{NAME}] SET-ID id={my_id} n={ring_n} right={peer_tuples[(my_id+1)%ring_n][0]}")
        elif cmd == "STORE":
            if len(parts) < 4:
                continue
            target_id = int(parts[1]); pos = int(parts[2]); record = "|".join(parts[3:])
            if my_id is None:
                print(f"[{NAME}] Received STORE before SET-ID; discarding")
                continue
            if target_id == my_id:
                local_table.setdefault(pos, []).append(record)
                record_count += 1
                print(f"[{NAME}] STORED record at pos={pos} total_local={record_count}")
            else:
                right_idx = (my_id + 1) % ring_n
                nm, ip, pport = peer_tuples[right_idx]
                send_to_peer(ip, pport, s)
                print(f"[{NAME}] FORWARD STORE -> {nm} ({ip}:{pport}) target_id={target_id}")
        elif cmd == "COUNT-REQ":
            reqid = parts[1] if len(parts) > 1 else "0"
            reply = f"COUNT-REPLY|{reqid}|{NAME}|{record_count}"
            peer_sock.sendto(reply.encode(), addr)
            print(f"[{NAME}] COUNT-REQ answered reqid={reqid} count={record_count}")
        elif cmd == "COUNT-REPLY":
            print(f"[{NAME}] COUNT-REPLY raw: {s}")
        else:
            print(f"[{NAME}] unknown peer cmd: {s}")

def first_prime_above(x):
    def isprime(p):
        if p < 2: return False
        if p % 2 == 0:
            return p == 2
        r = int(p**0.5)
        for i in range(3, r+1, 2):
            if p % i == 0:
                return False
        return True
    y = x + 1
    while not isprime(y):
        y += 1
    return y

def leader_build_dht(tuples_str, yyyy):
    global my_id, ring_n, peer_tuples, local_table, record_count
    tuples = [t for t in tuples_str.split(';') if t]
    peer_list = []
    for t in tuples:
        nm, ip, pport = t.split(',')
        peer_list.append((nm, ip, int(pport)))
    n = len(peer_list)
    tuple_list_full = ";".join([f"{nm},{ip},{pport}" for nm,ip,pport in peer_list])
    peer_tuples = peer_list
    for i, (nm, ip, pport) in enumerate(peer_list):
        msg = f"SET-ID|{i}|{n}|{tuple_list_full}"
        send_to_peer(ip, pport, msg)
        print(f"[leader] SET-ID -> {nm} ({ip}:{pport}) id={i}")
    time.sleep(0.25)
    fname = f"details-{yyyy}.csv"
    try:
        with open(fname, newline='', encoding='utf-8', errors='ignore') as f:
            reader = list(csv.reader(f))
    except FileNotFoundError:
        print(f"[leader] dataset {fname} not found. Aborting DHT build.")
        return
    if len(reader) <= 1:
        print("[leader] dataset empty")
        return
    l = len(reader) - 1
    s = first_prime_above(2 * l)
    print(f"[leader] rows={l} s={s} n={n}")
    right = peer_list[1]
    for row in reader[1:]:
        try:
            event_id = int(row[0])
        except:
            event_id = abs(hash(",".join(row))) % (10**9)
        pos = event_id % s
        target_id = pos % n
        record_ser = ",".join(row)
        store_msg = f"STORE|{target_id}|{pos}|{record_ser}"
        send_to_peer(right[1], right[2], store_msg)
    print("[leader] injected all STORE messages into ring")
    time.sleep(0.35)
    temp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    temp_sock.bind(("", 0))
    temp_sock.settimeout(5.0)
    reqid = str(int(time.time()))
    replies = {}
    expected = set([nm for nm,_,_ in peer_list])
    temp_port = temp_sock.getsockname()[1]
    for nm, ip, pport in peer_list:
        msg = f"COUNT-REQ|{reqid}"
        temp_sock.sendto(msg.encode(), (ip, pport))
        print(f"[leader] COUNT-REQ -> {nm}")
    start = time.time()
    while expected and (time.time() - start) < 5.0:
        try:
            data, _ = temp_sock.recvfrom(BUFSIZE)
            s = data.decode().strip()
            parts = s.split('|')
            if parts[0] == "COUNT-REPLY" and len(parts) >= 4 and parts[1] == reqid:
                _, _, nm, cnt = parts[0], parts[1], parts[2], parts[3]
                replies[nm] = int(cnt)
                if nm in expected:
                    expected.remove(nm)
        except socket.timeout:
            break
        except Exception:
            continue
    for nm,_,_ in peer_list:
        if nm not in replies:
            replies[nm] = 0
    temp_sock.close()
    print("[leader] per-node stored counts:")
    for nm,_,_ in peer_list:
        print(f"  {nm}: {replies.get(nm,0)}")
    send_to_manager(f"DHT-COMPLETE|{NAME}")
    resp = handle_mgr_receive_blocking(timeout=5.0)
    print("[leader] manager reply:", resp)

print("peer started. First run: register")
while True:
    cmdline = input("cmd> ").strip()
    if not cmdline:
        continue
    parts = cmdline.split()
    cmd = parts[0].lower()
    if cmd == "register":
        if NAME is not None:
            print("already registered")
            continue
        name = input("peer-name (alpha <=15 chars): ").strip()
        ip = input("ip (use local host IP or 127.0.0.1): ").strip()
        try:
            mport = int(input("m-port (manager comms): ").strip())
            pport = int(input("p-port (peer comms): ").strip())
        except:
            print("bad ports")
            continue
        try:
            mgr_sock.close()
            mgr_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            mgr_sock.bind(("", mport))
            peer_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            peer_sock.bind(("", pport))
        except Exception as e:
            print("socket bind error:", e)
            continue
        NAME = name; MPORT = mport; PPORT = pport
        msg = f"REGISTER|{NAME}|{ip}|{MPORT}|{PPORT}"
        send_to_manager(msg)
        print("[peer] REGISTER sent, waiting for manager reply...")
        reply = handle_mgr_receive_blocking(timeout=5.0)
        print("[peer] manager reply:", reply)
        if reply and reply.startswith("MANAGER-REPLY|REGISTER|SUCCESS"):
            t = threading.Thread(target=peer_listener, daemon=True)
            t.start()
        else:
            print("register failed")
            NAME = None
    elif cmd == "setup-dht":
        if NAME is None:
            print("must register first")
            continue
        if len(parts) != 3:
            print("Usage: setup-dht <n> <YYYY>")
            continue
        n = parts[1]; yyyy = parts[2]
        msg = f"SETUP-DHT|{NAME}|{n}|{yyyy}"
        send_to_manager(msg)
        print("[peer] SETUP-DHT sent, waiting for manager reply...")
        reply = handle_mgr_receive_blocking(timeout=10.0)
        print("[peer] manager reply:", reply)
        if reply and reply.startswith("MANAGER-REPLY|SETUP-DHT|SUCCESS|"):
            tuples_str = reply.split('|',3)[3]
            leader_build_dht(tuples_str, yyyy)
        else:
            print("setup-dht failed or not leader")
    else:
        print("supported commands: register, setup-dht")