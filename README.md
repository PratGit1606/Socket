# DHT Milestone

A distributed hash table implementation over UDP, supporting peer registration, ring-based DHT setup, and distributed record storage.

---

## Files Included

| File | Description |
|------|-------------|
| `manager.py` | UDP manager program |
| `peer.py` | UDP peer program |
| `details-1950.csv` | Dataset used in milestone demo |
| `design_document.pdf` | Milestone design document |
| `README.md` | This file |

---

## How to Run (Single Machine Testing)

### 1. Start the Manager

```bash
python3 manager.py 20000
```

### 2. Start Three Peers (in separate terminals)

```bash
python3 peer.py 127.0.0.1 20000
```

At the prompt, register each peer with the following details:

| Peer Name | DHT Port | Query Port |
|-----------|----------|------------|
| `alphanet` | `21000` | `31000` |
| `betanet` | `21001` | `31001` |
| `gammanet` | `21002` | `31002` |

**Example registration input:**
```
register
alphanet
127.0.0.1
21000
31000
```

### 3. Build the DHT

On the **leader peer**, run:

```
setup-dht 3 1950
```

**Expected behavior:**
- Leader assigns IDs to peers using `SET-ID`
- `STORE` messages circulate around the ring
- Each peer stores its assigned records
- Leader prints per-node record counts
- Leader sends `DHT-COMPLETE`
- Manager acknowledges

---

## Two-Host Demo Notes

For grading across two machines:

- Run the **manager** on Host A
- Run at least one **peer** on Host B
- Use the manager's **actual IP** (not `127.0.0.1`) when registering remote peers
- Ensure the firewall allows the chosen ports

---

## Common Errors

| Error | Cause |
|-------|-------|
| `bad-name` | Peer name must be alphabetic only |
| `port-duplicate` | Each peer must use unique ports |
| `dataset not found` | Ensure the CSV file is in the leader's working directory |
| `manager-busy` | Wait until `DHT-COMPLETE` is received before retrying |

---

## Author

**Pratham Hegde
