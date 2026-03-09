DHT Milestone — README
Files Included

manager.py — UDP manager program

peer.py — UDP peer program

details-1950.csv — dataset used in milestone demo

design_document.pdf — milestone design document

README.md — this file

How to Run (Single Machine Testing)

1. Start Manager
   python3 manager.py 20000
2. Start Three Peers (in separate terminals)

Example:

python3 peer.py 127.0.0.1 20000

At the prompt:

register
alphanet
127.0.0.1
21000
31000

Repeat for:

betanet → 21001 / 31001

gammanet → 21002 / 31002

3. Build the DHT

On the leader peer:

setup-dht 3 1950

Expected behavior:

Leader assigns IDs using SET-ID

STORE messages circulate around ring

Each peer stores assigned records

Leader prints per-node record counts

Leader sends DHT-COMPLETE

Manager acknowledges

Two-Host Demo Notes

For grading:

Run manager on Host A

Run at least one peer on Host B

Use manager’s actual IP (not 127.0.0.1) when registering remote peers

Ensure firewall allows chosen ports

Common Errors

bad-name → name must be alphabetic

port-duplicate → use unique ports

dataset not found → ensure CSV is in leader's directory

manager-busy → wait until DHT-COMPLETE

Author

Pratham Hegde
