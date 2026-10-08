# P2P Network - Communication & File Sharing

CSE 433 (Blockchain & Distributed Security Lab), University of Asia Pacific.

A lightweight peer-to-peer application in Python. Every peer is both a TCP server
and a TCP client; there is no central server. Peers exchange text messages and
binary files (images, audio, video, PDF, ZIP...) directly over TCP.

## Requirements
- Python 3.9+ (tkinter included)
- No third-party packages (see requirements.txt)

## Project Structure
| File | Responsibility |
|------|----------------|
| main.py | Tkinter GUI |
| p2p_node.py | Networking: server, connections, text, file transfer |
| protocol.py | Message framing and JSON encode/decode |
| downloads/ | Received files |

## Protocol
Every message: `[4-byte big-endian length][UTF-8 JSON payload]`
Types: `hello`, `hello_ack`, `text`, `file` (file metadata is followed by raw bytes in 64 KB chunks).

## Run
(TODO: Phase 8)