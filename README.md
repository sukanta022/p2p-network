# P2P Network - Communication & File Sharing

CSE 433 (Blockchain & Distributed Security Lab), University of Asia Pacific.

A lightweight peer-to-peer application in Python. Every peer is both a TCP server
and a TCP client; there is no central server. Peers exchange text messages and
binary files (images, audio, video, PDF, ZIP...) directly over TCP.

## Requirements
- Python 3.9+ (with tkinter)
- No third-party packages (see `requirements.txt`)

## Project Structure
| File | Responsibility |
|------|----------------|
| `main.py` | Tkinter GUI and user interaction |
| `p2p_node.py` | Networking: TCP server, client connections, handshake, threads, text and file transfer |
| `protocol.py` | Message framing and JSON encoding/decoding |
| `downloads/` | Received files are saved here |

## Architecture
```
User Interface (main.py)
        |
   P2P Node (p2p_node.py)   = TCP Server + TCP Client
        |
   Protocol (protocol.py)
        |
   TCP Socket  <-->  Other Peer
```
- **Server role:** a listening socket on `0.0.0.0:<port>`; the accept loop runs in its own thread and starts one handler thread per incoming connection.
- **Client role:** `connect_to_peer(ip, port)` opens an outgoing TCP connection and performs the handshake.
- **Peer registry:** connected peers are stored in a dictionary (`peer_id -> Peer`) protected by a lock.
- Each connected peer has its own receive thread.

## Concurrency Model
| Thread | Purpose |
|--------|---------|
| GUI / main thread | Tkinter event loop, never blocks on network I/O |
| Accept thread | `accept()` loop on the listening socket |
| Handshake thread (per incoming connection) | Runs hello/hello_ack, then becomes the receive thread |
| Receive thread (per peer) | Blocking `recv` loop for one peer |

- Shared state (`peers` dict) is protected by a `threading.Lock`.
- Each peer has its own `send_lock`, so concurrent sends from several threads cannot interleave the bytes of two frames.
- If two peers connect to each other at the same time, the connection initiated by
  the peer with the lower `peer_id` is kept; both sides apply the same rule.
- Closing a connection uses `shutdown()` then `close()` so the blocked receive thread wakes up.

## Protocol
Every message is framed as:

```
[4-byte big-endian length][UTF-8 JSON payload]
```

TCP is a byte stream and does not preserve message boundaries, so the receiver
reads exactly 4 bytes, then exactly `length` bytes (`recv_exact`).

Message types: `hello`, `hello_ack`, `text`, `file`.

### Handshake
```
Initiator                              Acceptor
  connect() ---------------------------> accept()
  {"type":"hello","peer_id":"a83f21c4",
   "peer_name":"Alice","port":5000} ---->
  <---- {"type":"hello_ack","peer_id":"92bd71e3",
         "peer_name":"Bob","port":5001}
```
- `peer_id` is a random 8-character ID that identifies a peer.
- `port` is the peer's **listening** port (an outgoing connection uses a random ephemeral port).
- Duplicate connections, self-connections and invalid handshakes are rejected.
- The handshake has a 5-second timeout.

### Text message
```json
{"type": "text", "sender_id": "a83f21c4", "sender_name": "Alice", "message": "Hello!"}
```
- Sent to the selected peer (`send_text`) or to all connected peers (`broadcast_text`).
- The receiver shows the name registered during the handshake, not the name inside the message.
- Empty messages and messages longer than 10,000 characters are rejected. Malformed
  or unknown messages are ignored with a warning; they never crash the receiver.
- Text is UTF-8 encoded, so non-English text (e.g. Bengali) works.

### File transfer (planned)
1. Send framed JSON metadata: `{"type":"file","filename":...,"filesize":...}`
2. Send the raw file bytes in 64 KB chunks.
3. The receiver reads exactly `filesize` bytes and saves them to `downloads/`.

## Development Progress
- [x] Phase 1: message framing (`protocol.py`)
- [x] Phase 2: TCP server and client
- [x] Phase 3: HELLO handshake and peer tracking
- [x] Phase 4: multi-peer connection manager
- [x] Phase 5: text messaging
- [ ] Phase 6: chunked file transfer
- [ ] Phase 7: Tkinter GUI
- [ ] Phase 8: error handling, final testing, screenshots

## Running (development, command line)
Self-tests:
```
python protocol.py
python p2p_node.py selftest      # handshake and errors
python p2p_node.py selftest4     # multi-peer and simultaneous connect
python p2p_node.py selftest5     # text messaging
```
Two peers on one computer (two terminals):
```
python p2p_node.py node Alice 5000
python p2p_node.py node Bob 5001 127.0.0.1 5000
```
Commands inside `node` mode: `peers`, `send <peer_id> <text>`, `broadcast <text>`,
`disconnect <peer_id>`, `quit`.

## Running the GUI, connecting peers, transferring files, screenshots
(TODO: Phase 7-8)