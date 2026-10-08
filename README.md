# P2P Network - Communication & File Sharing

CSE 433 (Blockchain & Distributed Security Lab), University of Asia Pacific.

A lightweight peer-to-peer application in Python. Every peer is both a TCP server
and a TCP client; there is no central server. Peers exchange text messages and
binary files (images, audio, video, PDF, ZIP...) directly over TCP, on one
computer or across a local network (LAN / Wi-Fi), with a Tkinter GUI.

## Requirements
- Python 3.9+ (with tkinter)
- No third-party packages (see `requirements.txt`)

## Project Structure
| File | Responsibility |
|------|----------------|
| `main.py` | Tkinter GUI and user interaction |
| `p2p_node.py` | Networking: TCP server, client connections, handshake, threads, text and file transfer |
| `protocol.py` | Message framing, JSON encoding/decoding, file chunk helpers |
| `downloads/` | Received files are saved here |

## How to Run the GUI
```
python main.py
```
1. **My Peer:** enter a name and a port (e.g. 5000) and press **Start Peer**. The status line shows your peer ID and your LAN IP.
2. **Connect to Peer:** enter the other peer's IP and port and press **Connect**. Use `127.0.0.1` for a peer on the same computer.
3. The peer appears in **Connected Peers**. Select it to choose who receives your messages and files.
4. **Send Text:** type a message and press **Send** (or Enter). Tick **Send to all peers** to send to everyone.
5. **Send File:** press **Choose File & Send**. A progress bar shows the transfer. Received files are saved in `downloads/` (**Open Downloads Folder** opens it).
6. Press **Stop** to shut the peer down.

### Two peers on one computer
Run `python main.py` twice. Start the first as `Alice` / `5000` and the second as `Bob` / `5001`, then connect Bob to `127.0.0.1` port `5000`.

### Two computers on the same Wi-Fi / LAN
Start a peer on each computer. On the second computer connect to the first computer's IP (shown in its status line) and port. If the connection times out, allow Python through the firewall for **private networks**.

## GUI Layout
| Section | Purpose |
|---------|---------|
| My Peer | Name, port, Start / Stop |
| Connect to Peer | Remote IP, remote port, Connect |
| Connected Peers | Active peers (name, ID, address); Disconnect button |
| Communication / Event Log | Status events, incoming and outgoing messages, file notices |
| Send Text | Message box, Send, "Send to all peers" |
| Send File | Choose file and send, progress bar, open downloads folder |

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
| GUI / main thread | Tkinter event loop; never blocks on network I/O |
| Accept thread | `accept()` loop on the listening socket |
| Handshake thread (per incoming connection) | Runs hello/hello_ack, then becomes the receive thread |
| Receive thread (per peer) | Blocking `recv` loop for one peer; also receives file bytes |
| File sender thread (per transfer) | Sends one file so the GUI never freezes |
| Connect / send worker threads | Blocking connect and text sends triggered from the GUI |

- Shared state (`peers` dict) is protected by a `threading.Lock`.
- Each peer has its own `send_lock`. It is held for a whole message, and for a whole
  file transfer, so bytes of different messages can never interleave.
- If two peers connect to each other at the same time, the connection initiated by
  the peer with the lower `peer_id` is kept; both sides apply the same rule.
- Closing a connection uses `shutdown()` then `close()` so the blocked receive thread wakes up.
- **Tkinter is not thread-safe.** Network threads never touch widgets: they put events
  into a `queue.Queue`, and the GUI thread drains the queue every 100 ms with
  `root.after()`. Transfer progress is a shared variable that the GUI polls.

## Protocol
Every JSON message is framed as:

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

### File transfer
```
Sender                                            Receiver
[4-byte len][{"type":"file","filename":"a.pdf",
              "filesize":1234567, ...}]  ------>  reads metadata
[64 KB][64 KB]...[last chunk]  (exactly 1234567 bytes, not framed)
                                         ------>  reads exactly 1234567 bytes
                                                  saves to downloads/a.pdf
```
- File bytes are sent raw in 64 KB chunks (`64 * 1024`), so memory use stays constant for any file size.
- The receiver reads exactly `filesize` bytes and never more, so the next message's header is not consumed.
- The sender holds the peer's `send_lock` for the whole transfer.
- **Safety on the receiving side:**
  - the filename is sanitized (directories removed, invalid characters replaced), so `..\..\x` cannot escape `downloads/`;
  - existing files are never overwritten (`name (1).ext`);
  - an invalid `filesize` (negative or above 4 GB) disconnects the sender;
  - a disk error discards the remaining bytes to stay in sync;
  - an interrupted transfer deletes the partial file.
- Sender-side errors (missing file, unreadable file, disconnected peer) are reported without crashing.

## Development Progress
- [x] Phase 1: message framing (`protocol.py`)
- [x] Phase 2: TCP server and client
- [x] Phase 3: HELLO handshake and peer tracking
- [x] Phase 4: multi-peer connection manager
- [x] Phase 5: text messaging
- [x] Phase 6: chunked file transfer
- [x] Phase 7: Tkinter GUI
- [ ] Phase 8: error handling, final testing, screenshots

## Command-line Self-tests (development)
```
python protocol.py
python p2p_node.py selftest      # handshake and errors
python p2p_node.py selftest4     # multi-peer and simultaneous connect
python p2p_node.py selftest5     # text messaging
python p2p_node.py selftest6     # file transfer (SHA-256 verified)
```
Headless peer: `python p2p_node.py node Alice 5000` (commands: `peers`, `send`, `broadcast`,
`sendfile`, `disconnect`, `quit`; type the real peer ID without `<` `>`).

## Screenshots
(TODO: Phase 8)