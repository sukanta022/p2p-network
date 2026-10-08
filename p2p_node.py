"""p2p_node.py - Core network engine (Phase 5: handshake + multi-peer manager + text messaging)."""
import os
import socket
import sys
import threading
import time
import uuid
from dataclasses import dataclass, field

from protocol import (MSG_FILE, MSG_HELLO, MSG_HELLO_ACK, MSG_TEXT, ConnectionClosed,
                      ProtocolError, make_hello, make_hello_ack, make_text,
                      recv_message, send_message)

CONNECT_TIMEOUT = 5      # seconds for TCP connect()
HANDSHAKE_TIMEOUT = 5    # seconds to complete hello/hello_ack
ACCEPT_POLL = 1.0        # accept() wakes every second to check self.running
MAX_TEXT_LENGTH = 10_000 # characters per text message


class PeerConnectionError(Exception):
    """Raised when we cannot connect to a remote peer."""


class SendError(Exception):
    """Raised when a message cannot be sent."""


def validate_port(port) -> int:
    try:
        port = int(port)
    except (TypeError, ValueError):
        raise ValueError("Port must be a number")
    if not 1 <= port <= 65535:
        raise ValueError("Port must be between 1 and 65535")
    return port


@dataclass(eq=False)                         # eq=False: compare peers by identity
class Peer:
    peer_id: str
    name: str
    ip: str
    port: int                                # the peer's LISTENING port (from hello)
    sock: socket.socket
    initiated: bool = False                  # True if WE opened this connection
    closed: bool = False                     # True once we deliberately close it
    send_lock: threading.Lock = field(default_factory=threading.Lock)

    def label(self) -> str:
        return f"{self.name} [{self.peer_id}] {self.ip}:{self.port}"


class P2PNode:
    def __init__(self, name: str, port, host: str = "0.0.0.0",
                 on_event=None, on_peers_changed=None, on_text_received=None):
        name = (name or "").strip()
        if not name:
            raise ValueError("Peer name cannot be empty")
        self.name = name[:50]
        self.port = validate_port(port)
        self.host = host
        self.peer_id = uuid.uuid4().hex[:8]
        self.on_event = on_event or print                      # GUI will replace this later
        self.on_peers_changed = on_peers_changed or (lambda: None)
        self.on_text_received = on_text_received or (lambda peer, text: None)
        self.server_socket = None
        self.running = False
        self._accept_thread = None
        self.peers = {}                                        # peer_id -> Peer
        self._peers_lock = threading.Lock()                    # protects self.peers

    def _log(self, text: str) -> None:
        self.on_event(f"[{self.name}] {text}")

    # ---------- Server role ----------

    def start(self) -> None:
        if self.running:
            raise RuntimeError("Peer is already running")
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if os.name != "nt":
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind((self.host, self.port))
            sock.listen()
        except OSError as e:
            sock.close()
            raise OSError(f"Cannot listen on port {self.port}: {e.strerror or e}") from e
        sock.settimeout(ACCEPT_POLL)
        self.server_socket = sock
        self.running = True
        self._accept_thread = threading.Thread(target=self._accept_loop, daemon=True)
        self._accept_thread.start()
        self._log(f"Started. ID={self.peer_id}, listening on {self.host}:{self.port}")

    def _accept_loop(self) -> None:
        server = self.server_socket
        while self.running:
            try:
                conn, addr = server.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            self._log(f"Incoming connection from {addr[0]}:{addr[1]}")
            threading.Thread(target=self._handle_incoming,
                             args=(conn, addr), daemon=True).start()

    def _handle_incoming(self, conn: socket.socket, addr) -> None:
        """Acceptor side of the handshake: expect hello, reply hello_ack."""
        peer = None
        try:
            conn.settimeout(HANDSHAKE_TIMEOUT)
            hello = recv_message(conn)
            pid, name, port = self._parse_handshake(hello, MSG_HELLO)
            peer = Peer(pid, name, addr[0], port, conn)
            if not self._register_peer(peer):
                raise ProtocolError("duplicate or self connection rejected")
            send_message(conn, make_hello_ack(self.peer_id, self.name, self.port))
            conn.settimeout(None)
        except socket.timeout:
            self._log(f"[ERROR] Handshake with {addr[0]}:{addr[1]} timed out")
            self._drop(peer, conn)
            return
        except (ProtocolError, OSError) as e:
            self._log(f"[ERROR] Handshake with {addr[0]}:{addr[1]} failed: {e}")
            self._drop(peer, conn)
            return
        self._log(f"Peer connected: {peer.label()}")
        self._receive_loop(peer)

    def stop(self) -> None:
        self.running = False
        if self.server_socket:
            try:
                self.server_socket.close()
            except OSError:
                pass
            self.server_socket = None
        with self._peers_lock:
            peers = list(self.peers.values())
            self.peers.clear()
        for p in peers:
            p.closed = True
            self._close_socket(p.sock)
        t = self._accept_thread
        if t and t is not threading.current_thread():
            t.join(timeout=ACCEPT_POLL * 2)
        self._log("Stopped")
        self._notify_peers_changed()

    # ---------- Client role ----------

    def _tcp_connect(self, ip: str, port) -> socket.socket:
        """Plain TCP connect with validation and friendly errors (Phase 2)."""
        try:
            port = validate_port(port)
        except ValueError as e:
            raise PeerConnectionError(str(e)) from e
        if not str(ip).strip():
            raise PeerConnectionError("IP address is empty")

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(CONNECT_TIMEOUT)
        try:
            sock.connect((ip.strip(), port))
        except socket.gaierror:
            sock.close()
            raise PeerConnectionError(f"Invalid IP address or host: {ip}")
        except socket.timeout:
            sock.close()
            raise PeerConnectionError("Connection timed out")
        except ConnectionRefusedError:
            sock.close()
            raise PeerConnectionError("Connection refused (is the peer running?)")
        except OSError as e:
            sock.close()
            raise PeerConnectionError(e.strerror or str(e)) from e
        return sock

    def connect_to_peer(self, ip: str, port) -> Peer:
        """Initiator side: TCP connect, send hello, wait for hello_ack, register."""
        if not self.running:
            raise PeerConnectionError("Start your peer before connecting")
        sock = self._tcp_connect(ip, port)
        try:
            sock.settimeout(HANDSHAKE_TIMEOUT)
            send_message(sock, make_hello(self.peer_id, self.name, self.port))
            ack = recv_message(sock)
            pid, name, rport = self._parse_handshake(ack, MSG_HELLO_ACK)
            peer = Peer(pid, name, sock.getpeername()[0], rport, sock, initiated=True)
            if not self._register_peer(peer):
                raise ProtocolError("already connected to this peer (or yourself)")
            sock.settimeout(None)
        except socket.timeout:
            sock.close()
            raise PeerConnectionError("Handshake timed out")
        except ConnectionClosed:
            sock.close()
            raise PeerConnectionError(
                "Peer closed the connection during handshake "
                "(already connected, or connecting to yourself?)")
        except ProtocolError as e:
            sock.close()
            raise PeerConnectionError(f"Handshake failed: {e}") from e
        except OSError as e:
            sock.close()
            raise PeerConnectionError(e.strerror or str(e)) from e

        self._log(f"Peer connected: {peer.label()}")
        threading.Thread(target=self._receive_loop, args=(peer,), daemon=True).start()
        return peer

    # ---------- Peer registry ----------

    def _parse_handshake(self, msg: dict, expected_type: str):
        if msg.get("type") != expected_type:
            raise ProtocolError(f"expected '{expected_type}', got '{msg.get('type')}'")
        pid, name, port = msg.get("peer_id"), msg.get("peer_name"), msg.get("port")
        if not isinstance(pid, str) or not pid or not isinstance(name, str) or not name.strip():
            raise ProtocolError("malformed handshake")
        try:
            port = validate_port(port)
        except ValueError:
            raise ProtocolError("invalid port in handshake")
        return pid, name.strip()[:50], port

    def _is_preferred(self, peer: Peer) -> bool:
        """Tie-breaker when two connections exist between the same two peers:
        keep the one initiated by the peer with the LOWER peer_id.
        Both sides apply the same rule, so both keep the SAME connection."""
        initiator_id = self.peer_id if peer.initiated else peer.peer_id
        return initiator_id == min(self.peer_id, peer.peer_id)

    def _register_peer(self, peer: Peer) -> bool:
        if peer.peer_id == self.peer_id:
            return False                                  # connecting to ourselves
        old = None
        with self._peers_lock:                            # check + insert/replace is atomic
            existing = self.peers.get(peer.peer_id)
            if existing is not None:
                if self._is_preferred(peer) and not self._is_preferred(existing):
                    old = existing                        # new one wins: replace old
                else:
                    return False                          # keep existing, reject new
            self.peers[peer.peer_id] = peer
        if old:                                           # close outside the lock
            old.closed = True
            self._close_socket(old.sock)
            self._log(f"Duplicate connection with {peer.name} resolved")
        self._notify_peers_changed()
        return True

    def _remove_peer(self, peer: Peer) -> None:
        peer.closed = True
        removed = False
        with self._peers_lock:
            if self.peers.get(peer.peer_id) is peer:      # only if THIS connection is registered
                del self.peers[peer.peer_id]
                removed = True
        self._close_socket(peer.sock)
        if removed:
            if self.running:
                self._log(f"Peer disconnected: {peer.name} [{peer.peer_id}]")
            self._notify_peers_changed()

    def _notify_peers_changed(self) -> None:
        # NOTE: runs on a network thread. The Tkinter GUI must hand over via root.after().
        try:
            self.on_peers_changed()
        except Exception as e:
            self._log(f"[ERROR] peers-changed callback failed: {e}")

    def _drop(self, peer, conn) -> None:
        """Cleanup after a failed handshake."""
        if peer:
            self._remove_peer(peer)
        self._close_socket(conn)

    @staticmethod
    def _close_socket(sock: socket.socket) -> None:
        try:
            sock.shutdown(socket.SHUT_RDWR)      # wakes any thread blocked in recv()
        except OSError:
            pass
        try:
            sock.close()
        except OSError:
            pass

    def list_peers(self) -> list:
        with self._peers_lock:
            return list(self.peers.values())

    def get_peer(self, peer_id: str):
        with self._peers_lock:
            return self.peers.get(peer_id)

    def disconnect_peer(self, peer_id: str) -> bool:
        """Close the connection to one peer. Returns False if no such peer."""
        peer = self.get_peer(peer_id)
        if peer is None:
            return False
        self._remove_peer(peer)       # shutdown() wakes that peer's receive thread
        return True

    # ---------- Sending (Phase 5) ----------

    @staticmethod
    def _check_text(text) -> None:
        if not isinstance(text, str) or not text.strip():
            raise SendError("Cannot send an empty message")
        if len(text) > MAX_TEXT_LENGTH:
            raise SendError(f"Message too long (max {MAX_TEXT_LENGTH} characters)")

    def _send_to_peer(self, peer: Peer, message: dict) -> None:
        """Send one framed message to one peer; send_lock keeps frames from interleaving."""
        try:
            with peer.send_lock:
                send_message(peer.sock, message)
        except ProtocolError as e:                  # e.g. message too large; nothing was sent
            raise SendError(str(e)) from e
        except OSError as e:                        # connection broken
            self._remove_peer(peer)
            raise SendError(f"Connection to {peer.name} lost: {e.strerror or e}") from e

    def send_text(self, peer_id: str, text: str) -> None:
        """Send a text message to ONE connected peer."""
        self._check_text(text)
        peer = self.get_peer(peer_id)
        if peer is None:
            raise SendError("Peer is not connected")
        self._send_to_peer(peer, make_text(self.peer_id, self.name, text))

    def broadcast_text(self, text: str) -> tuple:
        """Send a text message to ALL connected peers. Returns (sent_count, failed_names)."""
        self._check_text(text)
        sent, failed = 0, []
        for peer in self.list_peers():              # list_peers() returns a safe copy
            try:
                self._send_to_peer(peer, make_text(self.peer_id, self.name, text))
                sent += 1
            except SendError:
                failed.append(peer.name)            # one bad peer must not stop the others
        return sent, failed

    # ---------- Receiving ----------

    def _receive_loop(self, peer: Peer) -> None:
        try:
            while True:
                msg = recv_message(peer.sock)
                self._dispatch(peer, msg)
        except ConnectionClosed:
            pass
        except ProtocolError as e:
            if self.running and not peer.closed:
                self._log(f"[ERROR] Protocol error from {peer.name}: {e}")
        except OSError as e:
            if self.running and not peer.closed:
                self._log(f"[ERROR] Socket error with {peer.name}: {e}")
        finally:
            self._remove_peer(peer)

    def _dispatch(self, peer: Peer, msg: dict) -> None:
        mtype = msg.get("type")
        if mtype == MSG_TEXT:
            self._handle_text(peer, msg)
        elif mtype == MSG_FILE:
            self._log(f"{peer.name} sent a file message (file transfer arrives in Phase 6)")
        else:
            self._log(f"[WARN] Ignoring unknown message type '{mtype}' from {peer.name}")

    def _handle_text(self, peer: Peer, msg: dict) -> None:
        text = msg.get("message")
        if not isinstance(text, str) or not text.strip():
            self._log(f"[WARN] Ignoring malformed text message from {peer.name}")
            return
        if len(text) > MAX_TEXT_LENGTH:
            self._log(f"[WARN] Ignoring over-long text message from {peer.name}")
            return
        # Use the identity registered at handshake, not the sender_name inside the message.
        self._log(f"{peer.name} [{peer.peer_id}]: {text}")
        try:
            self.on_text_received(peer, text)
        except Exception as e:
            self._log(f"[ERROR] text callback failed: {e}")


# ---------- Manual / self tests ----------

def _selftest() -> None:
    alice = P2PNode("Alice", 5000)
    bob = P2PNode("Bob", 5001)
    alice.start()
    bob.start()
    time.sleep(0.2)

    print("\n--- 1) Handshake: Bob connects to Alice ---")
    peer = bob.connect_to_peer("127.0.0.1", 5000)
    print("Bob received ack from:", peer.label())
    time.sleep(0.3)
    print("Alice's peers:", [p.label() for p in alice.list_peers()])
    print("Bob's peers:  ", [p.label() for p in bob.list_peers()])

    print("\n--- 2) Duplicate connection ---")
    try:
        bob.connect_to_peer("127.0.0.1", 5000)
    except PeerConnectionError as e:
        print("[ERROR] Connection failed:", e)

    print("\n--- 3) Connecting to yourself ---")
    try:
        alice.connect_to_peer("127.0.0.1", 5000)
    except PeerConnectionError as e:
        print("[ERROR] Connection failed:", e)

    print("\n--- 4) Client skips hello (sends text first) ---")
    raw = socket.create_connection(("127.0.0.1", 5000))
    send_message(raw, make_text("deadbeef", "Mallory", "hi"))
    time.sleep(0.3)
    raw.close()

    print("\n--- 5) Other errors ---")
    for ip, port in [("127.0.0.1", 5999), ("999.1.1.1", 5000), ("127.0.0.1", "abc")]:
        try:
            bob.connect_to_peer(ip, port)
        except PeerConnectionError as e:
            print(f"[ERROR] Connection failed ({ip!r}:{port!r}): {e}")

    print("\n--- 6) Bob disconnects ---")
    bob.stop()
    time.sleep(0.5)
    print("Alice's peers:", [p.label() for p in alice.list_peers()])
    alice.stop()


def _selftest_multi() -> None:
    a, b, c = P2PNode("A", 5100), P2PNode("B", 5101), P2PNode("C", 5102)
    for n in (a, b, c):
        n.start()
    time.sleep(0.2)

    def names(n):
        return sorted(p.name for p in n.list_peers())

    print("\n--- 1) Three peers, A->B, A->C, B->C ---")
    a.connect_to_peer("127.0.0.1", 5101)
    a.connect_to_peer("127.0.0.1", 5102)
    b.connect_to_peer("127.0.0.1", 5102)
    time.sleep(0.3)
    for n in (a, b, c):
        print(f"{n.name} peers:", names(n))

    print("\n--- 2) Simultaneous connect D<->E (race) ---")
    d, e = P2PNode("D", 5103), P2PNode("E", 5104)
    d.start()
    e.start()
    time.sleep(0.2)
    barrier = threading.Barrier(2)

    def go(node, port):
        barrier.wait()                       # both start at the same instant
        try:
            node.connect_to_peer("127.0.0.1", port)
        except PeerConnectionError as err:
            print(f"   ({node.name}) {err}")

    ts = [threading.Thread(target=go, args=(d, 5104)),
          threading.Thread(target=go, args=(e, 5103))]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    time.sleep(0.5)
    print("D peers:", names(d), "| E peers:", names(e))
    dp, ep = d.list_peers(), e.list_peers()
    if dp and ep:
        print("Same connection on both ends (initiated flags opposite):",
              dp[0].initiated != ep[0].initiated)

    print("\n--- 3) A disconnects one peer ---")
    target = a.list_peers()[0]
    a.disconnect_peer(target.peer_id)
    time.sleep(0.4)
    print("A peers:", names(a), "| B peers:", names(b), "| C peers:", names(c))

    print("\n--- 4) B stops; others notice ---")
    b.stop()
    time.sleep(0.5)
    print("A peers:", names(a), "| C peers:", names(c))
    for n in (a, c, d, e):
        n.stop()


def _selftest_text() -> None:
    inbox = {"Alice": [], "Bob": [], "Carol": []}
    inbox_lock = threading.Lock()

    def collector(owner):
        def callback(peer, text):
            with inbox_lock:
                inbox[owner].append((peer.name, text))
        return callback

    def texts(owner):
        with inbox_lock:
            return [t for _, t in inbox[owner]]

    alice = P2PNode("Alice", 5200, on_text_received=collector("Alice"))
    bob = P2PNode("Bob", 5201, on_text_received=collector("Bob"))
    carol = P2PNode("Carol", 5202, on_text_received=collector("Carol"))
    for n in (alice, bob, carol):
        n.start()
    time.sleep(0.2)
    to_alice = bob.connect_to_peer("127.0.0.1", 5200)    # Bob's Peer object for Alice
    carol.connect_to_peer("127.0.0.1", 5200)
    time.sleep(0.3)

    print("\n--- 1) Text in both directions (incl. Bengali) ---")
    bob.send_text(to_alice.peer_id, "Hello Alice!")
    alice.send_text(bob.peer_id, "Hi Bob! হ্যালো")
    time.sleep(0.3)
    print("Alice got:", texts("Alice"))
    print("Bob got:  ", texts("Bob"))

    print("\n--- 2) Order is preserved ---")
    for i in range(1, 6):
        bob.send_text(to_alice.peer_id, f"msg {i}")
    time.sleep(0.3)
    print("Alice got:", [t for t in texts("Alice") if t.startswith("msg")])

    print("\n--- 3) Concurrent sends from 4 threads (send_lock) ---")
    alice.on_event = lambda m: None                       # silence log during the burst
    n_threads, per_thread = 4, 50
    expected = {f"T{k}-{i}-" + "x" * 500 for k in range(n_threads) for i in range(per_thread)}

    def burst(k):
        for i in range(per_thread):
            bob.send_text(to_alice.peer_id, f"T{k}-{i}-" + "x" * 500)

    workers = [threading.Thread(target=burst, args=(k,)) for k in range(n_threads)]
    for w in workers:
        w.start()
    for w in workers:
        w.join()
    time.sleep(1.0)
    alice.on_event = print
    got = [t for t in texts("Alice") if t.startswith("T")]
    print(f"Received {len(got)}/{len(expected)} messages; all intact: {set(got) == expected}")

    print("\n--- 4) Send errors ---")
    for pid, text in [("nonexistent", "hi"), (to_alice.peer_id, "   "),
                      (to_alice.peer_id, "x" * (MAX_TEXT_LENGTH + 1))]:
        try:
            bob.send_text(pid, text)
        except SendError as e:
            print("[ERROR] Send failed:", e)

    print("\n--- 5) Broadcast: Alice -> Bob and Carol ---")
    sent, failed = alice.broadcast_text("Hello everyone")
    time.sleep(0.3)
    print(f"Sent to {sent} peers, failed: {failed}")
    print("Bob:", texts("Bob")[-1], "| Carol:", texts("Carol")[-1])

    print("\n--- 6) Malformed / unknown messages do not crash the receiver ---")
    raw = socket.create_connection(("127.0.0.1", 5200))
    send_message(raw, make_hello("cafebabe", "Mallory", 5999))
    print("   ack type:", recv_message(raw)["type"])
    send_message(raw, {"type": "text"})                   # missing 'message'
    send_message(raw, {"type": "banana"})                 # unknown type
    send_message(raw, make_text("cafebabe", "Mallory", "I am still connected"))
    time.sleep(0.3)
    raw.close()
    time.sleep(0.3)

    for n in (alice, bob, carol):
        n.stop()


if __name__ == "__main__":
    usage = ("Usage:\n  python p2p_node.py selftest | selftest4 | selftest5\n"
             "  python p2p_node.py node <name> <listen_port> [<remote_ip> <remote_port>]")
    if len(sys.argv) < 2:
        print(usage)
    elif sys.argv[1] == "selftest":
        _selftest()
    elif sys.argv[1] == "selftest4":
        _selftest_multi()
    elif sys.argv[1] == "selftest5":
        _selftest_text()
    elif sys.argv[1] == "node" and len(sys.argv) in (4, 6):
        try:
            node = P2PNode(sys.argv[2], sys.argv[3])
            node.start()
        except (ValueError, OSError) as e:
            print(f"[ERROR] {e}")
            sys.exit(1)
        if len(sys.argv) == 6:
            try:
                node.connect_to_peer(sys.argv[4], sys.argv[5])
            except PeerConnectionError as e:
                print(f"[ERROR] Connection failed: {e}")
        help_text = ("  commands: peers | send <peer_id> <text> | broadcast <text> | "
                     "disconnect <peer_id> | quit")
        try:
            while True:
                parts = input().strip().split(maxsplit=2)
                cmd = parts[0].lower() if parts else ""
                if not cmd:
                    continue
                try:
                    if cmd == "peers":
                        for p in node.list_peers():
                            print("  ", p.label())
                    elif cmd == "send" and len(parts) == 3:
                        node.send_text(parts[1], parts[2])
                    elif cmd == "broadcast" and len(parts) >= 2:
                        text = " ".join(parts[1:])
                        sent, failed = node.broadcast_text(text)
                        print(f"  sent to {sent} peer(s)" + (f", failed: {failed}" if failed else ""))
                    elif cmd == "disconnect" and len(parts) == 2:
                        print("  ok" if node.disconnect_peer(parts[1]) else "  no such peer")
                    elif cmd == "quit":
                        break
                    else:
                        print(help_text)
                except SendError as e:
                    print(f"[ERROR] Send failed: {e}")
        except (KeyboardInterrupt, EOFError):
            pass
        node.stop()
    else:
        print(usage)