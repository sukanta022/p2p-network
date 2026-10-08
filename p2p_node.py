"""p2p_node.py - Core network engine (Phase 3: TCP server/client + HELLO handshake)."""
import os
import socket
import sys
import threading
import time
import uuid
from dataclasses import dataclass, field

from protocol import (MSG_HELLO, MSG_HELLO_ACK, ConnectionClosed, ProtocolError,
                      make_hello, make_hello_ack, make_text, recv_message, send_message)

CONNECT_TIMEOUT = 5      # seconds for TCP connect()
HANDSHAKE_TIMEOUT = 5    # seconds to complete hello/hello_ack
ACCEPT_POLL = 1.0        # accept() wakes every second to check self.running


class PeerConnectionError(Exception):
    """Raised when we cannot connect to a remote peer."""


def validate_port(port) -> int:
    try:
        port = int(port)
    except (TypeError, ValueError):
        raise ValueError("Port must be a number")
    if not 1 <= port <= 65535:
        raise ValueError("Port must be between 1 and 65535")
    return port


@dataclass(eq=False)                      # eq=False: compare peers by identity
class Peer:
    peer_id: str
    name: str
    ip: str
    port: int                             # the peer's LISTENING port (from hello)
    sock: socket.socket
    send_lock: threading.Lock = field(default_factory=threading.Lock)

    def label(self) -> str:
        return f"{self.name} [{self.peer_id}] {self.ip}:{self.port}"


class P2PNode:
    def __init__(self, name: str, port, host: str = "0.0.0.0", on_event=None):
        name = (name or "").strip()
        if not name:
            raise ValueError("Peer name cannot be empty")
        self.name = name[:50]
        self.port = validate_port(port)
        self.host = host
        self.peer_id = uuid.uuid4().hex[:8]
        self.on_event = on_event or print          # GUI will replace this later
        self.server_socket = None
        self.running = False
        self._accept_thread = None
        self.peers = {}                            # peer_id -> Peer
        self._peers_lock = threading.Lock()        # protects self.peers

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
            self._close_socket(p.sock)
        t = self._accept_thread
        if t and t is not threading.current_thread():
            t.join(timeout=ACCEPT_POLL * 2)
        self._log("Stopped")

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
            peer = Peer(pid, name, sock.getpeername()[0], rport, sock)
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

    def _register_peer(self, peer: Peer) -> bool:
        with self._peers_lock:                       # check + insert must be atomic
            if peer.peer_id == self.peer_id or peer.peer_id in self.peers:
                return False
            self.peers[peer.peer_id] = peer
            return True

    def _remove_peer(self, peer: Peer) -> None:
        removed = False
        with self._peers_lock:
            if self.peers.get(peer.peer_id) is peer:
                del self.peers[peer.peer_id]
                removed = True
        self._close_socket(peer.sock)
        if removed and self.running:
            self._log(f"Peer disconnected: {peer.name} [{peer.peer_id}]")

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

    # ---------- Receive loop (one thread per peer) ----------

    def _receive_loop(self, peer: Peer) -> None:
        try:
            while True:
                msg = recv_message(peer.sock)
                self._dispatch(peer, msg)
        except ConnectionClosed:
            pass                                  # normal disconnect
        except ProtocolError as e:
            if self.running:
                self._log(f"[ERROR] Protocol error from {peer.name}: {e}")
        except OSError as e:
            if self.running:
                self._log(f"[ERROR] Socket error with {peer.name}: {e}")
        finally:
            self._remove_peer(peer)

    def _dispatch(self, peer: Peer, msg: dict) -> None:
        """Phase 3 placeholder: text (Phase 5) and file (Phase 6) handling comes later."""
        self._log(f"{peer.name} sent '{msg.get('type')}' (not handled yet)")


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


if __name__ == "__main__":
    usage = ("Usage:\n  python p2p_node.py selftest\n"
             "  python p2p_node.py node <name> <listen_port> [<remote_ip> <remote_port>]")
    if len(sys.argv) < 2:
        print(usage)
    elif sys.argv[1] == "selftest":
        _selftest()
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
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            pass
        node.stop()
    else:
        print(usage)