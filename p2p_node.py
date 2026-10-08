"""p2p_node.py - Core network engine (Phase 2: basic TCP server + client)."""
import os
import socket
import sys
import threading
import time

from protocol import ProtocolError, make_text, recv_message, send_message

CONNECT_TIMEOUT = 5     # seconds to wait for connect()
ACCEPT_POLL = 1.0       # accept() wakes up every second to check self.running


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


class P2PNode:
    def __init__(self, name: str, port, host: str = "0.0.0.0", on_event=None):
        self.name = name
        self.port = validate_port(port)
        self.host = host
        self.on_event = on_event or print     # GUI will replace this later
        self.server_socket = None
        self.running = False
        self._accept_thread = None

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
        self._log(f"Listening on {self.host}:{self.port}")

    def _accept_loop(self) -> None:
        server = self.server_socket          # local copy: stop() may set the attribute to None
        while self.running:
            try:
                conn, addr = server.accept()
            except socket.timeout:
                continue                     # just re-check self.running
            except OSError:
                break                        # socket was closed by stop()
            conn.settimeout(None)            # accepted socket: plain blocking mode
            self._log(f"Incoming connection from {addr[0]}:{addr[1]}")
            threading.Thread(target=self._handle_incoming,
                             args=(conn, addr), daemon=True).start()

    def _handle_incoming(self, conn: socket.socket, addr) -> None:
        """Phase 2 placeholder: read ONE message, log it, close.
        Replaced by the handshake + receive loop in Phase 3/4."""
        try:
            msg = recv_message(conn)
            self._log(f"Received from {addr[0]}:{addr[1]} -> {msg}")
        except ProtocolError as e:
            self._log(f"[ERROR] {e}")
        except OSError as e:
            self._log(f"[ERROR] Socket error: {e}")
        finally:
            conn.close()

    def stop(self) -> None:
        self.running = False
        if self.server_socket:
            try:
                self.server_socket.close()
            except OSError:
                pass
            self.server_socket = None
        t = self._accept_thread
        if t and t is not threading.current_thread():
            t.join(timeout=ACCEPT_POLL * 2)
        self._log("Stopped")

    # ---------- Client role ----------

    def connect_to_peer(self, ip: str, port) -> socket.socket:
        """Open a TCP connection to a remote peer. Returns the connected socket."""
        try:
            port = validate_port(port)
        except ValueError as e:
            raise PeerConnectionError(str(e)) from e
        if not str(ip).strip():
            raise PeerConnectionError("IP address is empty")

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(CONNECT_TIMEOUT)
        try:
            sock.connect((ip, port))
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
        sock.settimeout(None)
        self._log(f"Connected to {ip}:{port}")
        return sock


# ---------- Manual / self tests ----------

def _selftest() -> None:
    alice = P2PNode("Alice", 5000)
    alice.start()
    time.sleep(0.2)

    bob = P2PNode("Bob", 5001)               # not started; used only as a client
    s = bob.connect_to_peer("127.0.0.1", 5000)
    send_message(s, make_text("b0b0b0b0", "Bob", "Hello Alice!"))
    s.close()
    time.sleep(0.3)

    print("--- error cases ---")
    for ip, port in [("127.0.0.1", 5999), ("999.1.1.1", 5000),
                     ("127.0.0.1", "abc"), ("127.0.0.1", 70000), ("", 5000)]:
        try:
            bob.connect_to_peer(ip, port)
        except PeerConnectionError as e:
            print(f"[ERROR] Connection failed ({ip!r}:{port!r}): {e}")

    print("--- port already in use ---")
    try:
        P2PNode("Dup", 5000).start()
    except OSError as e:
        print("[ERROR]", e)

    alice.stop()


if __name__ == "__main__":
    usage = ("Usage:\n  python p2p_node.py selftest\n"
             "  python p2p_node.py server <port>\n"
             "  python p2p_node.py client <ip> <port>")
    if len(sys.argv) < 2:
        print(usage)
    elif sys.argv[1] == "selftest":
        _selftest()
    elif sys.argv[1] == "server" and len(sys.argv) == 3:
        node = P2PNode("Server", sys.argv[2])
        node.start()
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            pass
        node.stop()
    elif sys.argv[1] == "client" and len(sys.argv) == 4:
        node = P2PNode("Client", 5001)
        try:
            s = node.connect_to_peer(sys.argv[2], sys.argv[3])
            send_message(s, make_text("c1c1c1c1", "Client", "Hello from client!"))
            s.close()
        except PeerConnectionError as e:
            print(f"[ERROR] Connection failed: {e}")
    else:
        print(usage)