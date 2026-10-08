"""protocol.py - Application-level protocol: framing, message builders, file chunk helpers."""
import json
import socket
import struct

HEADER_SIZE = 4
HEADER_FORMAT = ">I"                 # big-endian unsigned 32-bit int
MAX_MESSAGE_SIZE = 1 * 1024 * 1024   # sanity limit for JSON messages (1 MB)
CHUNK_SIZE = 64 * 1024               # file transfer chunk size (64 KB)
MAX_FILE_SIZE = 4 * 1024 ** 3        # sanity limit for a single file (4 GB)

MSG_HELLO = "hello"
MSG_HELLO_ACK = "hello_ack"
MSG_TEXT = "text"
MSG_FILE = "file"


class ProtocolError(Exception):
    """Malformed or invalid message."""


class ConnectionClosed(ProtocolError):
    """Peer closed the connection before we got all expected bytes."""


# ---------- Framing ----------

def recv_exact(sock: socket.socket, n: int) -> bytes:
    """Read exactly n bytes from sock (loops because recv() may return fewer)."""
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:                       # b"" means peer closed (FIN)
            raise ConnectionClosed("Connection closed by peer")
        buf.extend(chunk)
    return bytes(buf)


def send_message(sock: socket.socket, message: dict) -> None:
    """Encode dict -> JSON -> bytes, prefix 4-byte length, send everything."""
    payload = json.dumps(message, ensure_ascii=False).encode("utf-8")
    if len(payload) > MAX_MESSAGE_SIZE:
        raise ProtocolError("Message too large")
    header = struct.pack(HEADER_FORMAT, len(payload))
    sock.sendall(header + payload)


def recv_message(sock: socket.socket) -> dict:
    """Read one framed JSON message and return it as a dict."""
    header = recv_exact(sock, HEADER_SIZE)
    (length,) = struct.unpack(HEADER_FORMAT, header)
    if length == 0 or length > MAX_MESSAGE_SIZE:
        raise ProtocolError(f"Invalid message length: {length}")
    payload = recv_exact(sock, length)
    try:
        message = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        raise ProtocolError(f"Invalid JSON payload: {e}")
    if not isinstance(message, dict) or "type" not in message:
        raise ProtocolError("Message must be a JSON object with a 'type' field")
    return message


# ---------- Message builders ----------

def make_hello(peer_id: str, peer_name: str, port: int) -> dict:
    return {"type": MSG_HELLO, "peer_id": peer_id, "peer_name": peer_name, "port": port}


def make_hello_ack(peer_id: str, peer_name: str, port: int) -> dict:
    return {"type": MSG_HELLO_ACK, "peer_id": peer_id, "peer_name": peer_name, "port": port}


def make_text(sender_id: str, sender_name: str, message: str) -> dict:
    return {"type": MSG_TEXT, "sender_id": sender_id,
            "sender_name": sender_name, "message": message}


def make_file_meta(sender_id: str, sender_name: str, filename: str, filesize: int) -> dict:
    return {"type": MSG_FILE, "sender_id": sender_id, "sender_name": sender_name,
            "filename": filename, "filesize": filesize}


# ---------- File chunk helpers (raw bytes, NOT framed) ----------

def send_file_bytes(sock: socket.socket, fileobj, filesize: int, on_progress=None) -> None:
    """Send exactly `filesize` raw bytes from fileobj, CHUNK_SIZE bytes at a time."""
    sent = 0
    while sent < filesize:
        chunk = fileobj.read(min(CHUNK_SIZE, filesize - sent))
        if not chunk:
            raise ProtocolError("File ended early (it changed while sending)")
        sock.sendall(chunk)
        sent += len(chunk)
        if on_progress:
            on_progress(sent, filesize)


def recv_file_bytes(sock: socket.socket, filesize: int, out_file=None, on_progress=None):
    """Read exactly `filesize` raw bytes from sock, never one byte more.

    Bytes are written to out_file (pass None to discard them). If writing fails
    (e.g. disk full) we keep reading and discarding so the stream stays in sync,
    and return that OSError. Returns None on complete success.
    Raises ConnectionClosed if the peer disconnects early.
    """
    received = 0
    write_error = None
    while received < filesize:
        chunk = sock.recv(min(CHUNK_SIZE, filesize - received))
        if not chunk:
            raise ConnectionClosed(f"connection closed after {received} of {filesize} bytes")
        if out_file is not None and write_error is None:
            try:
                out_file.write(chunk)
            except OSError as e:
                write_error = e
        received += len(chunk)
        if on_progress:
            on_progress(received, filesize)
    return write_error


# ---------- Self-test: python protocol.py ----------

if __name__ == "__main__":
    import io
    import os
    import threading
    import time

    a, b = socket.socketpair()   # two connected sockets, no network needed

    print("1) Round trip (with Bengali text):")
    send_message(a, make_text("a83f21c4", "Alice", "Hello Bob! হ্যালো"))
    print("  ", recv_message(b))

    print("2) Coalescing - two messages sent back-to-back:")
    send_message(a, make_text("a83f21c4", "Alice", "one"))
    send_message(a, make_text("a83f21c4", "Alice", "two"))
    print("  ", recv_message(b)["message"], "|", recv_message(b)["message"])

    print("3) Fragmentation - 1 byte at a time:")
    payload = json.dumps(make_hello("a83f21c4", "Alice", 5000)).encode()
    raw = struct.pack(HEADER_FORMAT, len(payload)) + payload

    def dribble():
        for i in range(len(raw)):
            a.sendall(raw[i:i + 1])
            time.sleep(0.005)

    t = threading.Thread(target=dribble)
    t.start()
    print("  ", recv_message(b))
    t.join()

    print("4) Peer closes connection:")
    a.close()
    try:
        recv_message(b)
    except ConnectionClosed as e:
        print("   ConnectionClosed:", e)

    print("5) Bad length header:")
    c, d = socket.socketpair()
    c.sendall(struct.pack(HEADER_FORMAT, 999_999_999))
    try:
        recv_message(d)
    except ProtocolError as e:
        print("   ProtocolError:", e)

    print("6) File bytes between two framed messages (200,000 bytes = 3 full chunks + remainder):")
    e, f = socket.socketpair()
    data = os.urandom(200_000)

    def file_sender():
        send_message(e, make_file_meta("a83f21c4", "Alice", "demo.bin", len(data)))
        send_file_bytes(e, io.BytesIO(data), len(data))
        send_message(e, make_text("a83f21c4", "Alice", "text after the file"))

    t = threading.Thread(target=file_sender)
    t.start()
    meta = recv_message(f)
    out = io.BytesIO()
    err = recv_file_bytes(f, meta["filesize"], out)
    nxt = recv_message(f)
    t.join()
    print("   metadata:", meta["filename"], meta["filesize"])
    print("   bytes identical:", out.getvalue() == data, "| write error:", err)
    print("   next message still intact:", nxt["message"])