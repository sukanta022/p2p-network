"""main.py - Tkinter GUI for the P2P network application (Phase 7).

Threading rule: Tkinter is NOT thread-safe. Network threads never touch widgets;
they put events into a queue (or a shared variable) and the GUI thread reads
them every POLL_MS milliseconds using root.after().
"""
import os
import queue
import re
import socket
import subprocess
import sys
import threading
import time
import tkinter as tk
from tkinter import filedialog, messagebox, scrolledtext, ttk

from p2p_node import (DEFAULT_DOWNLOAD_DIR, P2PNode, PeerConnectionError, SendError,
                      format_size, validate_port)

INCOMING_RE = re.compile(r"\[[0-9a-f]{8}\]: ")     # matches "Bob [1c783e25]: hello"


def get_local_ip() -> str:
    """Best-effort LAN IP (UDP connect sends no packet). Falls back to localhost."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("10.255.255.255", 1))
        return s.getsockname()[0]
    except OSError:
        return "127.0.0.1"
    finally:
        s.close()


def open_folder(path: str) -> None:
    try:
        os.makedirs(path, exist_ok=True)
        if sys.platform.startswith("win"):
            os.startfile(path)
        elif sys.platform == "darwin":
            subprocess.Popen(["open", path])
        else:
            subprocess.Popen(["xdg-open", path])
    except OSError as e:
        messagebox.showerror("Cannot open folder", str(e))


class P2PApp:
    POLL_MS = 100
    MAX_LOG_LINES = 5000

    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        root.title("P2P Network - Chat & File Sharing")
        root.minsize(840, 640)

        self.node = None                       # P2PNode while running, else None
        self.events = queue.Queue()            # network threads -> GUI thread
        self._listed_ids = []                  # peer_id for each Listbox row
        self._progress = None                  # (direction, filename, done, total) from network threads
        self._shown_progress = None
        self._log_prefix = ""                  # "[MyName] " prefix added by P2PNode._log

        self._build_ui()
        self._set_running(False)
        self.status_var.set("Not running. Enter a name and port, then press Start Peer.")
        root.protocol("WM_DELETE_WINDOW", self._on_close)
        root.after(self.POLL_MS, self._poll)

    # ------------------------------------------------------------------ UI

    def _build_ui(self) -> None:
        root = self.root
        root.columnconfigure(0, weight=1)
        root.rowconfigure(2, weight=1)

        # 1) My Peer
        f1 = ttk.LabelFrame(root, text="My Peer")
        f1.grid(row=0, column=0, sticky="ew", padx=10, pady=(10, 4))
        ttk.Label(f1, text="Name:").grid(row=0, column=0, padx=(8, 2), pady=6)
        self.name_var = tk.StringVar(value="Peer1")
        self.name_entry = ttk.Entry(f1, textvariable=self.name_var, width=18)
        self.name_entry.grid(row=0, column=1, padx=2)
        ttk.Label(f1, text="Port:").grid(row=0, column=2, padx=(12, 2))
        self.port_var = tk.StringVar(value="5000")
        self.port_entry = ttk.Entry(f1, textvariable=self.port_var, width=8)
        self.port_entry.grid(row=0, column=3, padx=2)
        self.start_btn = ttk.Button(f1, text="Start Peer", width=12, command=self._on_start_stop)
        self.start_btn.grid(row=0, column=4, padx=12)
        self.status_var = tk.StringVar()
        ttk.Label(f1, textvariable=self.status_var).grid(
            row=1, column=0, columnspan=5, sticky="w", padx=8, pady=(0, 6))

        # 2) Connect
        f2 = ttk.LabelFrame(root, text="Connect to Peer")
        f2.grid(row=1, column=0, sticky="ew", padx=10, pady=4)
        ttk.Label(f2, text="Remote IP:").grid(row=0, column=0, padx=(8, 2), pady=6)
        self.ip_var = tk.StringVar(value="127.0.0.1")
        self.ip_entry = ttk.Entry(f2, textvariable=self.ip_var, width=18)
        self.ip_entry.grid(row=0, column=1, padx=2)
        ttk.Label(f2, text="Remote Port:").grid(row=0, column=2, padx=(12, 2))
        self.rport_var = tk.StringVar(value="5001")
        self.rport_entry = ttk.Entry(f2, textvariable=self.rport_var, width=8)
        self.rport_entry.grid(row=0, column=3, padx=2)
        self.connect_btn = ttk.Button(f2, text="Connect", width=12, command=self._on_connect)
        self.connect_btn.grid(row=0, column=4, padx=12)
        self.ip_entry.bind("<Return>", lambda e: self._on_connect())
        self.rport_entry.bind("<Return>", lambda e: self._on_connect())

        # 3) Connected peers (left) + communication log (right)
        mid = ttk.Frame(root)
        mid.grid(row=2, column=0, sticky="nsew", padx=10, pady=4)
        mid.columnconfigure(0, weight=2)
        mid.columnconfigure(1, weight=3)
        mid.rowconfigure(0, weight=1)

        peers_frame = ttk.LabelFrame(mid, text="Connected Peers")
        peers_frame.grid(row=0, column=0, sticky="nsew", padx=(0, 5))
        peers_frame.rowconfigure(0, weight=1)
        peers_frame.columnconfigure(0, weight=1)
        self.peer_list = tk.Listbox(peers_frame, exportselection=False, activestyle="none", height=10)
        self.peer_list.grid(row=0, column=0, sticky="nsew", padx=(6, 0), pady=6)
        sb = ttk.Scrollbar(peers_frame, orient="vertical", command=self.peer_list.yview)
        sb.grid(row=0, column=1, sticky="ns", pady=6, padx=(0, 6))
        self.peer_list.config(yscrollcommand=sb.set)
        self.disconnect_btn = ttk.Button(peers_frame, text="Disconnect selected",
                                         command=self._on_disconnect)
        self.disconnect_btn.grid(row=1, column=0, columnspan=2, pady=(0, 6))

        log_frame = ttk.LabelFrame(mid, text="Communication / Event Log")
        log_frame.grid(row=0, column=1, sticky="nsew", padx=(5, 0))
        log_frame.rowconfigure(0, weight=1)
        log_frame.columnconfigure(0, weight=1)
        self.log = scrolledtext.ScrolledText(log_frame, wrap="word", height=12,
                                             state="disabled", font="TkFixedFont")
        self.log.grid(row=0, column=0, sticky="nsew", padx=6, pady=6)
        self.log.tag_config("error", foreground="#c0392b")
        self.log.tag_config("warn", foreground="#d68910")
        self.log.tag_config("incoming", foreground="#1a5fb4")
        self.log.tag_config("outgoing", foreground="#26734d")
        self.log.tag_config("success", foreground="#26734d")
        self.log.tag_config("info", foreground="#333333")

        # 4) Send text
        f4 = ttk.LabelFrame(root, text="Send Text")
        f4.grid(row=3, column=0, sticky="ew", padx=10, pady=4)
        f4.columnconfigure(0, weight=1)
        self.msg_var = tk.StringVar()
        self.msg_entry = ttk.Entry(f4, textvariable=self.msg_var)
        self.msg_entry.grid(row=0, column=0, sticky="ew", padx=(8, 4), pady=6)
        self.msg_entry.bind("<Return>", lambda e: self._on_send_text())
        self.send_btn = ttk.Button(f4, text="Send", width=10, command=self._on_send_text)
        self.send_btn.grid(row=0, column=1, padx=4)
        self.all_var = tk.BooleanVar(value=False)
        self.all_check = ttk.Checkbutton(f4, text="Send to all peers", variable=self.all_var)
        self.all_check.grid(row=0, column=2, padx=(4, 8))

        # 5) Send file
        f5 = ttk.LabelFrame(root, text="Send File")
        f5.grid(row=4, column=0, sticky="ew", padx=10, pady=(4, 10))
        f5.columnconfigure(2, weight=1)
        self.file_btn = ttk.Button(f5, text="Choose File & Send", command=self._on_send_file)
        self.file_btn.grid(row=0, column=0, padx=8, pady=6)
        ttk.Button(f5, text="Open Downloads Folder", command=self._on_open_downloads).grid(
            row=0, column=1, padx=4)
        self.progress = ttk.Progressbar(f5, orient="horizontal", mode="determinate", maximum=100)
        self.progress.grid(row=0, column=2, sticky="ew", padx=(8, 8))
        self.progress_var = tk.StringVar(value="No transfer in progress")
        ttk.Label(f5, textvariable=self.progress_var).grid(
            row=1, column=0, columnspan=3, sticky="w", padx=8, pady=(0, 6))

    def _set_running(self, running: bool) -> None:
        idle = "disabled" if running else "normal"
        live = "normal" if running else "disabled"
        self.name_entry.config(state=idle)
        self.port_entry.config(state=idle)
        self.start_btn.config(text="Stop" if running else "Start Peer")
        for w in (self.ip_entry, self.rport_entry, self.connect_btn, self.disconnect_btn,
                  self.msg_entry, self.send_btn, self.all_check, self.file_btn):
            w.config(state=live)

    # ------------------------------------------------------- start / stop

    def _on_start_stop(self) -> None:
        if self.node is None:
            self._start_node()
        else:
            self._stop_node()

    def _start_node(self) -> None:
        name = self.name_var.get().strip()
        if not name:
            messagebox.showerror("Invalid name", "Please enter a peer name.")
            return
        try:
            port = validate_port(self.port_var.get())
        except ValueError as e:
            messagebox.showerror("Invalid port", str(e))
            return
        try:
            node = P2PNode(
                name, port,
                on_event=lambda line: self.events.put(("log", line)),
                on_peers_changed=lambda: self.events.put(("peers", None)),
                on_progress=self._on_progress)
            node.start()
        except (ValueError, OSError) as e:
            messagebox.showerror("Cannot start peer", str(e))
            self._append(f"[ERROR] {e}", "error")
            return
        self.node = node
        self._log_prefix = f"[{node.name}] "
        self._progress = self._shown_progress = None
        self.progress["value"] = 0
        self.progress_var.set("No transfer in progress")
        self.status_var.set(f"Running  |  ID: {node.peer_id}  |  "
                            f"Your IP: {get_local_ip()}  |  Port: {node.port}")
        self._set_running(True)

    def _stop_node(self) -> None:
        node, self.node = self.node, None
        self._set_running(False)
        self.status_var.set("Stopped.")
        self._refresh_peers()
        # stop() may wait up to ~1 s for the accept thread, so do it off the GUI thread.
        threading.Thread(target=node.stop, daemon=True).start()

    def _on_close(self) -> None:
        if self.node is not None:
            self.node.stop()
            self.node = None
        self.root.destroy()

    # ------------------------------------------------------------ actions

    def _on_connect(self) -> None:
        node = self.node
        if node is None:
            return
        ip, port = self.ip_var.get().strip(), self.rport_var.get().strip()
        if not ip or not port:
            messagebox.showwarning("Missing information", "Enter the remote IP and port.")
            return
        self.connect_btn.config(state="disabled")
        self._append(f"Connecting to {ip}:{port} ...", "info")

        def work():
            try:
                node.connect_to_peer(ip, port)         # blocking: runs off the GUI thread
            except PeerConnectionError as e:
                self.events.put(("log", f"[ERROR] Connection failed: {e}"))
            except Exception as e:
                self.events.put(("log", f"[ERROR] Unexpected error: {e}"))
            finally:
                self.events.put(("connect_done", None))

        threading.Thread(target=work, daemon=True).start()

    def _on_disconnect(self) -> None:
        sel = self.peer_list.curselection()
        if not sel or self.node is None:
            messagebox.showinfo("Disconnect", "Select a peer in the list first.")
            return
        self.node.disconnect_peer(self._listed_ids[sel[0]])

    def _selected_peer(self):
        """The peer chosen in the list; if none is chosen and exactly one peer is connected, that one."""
        node = self.node
        sel = self.peer_list.curselection()
        if sel:
            peer = node.get_peer(self._listed_ids[sel[0]])
            if peer is None:
                messagebox.showinfo("Peer gone", "That peer is no longer connected.")
            return peer
        peers = node.list_peers()
        if len(peers) == 1:
            return peers[0]
        if not peers:
            messagebox.showinfo("No peers", "Connect to a peer first.")
        else:
            messagebox.showinfo("Select a peer",
                                "Select a peer in the list first (or tick 'Send to all peers').")
        return None

    def _run_bg(self, job) -> None:
        """Run job() in a worker thread; show its returned line (or the error) in the log."""
        def worker():
            try:
                line = job()
                if line:
                    self.events.put(("out", line))
            except (SendError, PeerConnectionError) as e:
                self.events.put(("log", f"[ERROR] Send failed: {e}"))
            except Exception as e:
                self.events.put(("log", f"[ERROR] Unexpected error: {e}"))
        threading.Thread(target=worker, daemon=True).start()

    def _on_send_text(self) -> None:
        node = self.node
        if node is None:
            return
        text = self.msg_var.get()
        if not text.strip():
            return
        if self.all_var.get():
            if not node.list_peers():
                messagebox.showinfo("No peers", "Connect to a peer first.")
                return

            def job():
                sent, failed = node.broadcast_text(text)
                line = f"You -> all ({sent} peer{'s' if sent != 1 else ''}): {text}"
                if failed:
                    line += f"   [failed: {', '.join(failed)}]"
                return line
        else:
            peer = self._selected_peer()
            if peer is None:
                return

            def job():
                node.send_text(peer.peer_id, text)
                return f"You -> {peer.name}: {text}"

        self.msg_var.set("")
        self._run_bg(job)

    def _on_send_file(self) -> None:
        node = self.node
        if node is None:
            return
        if self.all_var.get():
            targets = node.list_peers()
            if not targets:
                messagebox.showinfo("No peers", "Connect to a peer first.")
                return
        else:
            peer = self._selected_peer()
            if peer is None:
                return
            targets = [peer]
        path = filedialog.askopenfilename(title="Choose a file to send")
        if not path:
            return
        for p in targets:
            node.send_file_async(p.peer_id, path)      # runs in its own thread

    def _on_open_downloads(self) -> None:
        open_folder(self.node.download_dir if self.node else DEFAULT_DOWNLOAD_DIR)

    # --------------------------------------------- network-thread callbacks

    def _on_progress(self, direction: str, filename: str, done: int, total: int) -> None:
        # Called from network threads for EVERY 64 KB chunk: only remember the latest state.
        self._progress = (direction, filename, done, total)

    # -------------------------------------------------- GUI-thread updates

    def _poll(self) -> None:
        refresh = False
        try:
            for _ in range(200):                       # cap work per tick
                kind, payload = self.events.get_nowait()
                if kind == "log":
                    line = payload
                    if line.startswith(self._log_prefix):
                        line = line[len(self._log_prefix):]
                    self._append(line)
                elif kind == "out":
                    self._append(payload, "outgoing")
                elif kind == "peers":
                    refresh = True
                elif kind == "connect_done":
                    if self.node is not None:
                        self.connect_btn.config(state="normal")
        except queue.Empty:
            pass
        if refresh:
            self._refresh_peers()
        self._update_progress()
        self.root.after(self.POLL_MS, self._poll)

    def _tag_for(self, line: str) -> str:
        if "[ERROR]" in line:
            return "error"
        if "[WARN]" in line:
            return "warn"
        if INCOMING_RE.search(line):
            return "incoming"
        if line.startswith(("File saved", "Peer connected", "Sent '")):
            return "success"
        return "info"

    def _append(self, line: str, tag: str = None) -> None:
        tag = tag or self._tag_for(line)
        self.log.config(state="normal")
        self.log.insert(tk.END, f"[{time.strftime('%H:%M:%S')}] {line}\n", tag)
        if int(self.log.index("end-1c").split(".")[0]) > self.MAX_LOG_LINES:
            self.log.delete("1.0", "500.0")             # keep the log from growing forever
        self.log.see(tk.END)
        self.log.config(state="disabled")

    def _refresh_peers(self) -> None:
        keep = None
        sel = self.peer_list.curselection()
        if sel and sel[0] < len(self._listed_ids):
            keep = self._listed_ids[sel[0]]
        self.peer_list.delete(0, tk.END)
        self._listed_ids = []
        peers = self.node.list_peers() if self.node else []
        for p in sorted(peers, key=lambda p: p.name.lower()):
            self.peer_list.insert(tk.END, p.label())
            self._listed_ids.append(p.peer_id)
            if p.peer_id == keep:
                self.peer_list.selection_set(tk.END)

    def _update_progress(self) -> None:
        prog = self._progress
        if prog is None or prog == self._shown_progress:
            return
        self._shown_progress = prog
        direction, filename, done, total = prog
        pct = 100.0 * done / total if total else 100.0
        self.progress["value"] = pct
        if done >= total:
            word = "Sent" if direction == "send" else "Received"
            self.progress_var.set(f"{word}: {filename} ({format_size(total)})")
        else:
            word = "Sending" if direction == "send" else "Receiving"
            self.progress_var.set(f"{word} {filename}: {pct:.0f}% "
                                  f"({format_size(done)} / {format_size(total)})")


def main() -> None:
    root = tk.Tk()
    P2PApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()