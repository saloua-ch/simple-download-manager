"""
Simple Download Manager (SDM)
Flask backend with multi-threaded segmented downloads
"""

import os
import json
import time
import uuid
import threading
import requests
from flask import Flask, request, jsonify, send_from_directory
from datetime import datetime

app = Flask(__name__, static_folder="static")

DOWNLOAD_DIR = os.path.join(os.path.dirname(__file__), "downloads")
HISTORY_FILE = os.path.join(os.path.dirname(__file__), "history", "history.json")
os.makedirs(DOWNLOAD_DIR, exist_ok=True)
os.makedirs(os.path.dirname(HISTORY_FILE), exist_ok=True)

# In-memory store for active downloads
downloads = {}  # id -> DownloadTask

# ─────────────────────────────────────────────
# DownloadTask class
# ─────────────────────────────────────────────
class DownloadTask:
    def __init__(self, url, filename, num_threads=4, max_retries=3):
        self.id = str(uuid.uuid4())[:8]
        self.url = url
        self.filename = filename
        self.num_threads = num_threads
        self.max_retries = max_retries

        self.status = "pending"       # pending | downloading | paused | completed | error | cancelled
        self.total_size = 0
        self.downloaded = 0
        self.speed = 0                # bytes/sec
        self.start_time = None
        self.error = None
        self.segments = []

        self._pause_event = threading.Event()
        self._pause_event.set()       # not paused initially
        self._cancel_flag = False
        self._lock = threading.Lock()
        self._segment_threads = []

    # ── public controls ──────────────────────
    def pause(self):
        if self.status == "downloading":
            self._pause_event.clear()
            self.status = "paused"

    def resume(self):
        if self.status == "paused":
            self.status = "downloading"
            self._pause_event.set()

    def cancel(self):
        self._cancel_flag = True
        self._pause_event.set()       # unblock any paused threads
        self.status = "cancelled"

    # ── progress helpers ─────────────────────
    def add_progress(self, n):
        with self._lock:
            self.downloaded += n

    def progress_percent(self):
        if self.total_size == 0:
            return 0
        return round(self.downloaded / self.total_size * 100, 1)

    def to_dict(self):
        elapsed = time.time() - self.start_time if self.start_time else 0
        speed = self.downloaded / elapsed if elapsed > 0 else 0
        remaining = (self.total_size - self.downloaded) / speed if speed > 0 else 0
        return {
            "id": self.id,
            "url": self.url,
            "filename": self.filename,
            "status": self.status,
            "total_size": self.total_size,
            "downloaded": self.downloaded,
            "percent": self.progress_percent(),
            "speed": round(speed),
            "eta": round(remaining),
            "error": self.error,
            "num_threads": self.num_threads,
        }

    # ── main download logic ──────────────────
    def start(self):
        thread = threading.Thread(target=self._run, daemon=True)
        thread.start()

    def _run(self):
        self.status = "downloading"
        self.start_time = time.time()

        try:
            # HEAD request to get file size
            head = requests.head(self.url, allow_redirects=True, timeout=10)
            head.raise_for_status()
            self.total_size = int(head.headers.get("Content-Length", 0))
            supports_range = "bytes" in head.headers.get("Accept-Ranges", "")

            if self.total_size == 0 or not supports_range:
                # Fall back to single-threaded
                self._download_single()
            else:
                self._download_segmented()

            if not self._cancel_flag:
                self.status = "completed"
                self._save_history()
        except Exception as e:
            if self.status != "cancelled":
                self.status = "error"
                self.error = str(e)

    def _download_single(self):
        """Single-threaded fallback."""
        dest = os.path.join(DOWNLOAD_DIR, self.filename)
        for attempt in range(1, self.max_retries + 1):
            try:
                with requests.get(self.url, stream=True, timeout=30) as r:
                    r.raise_for_status()
                    self.total_size = int(r.headers.get("Content-Length", 0))
                    with open(dest, "wb") as f:
                        for chunk in r.iter_content(chunk_size=65536):
                            if self._cancel_flag:
                                return
                            self._pause_event.wait()
                            if chunk:
                                f.write(chunk)
                                self.add_progress(len(chunk))
                return
            except Exception as e:
                if attempt == self.max_retries:
                    raise e
                time.sleep(2 ** attempt)

    def _download_segmented(self):
        """Multi-threaded segmented download."""
        segment_size = self.total_size // self.num_threads
        self.segments = []
        for i in range(self.num_threads):
            start = i * segment_size
            end = self.total_size - 1 if i == self.num_threads - 1 else (i + 1) * segment_size - 1
            seg_path = os.path.join(DOWNLOAD_DIR, f"{self.id}_seg{i}")
            self.segments.append({"start": start, "end": end, "path": seg_path, "done": False})

        threads = []
        for i, seg in enumerate(self.segments):
            t = threading.Thread(target=self._download_segment, args=(i, seg), daemon=True)
            threads.append(t)
            self._segment_threads.append(t)
            t.start()

        for t in threads:
            t.join()

        if self._cancel_flag:
            self._cleanup_segments()
            return

        # Merge segments
        dest = os.path.join(DOWNLOAD_DIR, self.filename)
        with open(dest, "wb") as out:
            for seg in self.segments:
                if os.path.exists(seg["path"]):
                    with open(seg["path"], "rb") as f:
                        out.write(f.read())
                    os.remove(seg["path"])

    def _download_segment(self, idx, seg):
        """Download a single segment with retry."""
        headers = {"Range": f"bytes={seg['start']}-{seg['end']}"}
        for attempt in range(1, self.max_retries + 1):
            try:
                with requests.get(self.url, headers=headers, stream=True, timeout=30) as r:
                    r.raise_for_status()
                    with open(seg["path"], "wb") as f:
                        for chunk in r.iter_content(chunk_size=65536):
                            if self._cancel_flag:
                                return
                            self._pause_event.wait()
                            if chunk:
                                f.write(chunk)
                                self.add_progress(len(chunk))
                seg["done"] = True
                return
            except Exception as e:
                if attempt == self.max_retries:
                    self.error = f"Segment {idx} failed: {e}"
                    self._cancel_flag = True
                    return
                time.sleep(2 ** attempt)

    def _cleanup_segments(self):
        for seg in self.segments:
            if os.path.exists(seg["path"]):
                os.remove(seg["path"])

    def _save_history(self):
        history = _load_history()
        history.append({
            "id": self.id,
            "url": self.url,
            "filename": self.filename,
            "size": self.total_size,
            "date": datetime.now().strftime("%Y-%m-%d %H:%M"),
            "threads": self.num_threads,
        })
        with open(HISTORY_FILE, "w") as f:
            json.dump(history, f, indent=2)


# ─────────────────────────────────────────────
# History helpers
# ─────────────────────────────────────────────
def _load_history():
    if os.path.exists(HISTORY_FILE):
        try:
            with open(HISTORY_FILE) as f:
                return json.load(f)
        except Exception:
            pass
    return []


# ─────────────────────────────────────────────
# API routes
# ─────────────────────────────────────────────
@app.route("/")
def index():
    return send_from_directory("static", "index.html")

@app.route("/api/download", methods=["POST"])
def add_download():
    data = request.json
    url = data.get("url", "").strip()
    if not url:
        return jsonify({"error": "URL is required"}), 400

    filename = data.get("filename") or url.split("/")[-1].split("?")[0] or "download"
    num_threads = int(data.get("threads", 4))
    max_retries = int(data.get("retries", 3))

    task = DownloadTask(url, filename, num_threads, max_retries)
    downloads[task.id] = task
    task.start()
    return jsonify(task.to_dict()), 201

@app.route("/api/downloads", methods=["GET"])
def list_downloads():
    return jsonify([t.to_dict() for t in downloads.values()])

@app.route("/api/downloads/<did>/pause", methods=["POST"])
def pause_download(did):
    task = downloads.get(did)
    if not task:
        return jsonify({"error": "Not found"}), 404
    task.pause()
    return jsonify(task.to_dict())

@app.route("/api/downloads/<did>/resume", methods=["POST"])
def resume_download(did):
    task = downloads.get(did)
    if not task:
        return jsonify({"error": "Not found"}), 404
    task.resume()
    return jsonify(task.to_dict())

@app.route("/api/downloads/<did>/cancel", methods=["POST"])
def cancel_download(did):
    task = downloads.get(did)
    if not task:
        return jsonify({"error": "Not found"}), 404
    task.cancel()
    return jsonify(task.to_dict())

@app.route("/api/downloads/<did>", methods=["DELETE"])
def delete_download(did):
    task = downloads.pop(did, None)
    if task:
        task.cancel()
    return jsonify({"ok": True})

@app.route("/api/history", methods=["GET"])
def get_history():
    return jsonify(_load_history())

@app.route("/api/history", methods=["DELETE"])
def clear_history():
    with open(HISTORY_FILE, "w") as f:
        json.dump([], f)
    return jsonify({"ok": True})

if __name__ == "__main__":
    app.run(debug=True, port=5000, threaded=True)
