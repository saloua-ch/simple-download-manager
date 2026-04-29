"""
Simple Download Manager (SDM) - v2
Flask backend with multi-threaded segmented downloads

NEW in v2:
  - Resume after app restart  (state saved to history/queue.json)
  - Bandwidth limiting        (bytes/sec cap per download)
  - Download queue management (max concurrent downloads, queued state)
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
QUEUE_FILE   = os.path.join(os.path.dirname(__file__), "history", "queue.json")

os.makedirs(DOWNLOAD_DIR, exist_ok=True)
os.makedirs(os.path.dirname(HISTORY_FILE), exist_ok=True)

# ── Global settings ───────────────────────────────────────────────────────────
MAX_CONCURRENT = 3          # max downloads running at the same time
downloads = {}              # id -> DownloadTask
queue_lock = threading.Lock()

# ─────────────────────────────────────────────────────────────────────────────
# DownloadTask
# ─────────────────────────────────────────────────────────────────────────────
class DownloadTask:
    def __init__(self, url, filename, num_threads=4, max_retries=3,
                 bandwidth_limit=0, task_id=None):
        """
        bandwidth_limit : max bytes/sec per download (0 = unlimited)
        task_id         : pass existing id when restoring from queue.json
        """
        self.id            = task_id or str(uuid.uuid4())[:8]
        self.url           = url
        self.filename      = filename
        self.num_threads   = num_threads
        self.max_retries   = max_retries
        self.bandwidth_limit = bandwidth_limit  # bytes/sec, 0 = unlimited

        # status: pending | queued | downloading | paused | completed | error | cancelled
        self.status    = "pending"
        self.total_size = 0
        self.downloaded = 0
        self.start_time = None
        self.error      = None
        self.segments   = []           # list of segment dicts
        self.queue_pos  = 0            # position in queue (UI hint)

        self._pause_event   = threading.Event()
        self._pause_event.set()
        self._cancel_flag   = False
        self._lock          = threading.Lock()
        self._segment_threads = []

    # ── Controls ──────────────────────────────────────────────────────────────
    def pause(self):
        if self.status == "downloading":
            self._pause_event.clear()
            self.status = "paused"
            _persist_queue()

    def resume(self):
        if self.status == "paused":
            self.status = "downloading"
            self._pause_event.set()
            _persist_queue()

    def cancel(self):
        self._cancel_flag = True
        self._pause_event.set()
        self.status = "cancelled"
        _persist_queue()

    # ── Progress ──────────────────────────────────────────────────────────────
    def add_progress(self, n):
        with self._lock:
            self.downloaded += n

    def progress_percent(self):
        if self.total_size == 0:
            return 0
        return round(self.downloaded / self.total_size * 100, 1)

    def to_dict(self):
        elapsed  = time.time() - self.start_time if self.start_time else 0
        speed    = self.downloaded / elapsed if elapsed > 0 else 0
        remaining = (self.total_size - self.downloaded) / speed if speed > 0 else 0
        return {
            "id":               self.id,
            "url":              self.url,
            "filename":         self.filename,
            "status":           self.status,
            "total_size":       self.total_size,
            "downloaded":       self.downloaded,
            "percent":          self.progress_percent(),
            "speed":            round(speed),
            "eta":              round(remaining),
            "error":            self.error,
            "num_threads":      self.num_threads,
            "bandwidth_limit":  self.bandwidth_limit,
            "queue_pos":        self.queue_pos,
        }

    def to_state(self):
        """Serialisable snapshot for queue.json (resume-after-restart)."""
        return {
            "id":              self.id,
            "url":             self.url,
            "filename":        self.filename,
            "num_threads":     self.num_threads,
            "max_retries":     self.max_retries,
            "bandwidth_limit": self.bandwidth_limit,
            "status":          self.status,
            "total_size":      self.total_size,
            "downloaded":      self.downloaded,
            # Save per-segment byte offsets so we can resume exactly
            "segments": [
                {
                    "start": s["start"],
                    "end":   s["end"],
                    "done":  s["done"],
                    # how many bytes already written to the seg file
                    "written": os.path.getsize(s["path"]) if os.path.exists(s["path"]) else 0,
                }
                for s in self.segments
            ],
        }

    # ── Entry point ───────────────────────────────────────────────────────────
    def start(self):
        """Schedule or start the download respecting MAX_CONCURRENT."""
        with queue_lock:
            active = sum(1 for t in downloads.values() if t.status == "downloading")
            if active >= MAX_CONCURRENT:
                self.status = "queued"
                self.queue_pos = active
                _persist_queue()
                return
        # Enough slots — start immediately
        self._launch()

    def _launch(self):
        self.start_time = self.start_time or time.time()
        self.status = "downloading"
        _persist_queue()
        t = threading.Thread(target=self._run, daemon=True)
        t.start()

    def _run(self):
        try:
            head = requests.head(self.url, allow_redirects=True, timeout=10)
            head.raise_for_status()
            self.total_size   = int(head.headers.get("Content-Length", 0))
            supports_range    = "bytes" in head.headers.get("Accept-Ranges", "")

            if self.total_size == 0 or not supports_range:
                self._download_single()
            else:
                self._download_segmented()

            if not self._cancel_flag:
                self.status = "completed"
                self._save_history()
                _persist_queue()
                _promote_queued()          # wake up next queued download
        except Exception as e:
            if self.status != "cancelled":
                self.status = "error"
                self.error  = str(e)
                _persist_queue()
                _promote_queued()

    # ── Bandwidth-aware write helper ──────────────────────────────────────────
    def _write_chunk(self, f, chunk):
        """Write chunk to file, sleeping if needed for bandwidth cap."""
        if not chunk:
            return
        if self.bandwidth_limit > 0:
            chunk_size = len(chunk)
            t0 = time.time()
            f.write(chunk)
            self.add_progress(chunk_size)
            elapsed = time.time() - t0
            expected = chunk_size / self.bandwidth_limit
            if expected > elapsed:
                time.sleep(expected - elapsed)
        else:
            f.write(chunk)
            self.add_progress(len(chunk))

    # ── Single-thread fallback ────────────────────────────────────────────────
    def _download_single(self):
        dest = os.path.join(DOWNLOAD_DIR, self.filename)
        for attempt in range(1, self.max_retries + 1):
            try:
                resume_pos = os.path.getsize(dest) if os.path.exists(dest) else 0
                headers = {}
                if resume_pos:
                    headers["Range"] = f"bytes={resume_pos}-"
                    self.downloaded = resume_pos

                with requests.get(self.url, headers=headers, stream=True, timeout=30) as r:
                    r.raise_for_status()
                    if not self.total_size:
                        self.total_size = int(r.headers.get("Content-Length", 0)) + resume_pos
                    mode = "ab" if resume_pos else "wb"
                    with open(dest, mode) as f:
                        for chunk in r.iter_content(chunk_size=65536):
                            if self._cancel_flag:
                                return
                            self._pause_event.wait()
                            self._write_chunk(f, chunk)
                return
            except Exception as e:
                if attempt == self.max_retries:
                    raise e
                time.sleep(2 ** attempt)

    # ── Multi-thread segmented ────────────────────────────────────────────────
    def _download_segmented(self):
        # Build segment list only if not already restored from state
        if not self.segments:
            seg_size = self.total_size // self.num_threads
            for i in range(self.num_threads):
                start = i * seg_size
                end   = self.total_size - 1 if i == self.num_threads - 1 else (i + 1) * seg_size - 1
                seg_path = os.path.join(DOWNLOAD_DIR, f"{self.id}_seg{i}")
                self.segments.append({"start": start, "end": end, "path": seg_path, "done": False})
        else:
            # Restored — rebuild full paths (they aren't stored in queue.json)
            for i, seg in enumerate(self.segments):
                seg["path"] = os.path.join(DOWNLOAD_DIR, f"{self.id}_seg{i}")

        threads = []
        for i, seg in enumerate(self.segments):
            if seg.get("done"):
                continue                   # already finished before restart
            t = threading.Thread(target=self._download_segment, args=(i, seg), daemon=True)
            threads.append(t)
            self._segment_threads.append(t)
            t.start()

        for t in threads:
            t.join()

        if self._cancel_flag:
            self._cleanup_segments()
            return

        # Merge
        dest = os.path.join(DOWNLOAD_DIR, self.filename)
        with open(dest, "wb") as out:
            for seg in self.segments:
                if os.path.exists(seg["path"]):
                    with open(seg["path"], "rb") as f:
                        out.write(f.read())
                    os.remove(seg["path"])

    def _download_segment(self, idx, seg):
        """Download one segment, resuming from partial file if it exists."""
        seg_path = seg["path"]
        for attempt in range(1, self.max_retries + 1):
            try:
                written = os.path.getsize(seg_path) if os.path.exists(seg_path) else 0
                resume_start = seg["start"] + written
                if written:
                    self.add_progress(written)   # count already-downloaded bytes

                headers = {"Range": f"bytes={resume_start}-{seg['end']}"}
                with requests.get(self.url, headers=headers, stream=True, timeout=30) as r:
                    r.raise_for_status()
                    mode = "ab" if written else "wb"
                    with open(seg_path, mode) as f:
                        for chunk in r.iter_content(chunk_size=65536):
                            if self._cancel_flag:
                                return
                            self._pause_event.wait()
                            self._write_chunk(f, chunk)
                seg["done"] = True
                _persist_queue()
                return
            except Exception as e:
                if attempt == self.max_retries:
                    self.error       = f"Segment {idx} failed: {e}"
                    self._cancel_flag = True
                    return
                time.sleep(2 ** attempt)

    def _cleanup_segments(self):
        for seg in self.segments:
            if os.path.exists(seg.get("path", "")):
                os.remove(seg["path"])

    def _save_history(self):
        history = _load_history()
        history.append({
            "id":       self.id,
            "url":      self.url,
            "filename": self.filename,
            "size":     self.total_size,
            "date":     datetime.now().strftime("%Y-%m-%d %H:%M"),
            "threads":  self.num_threads,
        })
        with open(HISTORY_FILE, "w") as f:
            json.dump(history, f, indent=2)


# ─────────────────────────────────────────────────────────────────────────────
# Queue persistence  (resume-after-restart)
# ─────────────────────────────────────────────────────────────────────────────
def _persist_queue():
    """Save every non-completed, non-cancelled task to queue.json."""
    state = {
        tid: task.to_state()
        for tid, task in downloads.items()
        if task.status not in ("completed", "cancelled")
    }
    try:
        with open(QUEUE_FILE, "w") as f:
            json.dump(state, f, indent=2)
    except Exception:
        pass


def _restore_queue():
    """On startup, reload interrupted downloads from queue.json."""
    if not os.path.exists(QUEUE_FILE):
        return
    try:
        with open(QUEUE_FILE) as f:
            state = json.load(f)
    except Exception:
        return

    for tid, s in state.items():
        task = DownloadTask(
            url             = s["url"],
            filename        = s["filename"],
            num_threads     = s.get("num_threads", 4),
            max_retries     = s.get("max_retries", 3),
            bandwidth_limit = s.get("bandwidth_limit", 0),
            task_id         = s["id"],
        )
        task.total_size  = s.get("total_size", 0)
        task.downloaded  = s.get("downloaded", 0)
        task.segments    = s.get("segments", [])
        task.status      = "paused"    # start paused so user can review
        downloads[task.id] = task

    print(f"[SDM] Restored {len(state)} download(s) from previous session.")


# ─────────────────────────────────────────────────────────────────────────────
# Queue promotion (starts next queued download when a slot frees up)
# ─────────────────────────────────────────────────────────────────────────────
def _promote_queued():
    """Find the first queued task and launch it if a slot is free."""
    with queue_lock:
        active = sum(1 for t in downloads.values() if t.status == "downloading")
        if active >= MAX_CONCURRENT:
            return
        for task in list(downloads.values()):
            if task.status == "queued":
                task._launch()
                break
    # Re-number queue positions
    pos = 1
    for task in downloads.values():
        if task.status == "queued":
            task.queue_pos = pos
            pos += 1


# ─────────────────────────────────────────────────────────────────────────────
# History helpers
# ─────────────────────────────────────────────────────────────────────────────
def _load_history():
    if os.path.exists(HISTORY_FILE):
        try:
            with open(HISTORY_FILE) as f:
                return json.load(f)
        except Exception:
            pass
    return []


# ─────────────────────────────────────────────────────────────────────────────
# API routes
# ─────────────────────────────────────────────────────────────────────────────
@app.route("/")
def index():
    return send_from_directory("static", "index.html")


@app.route("/api/download", methods=["POST"])
def add_download():
    data = request.json
    url  = data.get("url", "").strip()
    if not url:
        return jsonify({"error": "URL is required"}), 400

    filename         = data.get("filename") or url.split("/")[-1].split("?")[0] or "download"
    num_threads      = int(data.get("threads", 4))
    max_retries      = int(data.get("retries", 3))
    bandwidth_limit  = int(data.get("bandwidth_limit", 0))   # bytes/sec (0=unlimited)

    task = DownloadTask(url, filename, num_threads, max_retries, bandwidth_limit)
    downloads[task.id] = task
    task.start()
    _persist_queue()
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
    # If it was paused (including restored-from-restart), launch now
    if task.status == "paused":
        with queue_lock:
            active = sum(1 for t in downloads.values() if t.status == "downloading")
        if active < MAX_CONCURRENT:
            task._launch()
        else:
            task.status = "queued"
            _persist_queue()
    else:
        task.resume()
    return jsonify(task.to_dict())


@app.route("/api/downloads/<did>/cancel", methods=["POST"])
def cancel_download(did):
    task = downloads.get(did)
    if not task:
        return jsonify({"error": "Not found"}), 404
    task.cancel()
    _promote_queued()
    return jsonify(task.to_dict())


@app.route("/api/downloads/<did>", methods=["DELETE"])
def delete_download(did):
    task = downloads.pop(did, None)
    if task:
        task.cancel()
    _persist_queue()
    _promote_queued()
    return jsonify({"ok": True})


# ── Queue management ──────────────────────────────────────────────────────────
@app.route("/api/settings", methods=["GET"])
def get_settings():
    return jsonify({"max_concurrent": MAX_CONCURRENT})


@app.route("/api/settings", methods=["POST"])
def update_settings():
    global MAX_CONCURRENT
    data = request.json
    if "max_concurrent" in data:
        MAX_CONCURRENT = max(1, int(data["max_concurrent"]))
        _promote_queued()
    return jsonify({"max_concurrent": MAX_CONCURRENT})


# ── History ───────────────────────────────────────────────────────────────────
@app.route("/api/history", methods=["GET"])
def get_history():
    return jsonify(_load_history())


@app.route("/api/history", methods=["DELETE"])
def clear_history():
    with open(HISTORY_FILE, "w") as f:
        json.dump([], f)
    return jsonify({"ok": True})


# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    _restore_queue()          # ← resume-after-restart
    app.run(debug=True, port=5000, threaded=True)