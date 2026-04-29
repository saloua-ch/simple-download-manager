# Simple Download Manager (SDM)

**Distributed Systems Project — MedTech**  
**GitHub:** https://github.com/saloua-ch/simple-download-manager

---

## Architecture

SDM uses a **Client–Server layered architecture**:

```
┌─────────────────────────────────────────────────┐
│              Browser UI (Client)                │
│     HTML/CSS/JS — polls /api/* every 800ms      │
└────────────────────┬────────────────────────────┘
                     │ HTTP / REST (JSON)
┌────────────────────▼────────────────────────────┐
│           Flask REST API (Server)               │  port 5000
├─────────────────────────────────────────────────┤
│            Download Manager Core                │
│   ┌─────────────┐   ┌────────────────────────┐  │
│   │   Queue     │   │   Segment Workers      │  │
│   │  Controller │   │  (N threads, Range)    │  │
│   └─────────────┘   └────────────────────────┘  │
├─────────────────────────────────────────────────┤
│   File Assembler   │   Persistence Module       │
│   (merge segments) │   (history + queue JSON)   │
└─────────────────────────────────────────────────┘
```

## System Components

| Component | Description |
|---|---|
| **Flask REST API** | Exposes all `/api/*` endpoints |
| **DownloadTask** | Manages full lifecycle of one download |
| **Queue Controller** | Enforces MAX_CONCURRENT limit, promotes queued tasks |
| **Segment Workers** | N threads each downloading a byte range via HTTP `Range` |
| **File Assembler** | Merges ordered segment files into the final file |
| **Persistence Module** | Saves state to `history/queue.json` (resume) and `history/history.json` (log) |
| **Web UI** | Single-page dark-theme app with real-time polling |

## Communication

- **UI ↔ Server**: HTTP REST (JSON)
- **Threads**: `threading.Thread`, `threading.Event` for pause/resume, `threading.Lock` for progress
- **Bandwidth cap**: per-chunk sleep calculated as `chunk_size / limit - elapsed`

---

## How Multi-Threading Works

1. `HEAD` request → get `Content-Length` and check `Accept-Ranges: bytes`
2. File split into N equal byte ranges
3. N threads start simultaneously, each with `Range: bytes=X-Y` header
4. Each thread writes to its own temp segment file (`{id}_seg{i}`)
5. After all threads join → segments merged in order → temp files deleted

---

## Features

### Core (Required)
- ✅ Add download via URL
- ✅ Start / Cancel download
- ✅ Multi-threaded segmented downloads (HTTP Range requests)
- ✅ Basic progress display

### Optional — All Implemented
- ✅ **Web UI** (dark theme, single-page app)
- ✅ **Pause / Resume** — `threading.Event` blocks all segment threads cleanly
- ✅ **Auto-retry** — exponential backoff (`2^attempt` seconds) per segment, configurable
- ✅ **Download history** — persisted to `history/history.json`
- ✅ **Download speed** display (bytes/sec, live)
- ✅ **Progress percentage** (live)
- ✅ **Estimated remaining time** (ETA, live)
- ✅ **Bandwidth limiting** — per-download KB/s cap, set in the UI form
- ✅ **Download queue management** — configurable max concurrent limit, auto-promotes queued tasks
- ✅ **Resume after app restart** — segment state saved to `queue.json`, restored on next launch

---

## Setup & Run

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Run the server
python app.py

# 3. Open browser
http://localhost:5000
```

---

## API Endpoints

| Method | Endpoint | Description |
|---|---|---|
| POST | `/api/download` | Add a new download |
| GET | `/api/downloads` | List all active downloads |
| POST | `/api/downloads/{id}/pause` | Pause a download |
| POST | `/api/downloads/{id}/resume` | Resume a paused/queued download |
| POST | `/api/downloads/{id}/cancel` | Cancel a download |
| DELETE | `/api/downloads/{id}` | Remove from list |
| GET | `/api/history` | Get completed download history |
| DELETE | `/api/history` | Clear history |
| GET | `/api/settings` | Get current settings (max_concurrent) |
| POST | `/api/settings` | Update settings |

### POST /api/download — Request Body

```json
{
  "url": "https://example.com/file.zip",
  "filename": "file.zip",
  "threads": 4,
  "retries": 3,
  "bandwidth_limit": 512000
}
```

`bandwidth_limit` is in bytes/sec (0 = unlimited).

---

## Project Structure

```
sdm/
├── app.py                  # Flask backend + DownloadTask engine
├── requirements.txt        # flask, requests
├── README.md               # This file
├── static/
│   └── index.html          # Web UI (single file, dark theme)
├── downloads/              # Final downloaded files saved here
└── history/
    ├── history.json        # Log of completed downloads
    └── queue.json          # Live state for resume-after-restart
```

---

## Thread Model

Each `DownloadTask` spawns N `threading.Thread` workers. Shared state is protected by a `threading.Lock` (progress counter). Pause/resume uses a `threading.Event` — cleared on pause (workers block at `.wait()`), set on resume (workers unblock). Cancel sets a boolean flag checked in each chunk loop.

```
DownloadTask
├── _pause_event  (threading.Event)  → pause/resume all segments
├── _cancel_flag  (bool)             → stop all segments
├── _lock         (threading.Lock)   → protect self.downloaded counter
└── segments[0..N-1]
    └── each runs in its own threading.Thread
```

## Queue Management

```
MAX_CONCURRENT = 3  (configurable via Settings tab in UI)

[downloading] [downloading] [downloading] ← slots full
[queued #1]  ← waits
[queued #2]  ← waits

When one finishes → _promote_queued() → queued #1 starts automatically
```

## Resume After Restart

```
On every chunk write  →  queue.json updated
                             ↓
                     { id, url, filename,
                       segments: [{start, end, written, done}] }

On app restart → _restore_queue() reads queue.json
              → tasks restored as status=paused
              → user clicks Resume → continues from exact byte offset
```

## Bandwidth Limiting

Each segment worker calls `_write_chunk()` which:
1. Records time before writing
2. Writes the chunk and increments the progress counter
3. Calculates `expected_time = chunk_size / bandwidth_limit`
4. Sleeps for `max(0, expected_time - actual_elapsed)`

This produces a smooth, accurate per-download speed cap.

---

## Evaluation Criteria Coverage

| Criterion | Weight | Evidence |
|---|---|---|
| Architecture design | 20% | Layered diagram, component table, README |
| Correct implementation | 30% | All core + all optional features working |
| Multithreading efficiency | 20% | N-thread segmented download, Lock, Event, queue |
| Code quality | 15% | Clean classes, retry backoff, separation of concerns |
| Report & explanation | 15% | Thread model, queue model, resume model documented above |

---

*Distributed Systems · Academic Year 2025–2026 · MedTech*  
*Group: Saloua Chouihi, Seifallah Chourou, Ghada Bezine*