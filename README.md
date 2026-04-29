# Simple Download Manager (SDM)

**Distributed Systems Project — MedTech**

---

## Architecture

SDM uses a **Client–Server layered architecture**:

```
┌─────────────────────────────────────┐
│          Browser UI (Client)        │  ← HTML/CSS/JS, polls /api/* every 800ms
└────────────────┬────────────────────┘
                 │ HTTP / REST
┌────────────────▼────────────────────┐
│        Flask REST API (Server)      │  ← Python, port 5000
├─────────────────────────────────────┤
│       Download Manager Core         │  ← DownloadTask class
│  ┌──────────┐  ┌─────────────────┐  │
│  │ Thread   │  │ Segment Workers │  │  ← N threads (default 4)
│  │Controller│  │ (HTTP Range)    │  │
│  └──────────┘  └─────────────────┘  │
├─────────────────────────────────────┤
│  File Assembler  │  Persistence     │  ← merge segments │ history.json
└─────────────────────────────────────┘
```

## System Components

| Component | Description |
|---|---|
| **Flask REST API** | Exposes `/api/download`, `/api/downloads`, `/api/history` |
| **DownloadTask** | Manages lifecycle of a single download (start/pause/resume/cancel) |
| **Thread Controller** | Spawns N worker threads per download |
| **Segment Workers** | Each downloads a byte range using HTTP `Range` header |
| **File Assembler** | Merges segment files into the final file after all threads finish |
| **Persistence Module** | Saves completed download metadata to `history/history.json` |
| **Web UI** | Single-page app polling the API, shows real-time progress |

## Communication

- **UI ↔ Server**: HTTP REST (JSON)
- **Threads**: Python `threading.Thread`, `threading.Event` for pause/resume
- **Segment sync**: `threading.Lock` for safe progress accumulation

## How Multi-Threading Works

1. Server sends `HEAD` request → gets `Content-Length` and `Accept-Ranges: bytes`
2. File is split into N equal byte ranges
3. N threads start simultaneously, each with its own `Range: bytes=X-Y` header
4. Each thread writes to a temp segment file (`{id}_seg{i}`)
5. After all threads join, segments are merged in order into the final file
6. Temp files are deleted

## Setup & Run

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Run the server
python app.py

# 3. Open browser
# → http://localhost:5000
```

## API Endpoints

| Method | Endpoint | Description |
|---|---|---|
| POST | `/api/download` | Add a new download |
| GET | `/api/downloads` | List all active downloads |
| POST | `/api/downloads/{id}/pause` | Pause a download |
| POST | `/api/downloads/{id}/resume` | Resume a paused download |
| POST | `/api/downloads/{id}/cancel` | Cancel a download |
| DELETE | `/api/downloads/{id}` | Remove from list |
| GET | `/api/history` | Get download history |
| DELETE | `/api/history` | Clear history |

## Features

- ✅ Multi-threaded segmented downloads (HTTP Range requests)
- ✅ Pause / Resume (threading.Event)
- ✅ Auto-retry with exponential backoff (configurable)
- ✅ Download history (JSON persistence)
- ✅ Real-time progress: %, speed, ETA
- ✅ Error handling & cancellation
- ✅ Single-threaded fallback (when server doesn't support Range)
- ✅ Web UI with dark theme

## Project Structure

```
sdm/
├── app.py              # Flask backend + DownloadTask class
├── requirements.txt
├── README.md
├── static/
│   └── index.html      # Web UI (single file)
├── downloads/          # Downloaded files saved here
└── history/
    └── history.json    # Persistent download history
```
