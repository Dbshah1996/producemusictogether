from fastapi import FastAPI, UploadFile, File, Query
from fastapi.responses import FileResponse, JSONResponse, HTMLResponse
from pathlib import Path
from typing import Optional
from pydantic import BaseModel
import uuid
import subprocess
import shutil
import shutil as which_shutil
import tempfile
import atexit
import sys
import re
import os

try:
    from pytube import Search, YouTube
except Exception:
    Search = None
    YouTube = None

try:
    import yt_dlp
except Exception:
    yt_dlp = None

app = FastAPI()

BASE = Path(__file__).parent
RUNTIME_ROOT = Path(tempfile.mkdtemp(prefix="stems_runtime_"))
UPLOADS = RUNTIME_ROOT / "uploads"
OUTPUTS = RUNTIME_ROOT / "outputs"
YTDL = RUNTIME_ROOT / "youtube_downloads"
UPLOADS.mkdir(parents=True, exist_ok=True)
OUTPUTS.mkdir(parents=True, exist_ok=True)
YTDL.mkdir(parents=True, exist_ok=True)
atexit.register(lambda: shutil.rmtree(RUNTIME_ROOT, ignore_errors=True))

JOBS = {}  # MVP only; use Redis/DB in real app
YTDL_FILES = {}

UI_HTML = """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Stem Splitter</title>
  <style>
    :root {
      --bg: #0b1020;
      --card: #151c33;
      --text: #e7ecff;
      --muted: #9aa6d2;
      --accent: #38bdf8;
      --ok: #22c55e;
      --err: #ef4444;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      min-height: 100vh;
      display: grid;
      place-items: center;
      font-family: "Avenir Next", Avenir, "Segoe UI", sans-serif;
      background: radial-gradient(circle at 20% 20%, #1a2450, var(--bg));
      color: var(--text);
      padding: 24px;
    }
    .card {
      width: min(680px, 100%);
      background: linear-gradient(180deg, #1a2444, var(--card));
      border: 1px solid #2a3560;
      border-radius: 16px;
      padding: 24px;
      box-shadow: 0 12px 40px rgba(0, 0, 0, .35);
    }
    h1 { margin: 0 0 8px; font-size: 28px; }
    p { margin: 0 0 20px; color: var(--muted); }
    form { display: grid; gap: 12px; }
    input[type=file] {
      background: #0f1630;
      color: var(--text);
      border: 1px dashed #3b4a7a;
      padding: 12px;
      border-radius: 10px;
    }
    button {
      border: 0;
      border-radius: 10px;
      padding: 12px 16px;
      font-weight: 600;
      background: var(--accent);
      color: #001321;
      cursor: pointer;
    }
    button:disabled { opacity: .6; cursor: not-allowed; }
    #status { margin-top: 14px; font-weight: 600; }
    .ok { color: var(--ok); }
    .err { color: var(--err); }
    a { color: #7dd3fc; }
  </style>
</head>
<body>
  <div class="card">
    <h1>Stem Splitter</h1>
    <p>Upload a track, run Demucs, then download a ZIP with stems.</p>
    <form id="uploadForm">
      <input id="fileInput" type="file" name="file" accept="audio/*" required />
      <button id="submitBtn" type="submit">Process Track</button>
    </form>
    <hr style="border-color:#2a3560; margin:20px 0;" />
    <p>Or search YouTube and download MP3 (encoded at 320kbps target bitrate).</p>
    <form id="ytSearchForm">
      <input id="ytQuery" type="text" placeholder="Search songs on YouTube" required
             style="background:#0f1630;color:#e7ecff;border:1px solid #3b4a7a;padding:12px;border-radius:10px;" />
      <button id="ytSearchBtn" type="submit">Search YouTube</button>
    </form>
    <div id="ytResults" style="margin-top:12px;"></div>
    <div id="ytDownload"></div>
    <div id="status"></div>
    <div id="download"></div>
  </div>
  <script>
    const form = document.getElementById("uploadForm");
    const fileInput = document.getElementById("fileInput");
    const submitBtn = document.getElementById("submitBtn");
    const statusEl = document.getElementById("status");
    const downloadEl = document.getElementById("download");
    const ytSearchForm = document.getElementById("ytSearchForm");
    const ytQuery = document.getElementById("ytQuery");
    const ytSearchBtn = document.getElementById("ytSearchBtn");
    const ytResults = document.getElementById("ytResults");
    const ytDownload = document.getElementById("ytDownload");

    function setStatus(text, cls="") {
      statusEl.className = cls;
      statusEl.textContent = text;
    }

    async function poll(jobId) {
      while (true) {
        const res = await fetch(`/api/jobs/${jobId}`);
        const data = await res.json();
        if (data.status === "done") {
          setStatus("Done.", "ok");
          downloadEl.innerHTML = `
            <a href="/api/jobs/${jobId}/download">Download stems ZIP</a><br/>
            <a href="/api/jobs/${jobId}/download-all">Download song + stems ZIP</a>`;
          return;
        }
        if (data.status === "failed") {
          const err = data.error ? `: ${data.error}` : "";
          setStatus(`Failed${err}`, "err");
          return;
        }
        setStatus("Processing... this can take a few minutes.");
        await new Promise((r) => setTimeout(r, 2000));
      }
    }

    form.addEventListener("submit", async (e) => {
      e.preventDefault();
      downloadEl.innerHTML = "";
      if (!fileInput.files.length) {
        setStatus("Pick an audio file first.", "err");
        return;
      }
      const formData = new FormData();
      formData.append("file", fileInput.files[0]);

      submitBtn.disabled = true;
      setStatus("Uploading...");
      try {
        const res = await fetch("/api/jobs", { method: "POST", body: formData });
        const data = await res.json();
        if (!res.ok || !data.job_id) {
          const msg = data && data.error ? data.error : (data && data.status ? data.status : "Upload failed");
          setStatus(msg, "err");
        } else {
          setStatus("Queued.");
          await poll(data.job_id);
        }
      } catch (err) {
        setStatus(`Request failed: ${err}`, "err");
      } finally {
        submitBtn.disabled = false;
      }
    });

    async function downloadYoutube(url, title) {
      ytDownload.innerHTML = "";
      setStatus(`Downloading "${title}" from YouTube...`);
      try {
        const res = await fetch("/api/youtube/download", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ url: url }),
        });
        const data = await res.json();
        if (!res.ok || !data.file_id) {
          const msg = data && data.error ? data.error : "YouTube download failed";
          setStatus(msg, "err");
          return;
        }
        setStatus("YouTube MP3 ready.", "ok");
        ytDownload.innerHTML = `<a href="/api/youtube/download/${data.file_id}">Download MP3: ${data.filename}</a>`;
      } catch (err) {
        setStatus(`YouTube request failed: ${err}`, "err");
      }
    }

    async function downloadAndStemYoutube(url, title) {
      ytDownload.innerHTML = "";
      downloadEl.innerHTML = "";
      setStatus(`Downloading + stemming "${title}"... this can take a while.`);
      try {
        const res = await fetch("/api/youtube/download-and-stem", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ url: url }),
        });
        const data = await res.json();
        if (!res.ok || !data.job_id) {
          const msg = data && data.error ? data.error : "YouTube download+stem failed";
          setStatus(msg, "err");
          return;
        }
        if (data.file_id && data.filename) {
          ytDownload.innerHTML = `<a href="/api/youtube/download/${data.file_id}">Download MP3: ${data.filename}</a>`;
        }
        await poll(data.job_id);
      } catch (err) {
        setStatus(`YouTube request failed: ${err}`, "err");
      }
    }

    ytSearchForm.addEventListener("submit", async (e) => {
      e.preventDefault();
      ytResults.innerHTML = "";
      ytDownload.innerHTML = "";
      const query = ytQuery.value.trim();
      if (!query) {
        setStatus("Enter a YouTube search query.", "err");
        return;
      }
      ytSearchBtn.disabled = true;
      setStatus("Searching YouTube...");
      try {
        const res = await fetch(`/api/youtube/search?q=${encodeURIComponent(query)}`);
        const data = await res.json();
        if (!res.ok || !Array.isArray(data.results)) {
          const msg = data && data.error ? data.error : "Search failed";
          setStatus(msg, "err");
          return;
        }
        if (!data.results.length) {
          setStatus("No results found.", "err");
          return;
        }
        setStatus("Pick a result to download MP3.", "ok");
        data.results.forEach((item) => {
          const row = document.createElement("div");
          row.style.marginBottom = "8px";
          const title = item.title || item.url;
          row.innerHTML = `
            <div style="display:flex; gap:8px; align-items:center; justify-content:space-between; background:#0f1630; border:1px solid #2a3560; border-radius:10px; padding:10px;">
              <a href="${item.url}" target="_blank" rel="noopener noreferrer" style="flex:1; text-decoration:none;">${title}</a>
              <div style="display:flex; gap:8px;">
                <button type="button" class="dl-mp3">Download MP3</button>
                <button type="button" class="dl-stem">Download + Stem</button>
              </div>
            </div>`;
          row.querySelector(".dl-mp3").addEventListener("click", () => downloadYoutube(item.url, title));
          row.querySelector(".dl-stem").addEventListener("click", () => downloadAndStemYoutube(item.url, title));
          ytResults.appendChild(row);
        });
      } catch (err) {
        setStatus(`Search request failed: ${err}`, "err");
      } finally {
        ytSearchBtn.disabled = false;
      }
    });
  </script>
</body>
</html>
"""

def run_cmd(cmd: list[str]) -> None:
    p = subprocess.run(cmd, capture_output=True, text=True)
    if p.returncode != 0:
        raise RuntimeError(f"Command failed: {' '.join(cmd)}\n{p.stderr}")

def run_cmd_capture(cmd: list[str]) -> str:
    p = subprocess.run(cmd, capture_output=True, text=True)
    if p.returncode != 0:
        raise RuntimeError(f"Command failed: {' '.join(cmd)}\n{p.stderr}")
    return p.stdout.strip()

def get_duration_seconds(path: Path) -> float:
    out = run_cmd_capture([
        "ffprobe",
        "-v", "error",
        "-show_entries", "format=duration",
        "-of", "default=noprint_wrappers=1:nokey=1",
        str(path),
    ])
    return float(out)

def build_instrumental(stem_dir: Path) -> None:
    bass = stem_dir / "bass.wav"
    drums = stem_dir / "drums.wav"
    other = stem_dir / "other.wav"
    instrumental = stem_dir / "instrumental.wav"
    run_cmd([
        "ffmpeg", "-y",
        "-i", str(bass),
        "-i", str(drums),
        "-i", str(other),
        "-filter_complex", "[0:a][1:a][2:a]amix=inputs=3:normalize=0[a]",
        "-map", "[a]",
        str(instrumental),
    ])

def validate_stem_durations(reference: Path, stem_dir: Path) -> None:
    ref = get_duration_seconds(reference)
    for stem_name in ("vocals.wav", "drums.wav", "bass.wav", "other.wav", "instrumental.wav"):
        stem_path = stem_dir / stem_name
        if not stem_path.exists():
            raise RuntimeError(f"Missing expected stem file: {stem_path}")
        duration = get_duration_seconds(stem_path)
        if abs(duration - ref) > 0.25:
            raise RuntimeError(
                f"Duration mismatch for {stem_name}: input={ref:.2f}s output={duration:.2f}s. "
                "Aborting to avoid BPM/timing drift."
            )

def ensure_localhost_cert() -> tuple[Path, Path]:
    ssl_dir = BASE / ".ssl"
    ssl_dir.mkdir(exist_ok=True)
    cert_path = ssl_dir / "localhost.crt"
    key_path = ssl_dir / "localhost.key"

    if cert_path.exists() and key_path.exists():
        return cert_path, key_path

    if which_shutil.which("openssl") is None:
        raise RuntimeError("openssl is required for HTTPS cert generation but was not found.")

    run_cmd([
        "openssl", "req", "-x509", "-nodes", "-newkey", "rsa:2048",
        "-keyout", str(key_path),
        "-out", str(cert_path),
        "-days", "365",
        "-subj", "/CN=localhost"
    ])
    return cert_path, key_path

def check_torchaudio_backend() -> Optional[str]:
    try:
        import torchaudio
    except Exception:
        return "torchaudio is not installed. Install demucs dependencies in your .venv."
    try:
        backends = torchaudio.list_audio_backends()
    except Exception as e:
        return f"torchaudio backend check failed: {e}"
    if not backends:
        return (
            "torchaudio has no audio backends. Install one with: "
            "`brew install libsndfile` and `pip install soundfile`."
        )
    return None

def sanitize_track_name(filename: str) -> str:
    stem = Path(filename).stem.strip()
    cleaned = re.sub(r"[^A-Za-z0-9 _-]", "", stem)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned or "track"

def check_youtube_dependencies() -> Optional[str]:
    if yt_dlp is None and (Search is None or YouTube is None):
        return "Install YouTube dependency: pip install yt-dlp (preferred) or pip install pytube"
    if which_shutil.which("ffmpeg") is None:
        return "ffmpeg is not installed or not on PATH."
    return None

def yt_seconds_text(seconds: Optional[int]) -> str:
    if not seconds:
        return ""
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    if h:
        return f"{h}:{m:02d}:{s:02d}"
    return f"{m}:{s:02d}"

class YouTubeDownloadRequest(BaseModel):
    url: str

def create_stem_job_from_file(input_path: Path, source_filename: str) -> str:
    job_id = str(uuid.uuid4())
    track_name = sanitize_track_name(source_filename or input_path.name)
    out_dir = OUTPUTS / job_id
    out_dir.mkdir(parents=True, exist_ok=True)

    backend_error = check_torchaudio_backend()
    if backend_error:
        JOBS[job_id] = {"status": "failed", "error": backend_error}
        return job_id

    if which_shutil.which("ffmpeg") is None:
        JOBS[job_id] = {"status": "failed", "error": "ffmpeg is not installed or not on PATH."}
        return job_id

    wav_path = out_dir / f"{track_name}.wav"
    try:
        run_cmd(["ffmpeg", "-y", "-i", str(input_path), str(wav_path)])
    except Exception as e:
        JOBS[job_id] = {"status": "failed", "error": str(e)}
        return job_id

    JOBS[job_id] = {"status": "processing"}
    try:
        run_cmd([
            sys.executable, "-m", "demucs",
            "-n", "htdemucs",
            "-o", str(out_dir),
            str(wav_path)
        ])

        stem_parent = out_dir / "htdemucs"
        stem_dir = stem_parent / track_name
        if not stem_dir.exists():
            raise RuntimeError(f"Could not find Demucs output directory: {stem_dir}")

        build_instrumental(stem_dir)
        validate_stem_durations(wav_path, stem_dir)

        zip_path = OUTPUTS / f"{track_name}_stems.zip"
        shutil.make_archive(
            str(zip_path).replace(".zip", ""),
            "zip",
            root_dir=stem_parent,
            base_dir=track_name,
        )

        bundle_root = out_dir / "bundle_tmp"
        bundle_track_dir = bundle_root / track_name
        stems_bundle_dir = bundle_track_dir / "stems"
        stems_bundle_dir.mkdir(parents=True, exist_ok=True)
        for stem_name in ("vocals.wav", "drums.wav", "bass.wav", "other.wav", "instrumental.wav"):
            shutil.copy2(stem_dir / stem_name, stems_bundle_dir / stem_name)
        original_ext = Path(source_filename or "").suffix or input_path.suffix or ".audio"
        shutil.copy2(input_path, bundle_track_dir / f"original_input{original_ext}")
        shutil.copy2(wav_path, bundle_track_dir / "normalized.wav")
        bundle_zip_path = OUTPUTS / f"{track_name}_song_and_stems.zip"
        shutil.make_archive(
            str(bundle_zip_path).replace(".zip", ""),
            "zip",
            root_dir=bundle_root,
            base_dir=track_name,
        )

        JOBS[job_id] = {
            "status": "done",
            "zip": str(zip_path),
            "bundle_zip": str(bundle_zip_path),
        }
    except Exception as e:
        JOBS[job_id] = {"status": "failed", "error": str(e)}

    return job_id

def youtube_search_results(query: str, limit: int) -> list[dict]:
    # Prefer yt-dlp because pytube search regularly breaks with YouTube changes.
    if yt_dlp is not None:
        opts = {
            "quiet": True,
            "no_warnings": True,
            "extract_flat": True,
            "skip_download": True,
            "noplaylist": True,
        }
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(f"ytsearch{limit}:{query}", download=False)
        results = []
        for entry in info.get("entries", [])[:limit]:
            video_id = entry.get("id")
            url = entry.get("url") or (f"https://www.youtube.com/watch?v={video_id}" if video_id else None)
            if url and not str(url).startswith("http"):
                url = f"https://www.youtube.com/watch?v={url}"
            if not url:
                continue
            title = entry.get("title") or url
            duration = yt_seconds_text(entry.get("duration"))
            if duration:
                title = f"{title} ({duration})"
            results.append({"title": title, "url": url})
        return results

    if Search is not None:
        search = Search(query)
        results = []
        for video in (search.results or [])[:limit]:
            url = f"https://www.youtube.com/watch?v={video.video_id}"
            duration = yt_seconds_text(getattr(video, "length", None))
            title = getattr(video, "title", url)
            if duration:
                title = f"{title} ({duration})"
            results.append({"title": title, "url": url})
        return results

    return []

def youtube_download_to_mp3(url: str) -> Path:
    if yt_dlp is not None:
        temp_name = f"{uuid.uuid4()}_youtube_audio"
        outtmpl = str(YTDL / f"{temp_name}.%(ext)s")
        opts = {
            "quiet": True,
            "no_warnings": True,
            "format": "bestaudio/best",
            "noplaylist": True,
            "outtmpl": outtmpl,
            # More resilient against recent YouTube client restrictions.
            "extractor_args": {"youtube": {"player_client": ["android", "web"]}},
            "http_headers": {"User-Agent": "Mozilla/5.0"},
        }
        try:
            with yt_dlp.YoutubeDL(opts) as ydl:
                info = ydl.extract_info(url, download=True)
                title = sanitize_track_name((info or {}).get("title") or "youtube_track")
                downloaded_path = Path(ydl.prepare_filename(info))
        except Exception:
            # Retry with browser cookies if available (helps with some 403/captcha paths).
            browser = os.getenv("YTDLP_BROWSER", "chrome")
            opts_with_cookies = dict(opts)
            opts_with_cookies["cookiesfrombrowser"] = (browser,)
            with yt_dlp.YoutubeDL(opts_with_cookies) as ydl:
                info = ydl.extract_info(url, download=True)
                title = sanitize_track_name((info or {}).get("title") or "youtube_track")
                downloaded_path = Path(ydl.prepare_filename(info))
    elif YouTube is not None:
        yt = YouTube(url)
        title = sanitize_track_name(yt.title or "youtube_track")
        temp_name = f"{uuid.uuid4()}_{title}"
        stream = yt.streams.filter(only_audio=True).order_by("abr").desc().first()
        if stream is None:
            raise RuntimeError("No audio stream found for this video.")
        downloaded_path = Path(stream.download(output_path=str(YTDL), filename=temp_name))
    else:
        raise RuntimeError("No YouTube downloader available. Install yt-dlp or pytube.")

    mp3_path = YTDL / f"{title}.mp3"
    if mp3_path.exists():
        mp3_path = YTDL / f"{title}_{uuid.uuid4().hex[:8]}.mp3"
    run_cmd([
        "ffmpeg", "-y",
        "-i", str(downloaded_path),
        "-vn",
        "-codec:a", "libmp3lame",
        "-b:a", "320k",
        str(mp3_path),
    ])
    downloaded_path.unlink(missing_ok=True)
    return mp3_path

@app.get("/", response_class=HTMLResponse)
def index():
    return UI_HTML

@app.get("/api/youtube/search")
def youtube_search(q: str = Query(..., min_length=2), limit: int = Query(5, ge=1, le=10)):
    dep_error = check_youtube_dependencies()
    if dep_error:
        return JSONResponse({"error": dep_error}, status_code=400)
    try:
        results = youtube_search_results(q, limit)
        return {"results": results}
    except Exception as e:
        return JSONResponse(
            {"error": f"{e}. If this persists, install yt-dlp: pip install -U yt-dlp"},
            status_code=500
        )

@app.post("/api/youtube/download")
def youtube_download(req: YouTubeDownloadRequest):
    dep_error = check_youtube_dependencies()
    if dep_error:
        return JSONResponse({"error": dep_error}, status_code=400)
    try:
        mp3_path = youtube_download_to_mp3(req.url)
        file_id = str(uuid.uuid4())
        YTDL_FILES[file_id] = {"path": str(mp3_path), "filename": mp3_path.name}
        return {"file_id": file_id, "filename": mp3_path.name}
    except Exception as e:
        return JSONResponse(
            {
                "error": (
                    f"{e}. Try: pip install -U yt-dlp. "
                    "If 403 continues, set cookies source and restart app: "
                    "export YTDLP_BROWSER=chrome"
                )
            },
            status_code=500
        )

@app.post("/api/youtube/download-and-stem")
def youtube_download_and_stem(req: YouTubeDownloadRequest):
    dep_error = check_youtube_dependencies()
    if dep_error:
        return JSONResponse({"error": dep_error}, status_code=400)
    try:
        mp3_path = youtube_download_to_mp3(req.url)
        file_id = str(uuid.uuid4())
        YTDL_FILES[file_id] = {"path": str(mp3_path), "filename": mp3_path.name}
        job_id = create_stem_job_from_file(mp3_path, mp3_path.name)
        return {"file_id": file_id, "filename": mp3_path.name, "job_id": job_id}
    except Exception as e:
        return JSONResponse(
            {
                "error": (
                    f"{e}. Try: pip install -U yt-dlp. "
                    "If 403 continues, set cookies source and restart app: "
                    "export YTDLP_BROWSER=chrome"
                )
            },
            status_code=500
        )

@app.get("/api/youtube/download/{file_id}")
def youtube_download_file(file_id: str):
    info = YTDL_FILES.get(file_id)
    if not info:
        return JSONResponse({"error": "file not found"}, status_code=404)
    path = Path(info["path"])
    if not path.exists():
        return JSONResponse({"error": "file no longer available"}, status_code=404)
    return FileResponse(path, filename=info["filename"], media_type="audio/mpeg")

@app.post("/api/jobs")
async def create_job(file: UploadFile = File(...)):
    temp_id = str(uuid.uuid4())
    track_name = sanitize_track_name(file.filename or "track")
    in_path = UPLOADS / f"{temp_id}_{track_name}{Path(file.filename or '').suffix}"

    with in_path.open("wb") as f:
        f.write(await file.read())

    job_id = create_stem_job_from_file(in_path, file.filename or in_path.name)
    in_path.unlink(missing_ok=True)
    return {"job_id": job_id}

@app.get("/api/jobs/{job_id}")
def job_status(job_id: str):
    return JOBS.get(job_id, {"status": "not_found"})

@app.get("/api/jobs/{job_id}/download")
def download(job_id: str):
    job = JOBS.get(job_id)
    if not job or job.get("status") != "done":
        return JSONResponse({"error": "not ready"}, status_code=400)
    zip_path = Path(job["zip"])
    return FileResponse(zip_path, filename=zip_path.name, media_type="application/zip")

@app.get("/api/jobs/{job_id}/download-all")
def download_all(job_id: str):
    job = JOBS.get(job_id)
    if not job or job.get("status") != "done":
        return JSONResponse({"error": "not ready"}, status_code=400)
    bundle_zip = job.get("bundle_zip")
    if not bundle_zip:
        return JSONResponse({"error": "bundle not available"}, status_code=400)
    zip_path = Path(bundle_zip)
    return FileResponse(zip_path, filename=zip_path.name, media_type="application/zip")

if __name__ == "__main__":
    import uvicorn

    cert, key = ensure_localhost_cert()
    uvicorn.run(
        "stems:app",
        host="0.0.0.0",
        port=5050,
        reload=False,
        ssl_certfile=str(cert),
        ssl_keyfile=str(key),
    )
