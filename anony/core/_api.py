import asyncio
import os
import re
import time
import uuid
import contextlib
import urllib.parse
import aiohttp
import aiofiles
from pathlib import Path
from typing import Optional, Any

from pyrogram import errors
from anony import app, config, logger, db

# === Polling & Request Settings ===
JOB_POLL_ATTEMPTS = 15     
JOB_POLL_INTERVAL = 2.0    
JOB_POLL_BACKOFF = 1.2     
HARD_TIMEOUT = 80          

V2_DOWNLOAD_CYCLES = 8
NO_CANDIDATE_WAIT = 4
CDN_RETRIES = 5
CDN_RETRY_DELAY = 2
CHUNK_SIZE = 1024 * 1024

# Circuit Breaker: Stores the timestamp when TG is allowed again
TG_FLOOD_COOLDOWN = 0.0


class FallenApi:
    def __init__(self):
        self.api_url = config.API_URL.rstrip("/")
        self.api_key = config.API_KEY
        self.download_dir = Path("downloads")
        self.download_dir.mkdir(parents=True, exist_ok=True)
        
        self._session: Optional[aiohttp.ClientSession] = None
        self._session_lock = asyncio.Lock()

    async def get_http_session(self) -> aiohttp.ClientSession:
        if self._session and not self._session.closed:
            return self._session
        async with self._session_lock:
            if self._session and not self._session.closed:
                return self._session
            timeout = aiohttp.ClientTimeout(total=HARD_TIMEOUT, sock_connect=10, sock_read=30)
            connector = aiohttp.TCPConnector(limit=100, ttl_dns_cache=300, enable_cleanup_closed=True)
            self._session = aiohttp.ClientSession(timeout=timeout, connector=connector)
            return self._session

    def _looks_like_status_text(self, s: Optional[str]) -> bool:
        if not s: return False
        low = s.lower()
        return any(x in low for x in ("download started", "background", "jobstatus", "job_id", "processing", "queued"))

    def _extract_candidate(self, obj: Any) -> Optional[str]:
        if obj is None: return None
        if isinstance(obj, str):
            s = obj.strip()
            return s if s else None
        if isinstance(obj, list) and obj:
            return self._extract_candidate(obj[0])
        if isinstance(obj, dict):
            job = obj.get("job")
            if isinstance(job, dict):
                res = job.get("result")
                if isinstance(res, dict):
                    for k in ("public_url", "cdnurl", "download_url", "url"):
                        v = res.get(k)
                        if isinstance(v, str) and v.strip(): return v.strip()
            for k in ("public_url", "cdnurl", "download_url", "url", "tg_link"):
                v = obj.get(k)
                if isinstance(v, str) and v.strip(): return v.strip()
            for wrap in ("result", "results", "data", "items"):
                v = obj.get(wrap)
                if v: return self._extract_candidate(v)
        return None

    def _normalize_url(self, candidate: str) -> Optional[str]:
        if not self.api_url or not candidate: return None
        c = candidate.strip()
        if c.startswith(("http://", "https://")): return c
        if c.startswith("/"):
            if c.startswith(("/root", "/home")): return None
            return f"{self.api_url}{c}"
        return f"{self.api_url}/{c.lstrip('/')}"

    def extract_safe_id(self, link: str) -> Optional[str]:
        try:
            if "v=" in link: vid = link.split("v=")[-1].split("&")[0]
            elif "youtu.be" in link: vid = link.split("/")[-1].split("?")[0]
            else: return None
            if re.match(r"^[a-zA-Z0-9_-]{11}$", vid): return vid
        except: pass
        return None

    async def _download_from_media_db(self, track_id: str, is_video: bool, file_id: str) -> Optional[str]:
        """Attempt to fetch from Telegram channel storage if available."""
        global TG_FLOOD_COOLDOWN
        
        media_channel_id = getattr(config, "MEDIA_CHANNEL_ID", None)
        if not media_channel_id or not track_id:
            return None

        if time.time() < TG_FLOOD_COOLDOWN:
            return None

        try:
            ch_id = int(media_channel_id)
        except ValueError:
            return None

        # Try searching DB for common extensions
        ext = "mp4" if is_video else "mp3"
        keys_to_try = [
            f"{track_id}.{ext}",
            track_id,
            f"{track_id}_{'v' if is_video else 'a'}",
            f"{track_id}_{'v' if is_video else 'a'}.{ext}",
        ]

        msg_id = None
        for k in keys_to_try:
            msg_id = await db.get_media_id(k, is_video)
            if msg_id:
                break

        if not msg_id:
            return None

        try:
            msg = await app.get_messages(ch_id, msg_id)
            if not msg:
                return None

            # DYNAMIC EXTENSION DETECTION (Reads the exact extension from Telegram)
            tg_ext = "mp4" if is_video else "mp3"
            for media_type in (msg.document, msg.audio, msg.video):
                if media_type and getattr(media_type, "file_name", None):
                    tg_ext = media_type.file_name.split('.')[-1].lower()
                    break
            
            # Fallback if extension is somehow weird
            if tg_ext not in ["mp3", "m4a", "webm", "mp4", "mkv"]:
                tg_ext = "mp4" if is_video else "mp3"

            final_path = str(self.download_dir / f"{file_id}.{tg_ext}")

            dl_res = await asyncio.wait_for(
                app.download_media(msg, file_name=final_path),
                timeout=HARD_TIMEOUT
            )

            if not dl_res or not os.path.exists(dl_res) or os.path.getsize(dl_res) <= 0:
                return None

            return dl_res

        except asyncio.TimeoutError:
            logger.error(f"❌ DB Timeout > {HARD_TIMEOUT}s | ID: {track_id}")
        except errors.FloodWait as e:
            TG_FLOOD_COOLDOWN = time.time() + e.value + 5
            logger.error(f"⚠️ FloodWait in DB Download. Cooldown: {e.value + 5}s")
        except Exception as e:
            logger.error(f"DB Download Error: {e}")

        return None

    async def _download_cdn(self, url: str, out_path: str) -> bool:
        # Handle Telegram t.me links natively if the API returns them
        tg_match = re.match(r"https?://t\.me/([^/]+)/(\d+)", url)
        if tg_match:
            try:
                msg = await app.get_messages(message_ids=url)
                file_path = await msg.download(file_name=out_path)
                return True if file_path else False
            except errors.FloodWait as e:
                await asyncio.sleep(e.value)
                return await self._download_cdn(url, out_path)
            except Exception as e:
                logger.error(f"[TG DOWNLOAD ERROR] {e}")
                return False

        # Handle Standard HTTP CDN links
        for attempt in range(1, CDN_RETRIES + 1):
            try:
                session = await self.get_http_session()
                async with session.get(url, timeout=HARD_TIMEOUT) as resp:
                    if resp.status != 200:
                        if attempt < CDN_RETRIES:
                            await asyncio.sleep(CDN_RETRY_DELAY)
                            continue
                        return False

                    async with aiofiles.open(out_path, "wb") as f:
                        async for chunk in resp.content.iter_chunked(CHUNK_SIZE):
                            if not chunk: break
                            await f.write(chunk)
                
                if os.path.exists(out_path) and os.path.getsize(out_path) > 0:
                    return True

            except asyncio.TimeoutError:
                if attempt < CDN_RETRIES: await asyncio.sleep(CDN_RETRY_DELAY)
            except Exception as e:
                if attempt == CDN_RETRIES:
                    logger.error(f"CDN Fail: {e}")
                await asyncio.sleep(CDN_RETRY_DELAY)
        
        return False

    async def download_track(self, link: str, video: bool = False) -> Optional[str]:
        vid = self.extract_safe_id(link) or link 
        file_id = self.extract_safe_id(link) or uuid.uuid4().hex[:10]
        
        # ------------------------------------------------------------------
        # --- NEW: DYNAMIC LOCAL DISK CHECK ---
        # ------------------------------------------------------------------
        # Check all possible extensions to see if ANY valid file already exists locally
        possible_exts = ["mp4", "mkv"] if video else ["mp3", "m4a", "webm"]
        for ext in possible_exts:
            check_path = self.download_dir / f"{file_id}.{ext}"
            if check_path.exists() and check_path.stat().st_size > 0:
                return str(check_path)

        # ------------------------------------------------------------------
        # --- DB DOWNLOAD CHECK ---
        # ------------------------------------------------------------------
        db_path = await self._download_from_media_db(vid, video, file_id)
        if db_path and os.path.exists(db_path):
            return db_path

        # ------------------------------------------------------------------
        # --- FALLBACK: POLLING API ---
        # ------------------------------------------------------------------
        if not self.api_url or not self.api_key:
            logger.error("API Creds Missing")
            return None

        for cycle in range(1, V2_DOWNLOAD_CYCLES + 1):
            try:
                session = await self.get_http_session()
                url = f"{self.api_url}/youtube/v2/download"
                params = {"query": vid, "isVideo": str(video).lower(), "api_key": self.api_key}
                
                async with session.get(url, params=params) as resp:
                    if resp.status != 200:
                        if cycle < V2_DOWNLOAD_CYCLES: await asyncio.sleep(1); continue
                        return None
                    data = await resp.json()

                candidate = self._extract_candidate(data)
                if candidate and self._looks_like_status_text(candidate):
                    candidate = None

                job_id = data.get("job_id")
                if isinstance(data.get("job"), dict):
                     job_id = data.get("job").get("id")

                if job_id and not candidate:
                    interval = JOB_POLL_INTERVAL
                    
                    for _ in range(JOB_POLL_ATTEMPTS):
                        await asyncio.sleep(interval)
                        status_url = f"{self.api_url}/youtube/jobStatus"
                        
                        try:
                            async with session.get(status_url, params={"job_id": job_id}) as s_resp:
                                if s_resp.status == 200:
                                    s_data = await s_resp.json()
                                    candidate = self._extract_candidate(s_data)
                                    if candidate and not self._looks_like_status_text(candidate):
                                        break
                                    
                                    job_data = s_data.get("job", {}) if isinstance(s_data, dict) else {}
                                    if job_data.get("status") == "error":
                                        logger.error(f"❌ Job Error: {job_data.get('error')}")
                                        break
                        except Exception:
                            pass
                        
                        interval *= JOB_POLL_BACKOFF
                
                if not candidate:
                    if cycle < V2_DOWNLOAD_CYCLES: await asyncio.sleep(NO_CANDIDATE_WAIT); continue
                    return None

                final_url = self._normalize_url(candidate)
                if not final_url:
                     if cycle < V2_DOWNLOAD_CYCLES: await asyncio.sleep(NO_CANDIDATE_WAIT); continue
                     return None

                # ------------------------------------------------------------------
                # --- NEW: DYNAMIC URL PARSING ---
                # ------------------------------------------------------------------
                # Read the actual extension provided by the API CDN
                parsed_path = urllib.parse.urlparse(final_url).path
                dynamic_ext = os.path.splitext(parsed_path)[1].lstrip('.').lower()
                
                # Default back to mp4/mp3 if the API sends weird/no extensions
                if not dynamic_ext or dynamic_ext not in ["mp3", "m4a", "webm", "mp4", "mkv"]:
                    dynamic_ext = "mp4" if video else "mp3"
                    
                dynamic_out_path = str(self.download_dir / f"{file_id}.{dynamic_ext}")

                if await self._download_cdn(final_url, dynamic_out_path):
                    return dynamic_out_path
            
            except Exception as e:
                logger.error(f"API Cycle Error: {e}")
                if cycle < V2_DOWNLOAD_CYCLES: await asyncio.sleep(1)
        
        return None
