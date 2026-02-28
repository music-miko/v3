import asyncio
import os
import re
import uuid
import aiohttp
import aiofiles
from pathlib import Path
from typing import Optional, Any

from pyrogram import errors
from anony import app, config, logger

# === Polling & Request Settings ===
JOB_POLL_ATTEMPTS = 15     
JOB_POLL_INTERVAL = 2.0    
JOB_POLL_BACKOFF = 1.2     
HARD_TIMEOUT = 80          

V2_DOWNLOAD_CYCLES = 5
NO_CANDIDATE_WAIT = 4
CDN_RETRIES = 5
CDN_RETRY_DELAY = 2
CHUNK_SIZE = 1024 * 1024


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

    async def _download_cdn(self, url: str, out_path: str) -> bool:
        logger.info(f"🔗 Downloading from CDN: {url}")
        
        # Handle Telegram t.me links natively if the API returns them
        tg_match = re.match(r"https?://t\.me/([^/]+)/(\d+)", url)
        if tg_match:
            try:
                msg = await app.get_messages(message_ids=url)
                file_path = await msg.download(file_name=out_path)
                return True if file_path else False
            except errors.FloodWait as e:
                logger.warning(f"[FLOODWAIT] Sleeping {e.value}s before TG retry.")
                await asyncio.sleep(e.value)
                return await self._download_cdn(url, out_path)
            except Exception as e:
                logger.warning(f"[TG DOWNLOAD ERROR] {e}")
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
                logger.error(f"CDN Fail: {e}")
                if attempt < CDN_RETRIES: await asyncio.sleep(CDN_RETRY_DELAY)
        
        return False

    async def download_track(self, link: str, video: bool = False) -> Optional[str]:
        vid = self.extract_safe_id(link) or link 
        file_id = self.extract_safe_id(link) or uuid.uuid4().hex[:10]
        
        ext = "mp4" if video else "m4a"
        out_path = self.download_dir / f"{file_id}.{ext}"

        if out_path.exists() and out_path.stat().st_size > 0:
            return str(out_path)

        if not self.api_url or not self.api_key:
            logger.error("API Creds Missing")
            return None

        for cycle in range(1, V2_DOWNLOAD_CYCLES + 1):
            try:
                session = await self.get_http_session()
                url = f"{self.api_url}/youtube/v2/download"
                params = {"query": vid, "isVideo": str(video).lower(), "api_key": self.api_key}
                
                logger.info(f"📡 API Job Start (Cycle {cycle}): {vid}...")
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
                    logger.info(f"⏳ Polling Job: {job_id}")
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

                if await self._download_cdn(final_url, str(out_path)):
                    return str(out_path)
            
            except Exception as e:
                logger.error(f"API Cycle Error: {e}")
                if cycle < V2_DOWNLOAD_CYCLES: await asyncio.sleep(1)
        
        return None
