"""High-performance asynchronous Internet Archive client."""

from __future__ import annotations
import asyncio
import hashlib
import json
import logging
import math
import os
import random
import time
from pathlib import Path
from typing import AsyncIterable, AsyncIterator, Callable, Dict, Any, List, Optional, Tuple, Union

import httpx

from .config import ArchivistConfig, S3Credentials

logger = logging.getLogger("archivist.client")


class TokenBucketRateLimiter:
    """Async token bucket rate limiter for per-domain throttle control."""

    def __init__(self, rate: float, capacity: float = 5.0):
        self.rate = rate  # tokens per second
        self.capacity = capacity
        self.tokens = capacity
        self.last_update = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self, tokens: float = 1.0) -> None:
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self.last_update
            self.last_update = now
            self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)

            if self.tokens < tokens:
                wait_time = (tokens - self.tokens) / self.rate
                await asyncio.sleep(wait_time)
                self.tokens = 0.0
                self.last_update = time.monotonic()
            else:
                self.tokens -= tokens


class ArchiveError(Exception):
    """Base exception for Archive.org operations."""
    def __init__(self, message: str, status_code: Optional[int] = None, response_text: Optional[str] = None):
        super().__init__(message)
        self.status_code = status_code
        self.response_text = response_text


class ArchiveRateLimitError(ArchiveError):
    """Raised when request is throttled (429 or 503 Slow Down)."""
    pass


class ArchiveAccessDeniedError(ArchiveError):
    """Raised when a request is refused as a matter of access, not of load.

    Retrying cannot satisfy it and rotating credentials cannot unlock it. The
    common case is an archive.org item whose text is lending-restricted: the
    derivative URL answers ``401 Authorization Required`` (nginx) and the full
    text is simply not downloadable. The item may still be quotable through the
    full-text index, which is a different service -- see
    :meth:`~archivist.fulltext.FullTextEngine.quote_from_index`.
    """
    pass


# --- HTTP outcome classification -------------------------------------------
#
# A bare status code is not enough to decide what to do next, and asserting a
# cause the code does not carry is how a lending restriction gets logged as a
# rate limit. Observed against archive.org:
#
#   * a lending-restricted text derivative answers 401 (nginx,
#     "401 Authorization Required") after the /download/ redirect -- terminal;
#   * 429 is throttling;
#   * 503 is the archive's "Slow Down";
#   * 403 is ambiguous in practice: it appears both for throttling and for a
#     credential or access refusal, and the status alone does not say which.
#     It is therefore reported as *unknown* rather than asserted to be either.
#
# The distinction matters because credential rotation helps for throttling and
# for a bad S3 key, and cannot help for an access decision.

OUTCOME_OK = "ok"
OUTCOME_RATE_LIMITED = "rate_limited"
OUTCOME_DENIED = "denied"
OUTCOME_REFUSED = "refused"
OUTCOME_SERVER_ERROR = "server_error"

#: Substrings that mark a 403 body as an access decision rather than load.
_DENIAL_MARKERS = (
    "authorization required",
    "lending",
    "restricted",
    "not available",
    "item not available",
)


def classify_status(status_code: int, headers: Any = None, body: str = "") -> str:
    """Classify an HTTP response by what is known, not by status alone.

    Pure: the retry loop, the reference checker and the tests all agree on what
    a status code means because there is one implementation.
    """
    if status_code < 400:
        return OUTCOME_OK
    if status_code in (429, 503):
        return OUTCOME_RATE_LIMITED
    if status_code in (401, 451):
        return OUTCOME_DENIED
    if status_code == 403:
        retry_after = ""
        try:
            retry_after = (headers or {}).get("retry-after", "") or ""
        except AttributeError:
            retry_after = ""
        if retry_after:
            return OUTCOME_RATE_LIMITED
        low = (body or "").lower()
        if any(marker in low for marker in _DENIAL_MARKERS):
            return OUTCOME_DENIED
        return OUTCOME_REFUSED
    if status_code >= 500:
        return OUTCOME_SERVER_ERROR
    return OUTCOME_OK


def _safe_body(response: Any, streaming: bool) -> str:
    """Best-effort body text for classification; never breaks a download."""
    if streaming:
        return ""
    try:
        return response.text[:400]
    except Exception:
        return ""


class ArchiveClient:
    """Unified asynchronous Archive.org API client covering S3, Metadata, Search, Wayback, and Tasks."""

    BASE_S3_URL = "https://s3.us.archive.org"
    BASE_METADATA_URL = "https://archive.org/metadata"
    BASE_SEARCH_URL = "https://archive.org/advancedsearch.php"
    BASE_SCRAPE_URL = "https://archive.org/services/search/v1/scrape"
    BASE_WAYBACK_CDX_URL = "https://web.archive.org/cdx/search/cdx"
    BASE_WAYBACK_SAVE_URL = "https://web.archive.org/save"
    BASE_WAYBACK_AVAIL_URL = "https://archive.org/wayback/available"
    BASE_TASKS_URL = "https://archive.org/services/tasks.php"
    BASE_DOWNLOAD_URL = "https://archive.org/download"

    def __init__(self, config: Optional[ArchivistConfig] = None, credentials: Optional[S3Credentials] = None):
        self.config = config or ArchivistConfig.load()
        from .rotation import KeyRotationManager
        self.rotation_manager = KeyRotationManager(self.config)
        self.credentials = credentials or self.rotation_manager.current_credentials
        self._session: Optional[httpx.AsyncClient] = None

        # Domain rate limiters
        self._s3_limiter = TokenBucketRateLimiter(rate=self.config.rate_limits.s3_requests_per_sec)
        self._wayback_limiter = TokenBucketRateLimiter(rate=self.config.rate_limits.wayback_requests_per_sec)
        self._search_limiter = TokenBucketRateLimiter(rate=self.config.rate_limits.search_requests_per_sec)

    async def __aenter__(self) -> "ArchiveClient":
        await self.get_session()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()

    async def get_session(self) -> httpx.AsyncClient:
        if self._session is None or self._session.is_closed:
            limits = httpx.Limits(max_keepalive_connections=50, max_connections=100)
            timeout = httpx.Timeout(self.config.timeout, connect=15.0, read=self.config.timeout)
            headers = {"User-Agent": self.config.user_agent}
            self._session = httpx.AsyncClient(limits=limits, timeout=timeout, headers=headers, follow_redirects=True)
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.is_closed:
            await self._session.aclose()
            self._session = None

    def _get_auth_header(self) -> Dict[str, str]:
        creds = self.rotation_manager.current_credentials or self.credentials
        if creds:
            return {"Authorization": creds.auth_header}
        return {}

    async def _request_with_retry(
        self,
        method: str,
        url: str,
        limiter: Optional[TokenBucketRateLimiter] = None,
        headers: Optional[Dict[str, str]] = None,
        **kwargs
    ) -> httpx.Response:
        session = await self.get_session()
        req_headers = dict(headers or {})
        retries = self.config.max_retries
        backoff = self.config.backoff_factor

        for attempt in range(retries + 1):
            if limiter:
                await limiter.acquire()

            try:
                response = await session.request(method, url, headers=req_headers, **kwargs)
                streaming = bool(kwargs.get("stream", False))
                body = _safe_body(response, streaming)
                outcome = classify_status(response.status_code, response.headers, body)

                # An access decision is terminal. Retrying it wastes quota and
                # rotating credentials cannot unlock a borrowed book, so this
                # is raised before any retry or rotation is considered.
                if outcome == OUTCOME_DENIED:
                    raise ArchiveAccessDeniedError(
                        f"Access denied (HTTP {response.status_code}) for {url}. "
                        "For an archive.org item this usually means the text is "
                        "lending-restricted and cannot be downloaded. Such items "
                        "are still indexed: passages can be retrieved with a "
                        "field query against the full-text API "
                        "(POST be-api.us.archive.org/ia-pub-fts-api, "
                        "q='identifier:<id> AND <term>').",
                        status_code=response.status_code,
                        response_text=body,
                    )

                if outcome in (OUTCOME_RATE_LIMITED, OUTCOME_REFUSED):
                    # Rotation helps for throttling and for a bad S3 key; both
                    # can surface as 403, which is why it stays available here.
                    if len(self.rotation_manager._pool) > 1 and not self.config.privacy_mode:
                        new_creds = await self.rotation_manager.rotate_to_next(
                            reason=f"HTTP {response.status_code} received on {url}"
                        )
                        if new_creds:
                            req_headers["Authorization"] = new_creds.auth_header
                            logger.info(f"Auto-rotated key to {new_creds.label} after HTTP {response.status_code}")
                            await asyncio.sleep(0.5)
                            continue

                    if attempt < retries:
                        sleep_time = (backoff ** attempt) + random.uniform(0.5, 2.0)
                        if outcome == OUTCOME_RATE_LIMITED:
                            logger.warning(f"Rate limited ({response.status_code}) on {url}. Retrying in {sleep_time:.2f}s...")
                        else:
                            logger.warning(
                                f"HTTP {response.status_code} on {url}: not disambiguable between "
                                f"throttling and an access or credential refusal; retrying in "
                                f"{sleep_time:.2f}s..."
                            )
                        await asyncio.sleep(sleep_time)
                        continue

                    if outcome == OUTCOME_RATE_LIMITED:
                        raise ArchiveRateLimitError(
                            f"Rate limit exceeded on {url} after {retries} retries",
                            status_code=response.status_code,
                            response_text=body,
                        )
                    raise ArchiveError(
                        f"HTTP {response.status_code} on {url} after {retries} retries; could "
                        "not be disambiguated between throttling and an access refusal",
                        status_code=response.status_code,
                        response_text=body,
                    )

                if outcome == OUTCOME_SERVER_ERROR:
                    if attempt < retries:
                        sleep_time = (backoff ** attempt) + random.uniform(0.5, 1.5)
                        logger.warning(f"Server error ({response.status_code}) on {url}. Retrying in {sleep_time:.2f}s...")
                        await asyncio.sleep(sleep_time)
                        continue

                return response

            except (httpx.ConnectError, httpx.ReadTimeout, httpx.NetworkError) as err:
                if attempt < retries:
                    sleep_time = (backoff ** attempt) + random.uniform(0.5, 1.5)
                    logger.warning(f"Network error ({err}) on {url}. Retrying in {sleep_time:.2f}s...")
                    await asyncio.sleep(sleep_time)
                    continue
                raise ArchiveError(f"Network connection failed for {url}: {err}") from err

        raise ArchiveError(f"Request failed after {retries} retries: {method} {url}")

    # ==========================================
    # S3 / Item Management Operations
    # ==========================================

    async def create_item(
        self,
        identifier: str,
        title: str,
        mediatype: Optional[str] = None,
        collection: Optional[str] = None,
        description: Optional[str] = None,
        creator: Optional[str] = None,
        date: Optional[str] = None,
        subject: Optional[Union[str, List[str]]] = None,
        extra_metadata: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """Create a new item (bucket) on Archive.org via IAS3."""
        if not self.credentials:
            raise ArchiveError("S3 credentials required to create Archive.org items.")

        url = f"{self.BASE_S3_URL}/{identifier}"
        headers = self._get_auth_header()
        headers["x-archive-auto-make-bucket"] = "1"
        headers["x-archive-meta-title"] = title
        headers["x-archive-meta-mediatype"] = mediatype or self.config.default_mediatype
        headers["x-archive-meta-collection"] = collection or self.config.default_collection

        if description:
            headers["x-archive-meta-description"] = description
        if creator:
            headers["x-archive-meta-creator"] = creator
        if date:
            headers["x-archive-meta-date"] = date
        if subject:
            if isinstance(subject, list):
                for idx, subj in enumerate(subject):
                    headers[f"x-archive-meta{idx:02d}-subject"] = subj
            else:
                headers["x-archive-meta-subject"] = subject

        if extra_metadata:
            for k, v in extra_metadata.items():
                clean_k = k.replace("_", "-")
                headers[f"x-archive-meta-{clean_k}"] = str(v)

        response = await self._request_with_retry("PUT", url, limiter=self._s3_limiter, headers=headers)
        if response.status_code not in (200, 201):
            raise ArchiveError(f"Failed to create item {identifier}: {response.status_code} {response.text}", response.status_code, response.text)

        return {"identifier": identifier, "status": "created", "url": f"https://archive.org/details/{identifier}"}

    async def upload_file(
        self,
        identifier: str,
        filename: str,
        file_path: Optional[Path] = None,
        content: Optional[bytes] = None,
        size_hint: Optional[int] = None,
        extra_headers: Optional[Dict[str, str]] = None,
        progress_callback: Optional[Callable[[int, int], None]] = None
    ) -> Dict[str, Any]:
        """Upload a file to an Archive.org item via IAS3 with checksum and progress support."""
        if not self.credentials:
            raise ArchiveError("S3 credentials required to upload files.")

        url = f"{self.BASE_S3_URL}/{identifier}/{filename}"
        headers = self._get_auth_header()
        headers["x-archive-interactive-priority"] = "1"

        if extra_headers:
            for k, v in extra_headers.items():
                headers[k] = v

        if file_path:
            file_path = Path(file_path)
            if not file_path.is_file():
                raise ArchiveError(f"Source file does not exist: {file_path}")
            total_size = file_path.stat().st_size
            headers["x-archive-size-hint"] = str(total_size)

            # Compute MD5 checksum
            hasher = hashlib.md5()
            with open(file_path, "rb") as f:
                while chunk := f.read(1024 * 1024):
                    hasher.update(chunk)
            headers["Content-MD5"] = hasher.hexdigest()

            # Stream upload with progress
            uploaded_bytes = 0
            async def file_generator() -> AsyncIterator[bytes]:
                nonlocal uploaded_bytes
                with open(file_path, "rb") as f:
                    while chunk := f.read(256 * 1024):
                        uploaded_bytes += len(chunk)
                        if progress_callback:
                            progress_callback(uploaded_bytes, total_size)
                        yield chunk

            response = await self._request_with_retry(
                "PUT", url, limiter=self._s3_limiter, headers=headers, content=file_generator()
            )
        elif content is not None:
            total_size = len(content)
            headers["x-archive-size-hint"] = str(total_size)
            headers["Content-MD5"] = hashlib.md5(content).hexdigest()
            if progress_callback:
                progress_callback(total_size, total_size)
            response = await self._request_with_retry(
                "PUT", url, limiter=self._s3_limiter, headers=headers, content=content
            )
        else:
            raise ArchiveError("Either file_path or content must be provided for upload.")

        if response.status_code not in (200, 201):
            raise ArchiveError(
                f"Failed to upload {filename} to {identifier}: {response.status_code} {response.text}",
                response.status_code, response.text
            )

        return {
            "identifier": identifier,
            "filename": filename,
            "size": total_size,
            "url": f"{self.BASE_DOWNLOAD_URL}/{identifier}/{filename}",
            "status": "uploaded"
        }

    async def download_file(
        self,
        identifier: str,
        filename: str,
        destination: Path,
        resume: bool = True,
        expected_size: Optional[int] = None,
        progress_callback: Optional[Callable[[int, int], None]] = None
    ) -> Path:
        """Stream download a file from an Archive.org item with HTTP Range resume support."""
        destination = Path(destination)
        destination.parent.mkdir(parents=True, exist_ok=True)
        url = f"{self.BASE_DOWNLOAD_URL}/{identifier}/{filename}"
        headers = self._get_auth_header()

        existing_bytes = 0
        if resume and destination.is_file():
            existing_bytes = destination.stat().st_size
            if expected_size and existing_bytes == expected_size:
                if progress_callback:
                    progress_callback(existing_bytes, existing_bytes)
                return destination
            if existing_bytes > 0:
                headers["Range"] = f"bytes={existing_bytes}-"

        session = await self.get_session()
        await self._s3_limiter.acquire()

        async with session.stream("GET", url, headers=headers) as response:
            if response.status_code == 416:  # Range not satisfiable (file already complete)
                return destination
            if response.status_code not in (200, 206):
                raise ArchiveError(
                    f"Download failed for {identifier}/{filename}: HTTP {response.status_code}",
                    response.status_code
                )

            total_size = int(response.headers.get("Content-Length", 0))
            if response.status_code == 206:
                total_size += existing_bytes
            elif response.status_code == 200:
                existing_bytes = 0

            mode = "ab" if existing_bytes > 0 else "wb"
            downloaded = existing_bytes

            with open(destination, mode) as f:
                async for chunk in response.aiter_bytes(chunk_size=128 * 1024):
                    f.write(chunk)
                    downloaded += len(chunk)
                    if progress_callback:
                        progress_callback(downloaded, total_size)

        return destination

    async def delete_file(self, identifier: str, filename: str) -> bool:
        """Delete a file from an item via IAS3."""
        if not self.credentials:
            raise ArchiveError("S3 credentials required to delete files.")
        url = f"{self.BASE_S3_URL}/{identifier}/{filename}"
        headers = self._get_auth_header()
        response = await self._request_with_retry("DELETE", url, limiter=self._s3_limiter, headers=headers)
        return response.status_code in (200, 204)

    # ==========================================
    # Metadata Operations
    # ==========================================

    async def get_metadata(self, identifier: str) -> Dict[str, Any]:
        """Fetch complete item metadata and file inventory."""
        url = f"{self.BASE_METADATA_URL}/{identifier}"
        response = await self._request_with_retry("GET", url, limiter=self._search_limiter)
        if response.status_code == 404:
            raise ArchiveError(f"Item not found: {identifier}", status_code=404)
        if response.status_code != 200:
            raise ArchiveError(f"Failed to fetch metadata for {identifier}: {response.status_code}", response.status_code)
        return response.json()

    async def modify_metadata(self, identifier: str, patch_operations: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Apply JSON Patch modifications to item metadata."""
        if not self.credentials:
            raise ArchiveError("S3 credentials required to modify metadata.")

        url = f"{self.BASE_METADATA_URL}/{identifier}"
        headers = self._get_auth_header()
        headers["Content-Type"] = "application/json"

        payload = {
            "-target": "metadata",
            "-patch": patch_operations
        }

        response = await self._request_with_retry(
            "POST", url, limiter=self._s3_limiter, headers=headers, json=payload
        )
        if response.status_code != 200:
            raise ArchiveError(
                f"Failed to patch metadata for {identifier}: {response.status_code} {response.text}",
                response.status_code
            )
        return response.json()

    # ==========================================
    # Search & Discovery Operations
    # ==========================================

    async def search(
        self,
        query: str,
        fields: Optional[List[str]] = None,
        rows: int = 50,
        page: int = 1,
        sorts: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Execute Lucene advanced search query against archive.org."""
        params: Dict[str, Any] = {
            "q": query,
            "output": "json",
            "rows": rows,
            "page": page
        }
        if fields:
            params["fl[]"] = fields
        else:
            params["fl[]"] = ["identifier", "title", "mediatype", "collection", "date", "downloads", "publicdate"]

        if sorts:
            params["sort[]"] = sorts

        response = await self._request_with_retry("GET", self.BASE_SEARCH_URL, limiter=self._search_limiter, params=params)
        if response.status_code != 200:
            raise ArchiveError(f"Search query failed: {response.status_code}", response.status_code)
        return response.json().get("response", {})

    async def scrape(
        self,
        query: str,
        fields: Optional[List[str]] = None,
        cursor: Optional[str] = None
    ) -> Dict[str, Any]:
        """Execute deep sweep query using Archive.org Scrape API (cursor-based pagination)."""
        params: Dict[str, Any] = {"q": query}
        if fields:
            params["fields"] = ",".join(fields)
        if cursor:
            params["cursor"] = cursor

        response = await self._request_with_retry("GET", self.BASE_SCRAPE_URL, limiter=self._search_limiter, params=params)
        if response.status_code != 200:
            raise ArchiveError(f"Scrape query failed: {response.status_code}", response.status_code)
        return response.json()

    # ==========================================
    # Wayback Machine Operations
    # ==========================================

    async def save_page_now(
        self,
        url: str,
        capture_all: bool = True,
        capture_outlinks: bool = False,
        capture_screenshot: bool = False
    ) -> Dict[str, Any]:
        """Submit URL to Save Page Now 2 (SPN2) with authenticated queueing and key auto-rotation."""
        headers = self._get_auth_header()
        headers["Accept"] = "application/json"

        data: Dict[str, Any] = {"url": url}
        if capture_all:
            data["capture_all"] = "1"
        if capture_outlinks:
            data["capture_outlinks"] = "1"
        if capture_screenshot:
            data["capture_screenshot"] = "1"

        response = await self._request_with_retry(
            "POST", f"{self.BASE_WAYBACK_SAVE_URL}/", limiter=self._wayback_limiter, headers=headers, data=data
        )

        try:
            result = response.json()
        except Exception:
            result = {"raw_text": response.text}

        # Check for quota exhaustion in JSON body
        status_ext = result.get("status_ext", "")
        if "too-many-daily-captures" in status_ext or "user-limit-exceeded" in status_ext:
            if len(self.rotation_manager._pool) > 1:
                new_key = await self.rotation_manager.rotate_to_next(reason=status_ext)
                if new_key:
                    logger.info(f"SPN2 quota hit. Re-trying save with rotated key {new_key.label}...")
                    new_headers = {"Authorization": new_key.auth_header, "Accept": "application/json"}
                    retry_res = await self._request_with_retry(
                        "POST", f"{self.BASE_WAYBACK_SAVE_URL}/", limiter=self._wayback_limiter, headers=new_headers, data=data
                    )
                    try:
                        return retry_res.json()
                    except Exception:
                        return {"raw_text": retry_res.text}

        if response.status_code == 200 and "job_id" in result:
            return result
        elif result.get("status") == "error":
            return result
        else:
            raise ArchiveError(f"SPN2 submission failed: {response.status_code} {response.text}", response.status_code)

    async def check_spn_status(self, job_id: str) -> Dict[str, Any]:
        """Query status of a Save Page Now 2 capture job."""
        headers = self._get_auth_header()
        headers["Accept"] = "application/json"
        url = f"{self.BASE_WAYBACK_SAVE_URL}/status/{job_id}"

        response = await self._request_with_retry("GET", url, limiter=self._wayback_limiter, headers=headers)
        if response.status_code != 200:
            raise ArchiveError(f"SPN2 status query failed for {job_id}: {response.status_code}", response.status_code)
        return response.json()

    async def wait_spn_completion(self, job_id: str, timeout: float = 60.0, poll_interval: float = 3.0) -> Dict[str, Any]:
        """Poll SPN2 status until job completes, fails, or times out."""
        start = time.monotonic()
        while time.monotonic() - start < timeout:
            status_data = await self.check_spn_status(job_id)
            status = status_data.get("status")
            if status in ("success", "error"):
                return status_data
            await asyncio.sleep(poll_interval)
        return {"status": "timeout", "job_id": job_id, "message": f"Timed out waiting after {timeout}s"}

    async def cdx_search(
        self,
        url: str,
        match_type: str = "exact",
        limit: int = 100,
        status_codes: Optional[List[int]] = None,
        from_ts: Optional[str] = None,
        to_ts: Optional[str] = None,
        collapse: Optional[str] = None,
        fl: Optional[List[str]] = None
    ) -> List[Dict[str, str]]:
        """Query Wayback CDX Server for historical snapshots."""
        params: Dict[str, Any] = {
            "url": url,
            "output": "json",
            "limit": limit
        }
        if match_type != "exact":
            params["matchType"] = match_type
        if from_ts:
            params["from"] = from_ts
        if to_ts:
            params["to"] = to_ts
        if collapse:
            params["collapse"] = collapse
        if fl:
            params["fl"] = ",".join(fl)

        if status_codes:
            for sc in status_codes:
                params["filter"] = f"statuscode:{sc}"

        response = await self._request_with_retry("GET", self.BASE_WAYBACK_CDX_URL, limiter=self._wayback_limiter, params=params)
        if response.status_code != 200:
            raise ArchiveError(f"CDX search failed: {response.status_code}", response.status_code)

        data = response.json()
        if not data or len(data) <= 1:
            return []

        header = data[0]
        results = []
        for row in data[1:]:
            results.append(dict(zip(header, row)))
        return results

    async def get_wayback_snapshot(self, url: str, timestamp: str, raw: bool = True) -> bytes:
        """Fetch historical snapshot content from Wayback Machine (raw=True uses id_ modifier)."""
        mode = "id_" if raw else ""
        snapshot_url = f"https://web.archive.org/web/{timestamp}{mode}/{url}"
        response = await self._request_with_retry("GET", snapshot_url, limiter=self._wayback_limiter)
        if response.status_code != 200:
            raise ArchiveError(f"Failed to fetch snapshot at {timestamp}: HTTP {response.status_code}", response.status_code)
        return response.content

    async def check_availability(self, url: str, timestamp: Optional[str] = None) -> Dict[str, Any]:
        """Check closest snapshot availability on Wayback Machine."""
        params = {"url": url}
        if timestamp:
            params["timestamp"] = timestamp
        response = await self._request_with_retry("GET", self.BASE_WAYBACK_AVAIL_URL, limiter=self._wayback_limiter, params=params)
        if response.status_code != 200:
            raise ArchiveError(f"Availability check failed: {response.status_code}", response.status_code)
        return response.json()

    # ==========================================
    # Task & Derivation Operations
    # ==========================================

    async def get_tasks(self, identifier: str) -> Dict[str, Any]:
        """Retrieve catalog task history and active background processing jobs for an item."""
        url = f"{self.BASE_TASKS_URL}?identifier={identifier}"
        response = await self._request_with_retry("GET", url, limiter=self._search_limiter)
        if response.status_code != 200:
            raise ArchiveError(f"Failed to fetch tasks for {identifier}: {response.status_code}", response.status_code)
        return response.json()

    # ==========================================
    # Internal & Advanced Unpublicized Endpoints
    # ==========================================

    async def get_user_spn_quota(self) -> Dict[str, Any]:
        """Retrieve authenticated SPN2 user capture limits, daily usage, and available slots."""
        url = f"{self.BASE_WAYBACK_SAVE_URL}/status/user"
        headers = self._get_auth_header()
        headers["Accept"] = "application/json"
        response = await self._request_with_retry("GET", url, limiter=self._wayback_limiter, headers=headers)
        if response.status_code != 200:
            raise ArchiveError(f"Failed to fetch SPN2 user quota: {response.status_code}", response.status_code)
        data = response.json()
        creds = self.rotation_manager.current_credentials or self.credentials
        if creds and isinstance(data, dict):
            await self.rotation_manager.update_key_quota(creds.access_key, data)
        return data

    async def get_system_spn_status(self) -> Dict[str, Any]:
        """Retrieve global Wayback Machine capture cluster status and queue backlog."""
        url = f"{self.BASE_WAYBACK_SAVE_URL}/status/system"
        response = await self._request_with_retry("GET", url, limiter=self._wayback_limiter)
        if response.status_code != 200:
            raise ArchiveError(f"Failed to fetch SPN2 system status: {response.status_code}", response.status_code)
        return response.json()

    async def get_item_views(self, identifier: str) -> Dict[str, Any]:
        """Retrieve real-time and all-time viewership traffic analytics from backend views API."""
        url = f"https://be-api.us.archive.org/views/v1/short/{identifier}"
        response = await self._request_with_retry("GET", url, limiter=self._search_limiter)
        if response.status_code != 200:
            raise ArchiveError(f"Failed to fetch views for {identifier}: {response.status_code}", response.status_code)
        return response.json().get(identifier, {})

    async def get_item_subpath(self, identifier: str, subpath: str) -> Any:
        """Fetch targeted metadata sub-resource (files, server, workable_servers, reviews)."""
        url = f"{self.BASE_METADATA_URL}/{identifier}/{subpath}"
        response = await self._request_with_retry("GET", url, limiter=self._search_limiter)
        if response.status_code != 200:
            raise ArchiveError(f"Failed to fetch {subpath} for {identifier}: {response.status_code}", response.status_code)
        data = response.json()
        return data.get("result", data)

    async def get_cdx_page_count(self, url: str) -> int:
        """Estimate total 10k-page blocks available in the Wayback CDX index for a target."""
        params = {"url": url, "showNumPages": "true"}
        response = await self._request_with_retry("GET", self.BASE_WAYBACK_CDX_URL, limiter=self._wayback_limiter, params=params)
        if response.status_code != 200:
            raise ArchiveError(f"Failed to fetch CDX page count: {response.status_code}", response.status_code)
        try:
            return int(response.text.strip())
        except ValueError:
            return 1
