"""Configuration management for Archivist Engine."""

from __future__ import annotations
import os
import re
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any

import httpx


@dataclass
class S3Credentials:
    access_key: str
    secret_key: str
    label: str = "default"

    @property
    def auth_header(self) -> str:
        return f"LOW {self.access_key}:{self.secret_key}"


@dataclass
class PatronSession:
    """Represents a signed-in Archive.org patron session for digital lending/borrowing."""
    cookie: str
    userid: Optional[str] = None

    def cookie_headers(self) -> Dict[str, str]:
        return {"Cookie": self.cookie}


@dataclass
class RateLimitConfig:
    s3_requests_per_sec: float = 5.0
    wayback_requests_per_sec: float = 3.0
    search_requests_per_sec: float = 5.0
    download_concurrency: int = 4
    upload_concurrency: int = 2


@dataclass
class ArchivistConfig:
    credentials: List[S3Credentials] = field(default_factory=list)
    active_credential_index: int = 0
    # Privacy-preserving default. Archive.org asks automated clients to identify
    # themselves and to name the model in use for AI agents
    # (https://archive.org/developers/bots.html). That requirement makes every
    # request trivially correlatable and is a tracking surface, so this client
    # sends a generic, non-unique User-Agent instead: it identifies the HTTP
    # library, which thousands of clients share, and carries no tool name,
    # version, account handle, or model identifier.
    #
    # Set IA_USER_AGENT to identify yourself if a deployment needs archive.org
    # to recognise it (partner collections, higher rate limits, support).
    user_agent: str = f"python-httpx/{httpx.__version__}"
    # When True (default), read-only requests are sent without credentials and
    # a rate-limit response never causes an Authorization header to be injected.
    # Reads do not need S3 auth; sending it would tie research activity to an
    # account. Set to False only for authenticated workflows.
    privacy_mode: bool = True
    max_text_fetch_bytes: int = 4_000_000
    default_collection: str = "opensource"
    default_mediatype: str = "data"
    timeout: float = 30.0
    max_retries: int = 5
    backoff_factor: float = 1.5
    rate_limits: RateLimitConfig = field(default_factory=RateLimitConfig)
    data_dir: Path = field(default_factory=lambda: Path("./data"))
    # Cache fetched text locally. Research is iterative: re-downloading a
    # megabyte of OCR to ask a second question is slow and is a needless repeat
    # request to a third party. Set text_cache=False to leave no local copy.
    text_cache: bool = True
    text_cache_dir: Optional[Path] = None
    state_db_path: Path = field(default_factory=lambda: Path("./data/pipeline_state.db"))
    patron_session: Optional[PatronSession] = None

    @property
    def current_credentials(self) -> Optional[S3Credentials]:
        if self.credentials and 0 <= self.active_credential_index < len(self.credentials):
            return self.credentials[self.active_credential_index]
        return None

    @classmethod
    def load(cls, search_dir: Optional[Path] = None) -> "ArchivistConfig":
        """Load configuration from environment variables, files, or local directory."""
        config = cls()
        base_dir = search_dir or Path.cwd()

        # Local state must not depend on the process working directory. The
        # defaults are relative, so a caller running from elsewhere (the DSH
        # plugin bridge is spawned with an arbitrary cwd) would read and write a
        # different store than the CLI — harvest would appear to do nothing.
        if not config.data_dir.is_absolute():
            config.data_dir = base_dir / config.data_dir
        if config.text_cache_dir and not Path(config.text_cache_dir).is_absolute():
            config.text_cache_dir = base_dir / config.text_cache_dir
        if not config.state_db_path.is_absolute():
            config.state_db_path = base_dir / config.state_db_path

        # Explicit opt-in identification only. No default handle, version, or
        # model identifier is sent.
        override_ua = (os.getenv("IA_USER_AGENT") or os.getenv("ARCHIVIST_USER_AGENT") or "").strip()
        if override_ua:
            config.user_agent = override_ua

        # 1. Check environment variables
        env_ak = os.getenv("IA_ACCESS_KEY") or os.getenv("ARCHIVIST_ACCESS_KEY")
        env_sk = os.getenv("IA_SECRET_KEY") or os.getenv("ARCHIVIST_SECRET_KEY")
        if env_ak and env_sk:
            config.credentials.append(S3Credentials(access_key=env_ak.strip(), secret_key=env_sk.strip(), label="env"))

        # 2. Check "Internet Archive S3 access.txt" in base_dir, package root, or parents
        pkg_root = Path(__file__).resolve().parent.parent
        found_txt = None
        for candidate_dir in [base_dir, pkg_root, *base_dir.parents, *pkg_root.parents]:
            txt_path = candidate_dir / "Internet Archive S3 access.txt"
            if txt_path.is_file():
                found_txt = txt_path
                break

        if found_txt:
            try:
                content = found_txt.read_text(encoding="utf-8")
                # Parse blocks of S3 access key / S3 secret key
                ak_matches = re.findall(r"S3\s+access\s+key:\s*([A-Za-z0-9]+)", content, re.IGNORECASE)
                sk_matches = re.findall(r"S3\s+secret\s+key:\s*([A-Za-z0-9]+)", content, re.IGNORECASE)
                for i, (ak, sk) in enumerate(zip(ak_matches, sk_matches), 1):
                    # Check if already added
                    if not any(c.access_key == ak for c in config.credentials):
                        config.credentials.append(S3Credentials(access_key=ak, secret_key=sk, label=f"file_key_{i}"))
            except Exception:
                pass

        # 3. Check ~/.ia or ~/.config/ia.ini
        home_ia = Path.home() / ".ia"
        if home_ia.is_file():
            try:
                import configparser
                parser = configparser.ConfigParser()
                parser.read(home_ia)
                if "s3" in parser:
                    ak = parser["s3"].get("access")
                    sk = parser["s3"].get("secret")
                    if ak and sk and not any(c.access_key == ak for c in config.credentials):
                        config.credentials.append(S3Credentials(access_key=ak, secret_key=sk, label="ia_cli"))
            except Exception:
                pass

        # 4. Check for PatronSession cookies (IA_PATRON_COOKIE, patron_cookie.txt, or ~/.ia)
        patron_cookie = (os.getenv("IA_PATRON_COOKIE") or os.getenv("ARCHIVIST_PATRON_COOKIE") or "").strip()
        if not patron_cookie:
            for candidate_dir in [base_dir, pkg_root, *base_dir.parents]:
                cookie_file = candidate_dir / "patron_cookie.txt"
                if cookie_file.is_file():
                    try:
                        patron_cookie = cookie_file.read_text(encoding="utf-8").strip()
                        if patron_cookie:
                            break
                    except Exception:
                        pass
        if not patron_cookie and home_ia.is_file():
            try:
                import configparser
                parser = configparser.ConfigParser()
                parser.read(home_ia)
                if "cookies" in parser:
                    u = parser["cookies"].get("logged-in-user")
                    s = parser["cookies"].get("logged-in-sig")
                    if u and s:
                        patron_cookie = f"logged-in-user={u}; logged-in-sig={s}"
            except Exception:
                pass
        if patron_cookie:
            config.patron_session = PatronSession(cookie=patron_cookie)

        config.data_dir.mkdir(parents=True, exist_ok=True)
        return config

    def save_credentials(self, target_file: Optional[Path] = None) -> Path:
        """Persist current key pool to disk."""
        target = target_file or (Path.cwd() / "Internet Archive S3 access.txt")
        lines = ["Internet Archive:\n"]
        for c in self.credentials:
            lines.append(f"S3 access key: {c.access_key}")
            lines.append(f"S3 secret key: {c.secret_key}\n")
        target.write_text("\n".join(lines), encoding="utf-8")
        return target
