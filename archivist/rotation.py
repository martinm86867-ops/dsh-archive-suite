"""Automatic S3 key rotation, quota tracking, and failover management."""

from __future__ import annotations
import asyncio
import logging
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field

from .config import S3Credentials, ArchivistConfig

logger = logging.getLogger("archivist.rotation")


@dataclass
class KeyQuotaStatus:
    credentials: S3Credentials
    is_exhausted: bool = False
    exhaustion_reason: Optional[str] = None
    exhausted_at: Optional[float] = None
    daily_captures_used: int = 0
    daily_captures_limit: int = 30000
    available_slots: int = 3
    cooldown_seconds: float = 3600.0  # 1 hour cooldown or until daily reset

    def check_cooldown_expired(self) -> bool:
        if self.is_exhausted and self.exhausted_at:
            now = time.time()
            exhausted_dt = datetime.fromtimestamp(self.exhausted_at, tz=timezone.utc)
            now_dt = datetime.fromtimestamp(now, tz=timezone.utc)
            # Automatic midnight UTC rollover: Archive.org resets daily capture quotas at 00:00 UTC
            if now_dt.date() > exhausted_dt.date():
                self.is_exhausted = False
                self.exhaustion_reason = None
                self.exhausted_at = None
                self.daily_captures_used = 0
                return True
            if now - self.exhausted_at > self.cooldown_seconds:
                self.is_exhausted = False
                self.exhaustion_reason = None
                self.exhausted_at = None
                return True
        return False


class KeyRotationManager:
    """Manages pool of Archive.org credentials, tracking quotas and rotating on exhaustion."""

    def __init__(self, config: ArchivistConfig):
        self.config = config
        self._pool: List[KeyQuotaStatus] = [
            KeyQuotaStatus(credentials=c) for c in config.credentials
        ]
        self._state_file = Path(config.data_dir) / "active_key_index.txt"
        self._current_index = self._load_active_index(config.active_credential_index)
        self._lock = asyncio.Lock()

    def _load_active_index(self, default_idx: int) -> int:
        if self._state_file.is_file():
            try:
                val = int(self._state_file.read_text().strip())
                if 0 <= val < len(self._pool):
                    return val
            except Exception:
                pass
        return default_idx

    def _save_active_index(self, idx: int) -> None:
        try:
            self._state_file.parent.mkdir(parents=True, exist_ok=True)
            self._state_file.write_text(str(idx))
        except Exception:
            pass

    @property
    def current_credentials(self) -> Optional[S3Credentials]:
        if not self._pool:
            return None
        return self._pool[self._current_index].credentials

    def get_pool_status(self) -> List[Dict[str, Any]]:
        """Return status for all keys in the pool."""
        statuses = []
        for idx, k in enumerate(self._pool):
            k.check_cooldown_expired()
            statuses.append({
                "index": idx,
                "label": k.credentials.label,
                "access_key": f"{k.credentials.access_key[:4]}...{k.credentials.access_key[-4:]}",
                "is_active": idx == self._current_index,
                "is_exhausted": k.is_exhausted,
                "exhaustion_reason": k.exhaustion_reason,
                "daily_captures": k.daily_captures_used,
                "daily_limit": k.daily_captures_limit,
                "available_slots": k.available_slots
            })
        return statuses

    async def rotate_to_next(self, reason: str = "quota_exhausted") -> Optional[S3Credentials]:
        """Rotate to the next available non-exhausted key in the pool."""
        async with self._lock:
            if not self._pool:
                return None

            curr = self._pool[self._current_index]
            curr.is_exhausted = True
            curr.exhaustion_reason = reason
            curr.exhausted_at = time.time()

            logger.warning(
                f"Rotating S3 credentials away from {curr.credentials.label} ({curr.credentials.access_key[:4]}...): {reason}"
            )

            # Look for next non-exhausted key
            num_keys = len(self._pool)
            for offset in range(1, num_keys + 1):
                idx = (self._current_index + offset) % num_keys
                candidate = self._pool[idx]
                if candidate.check_cooldown_expired() or not candidate.is_exhausted:
                    self._current_index = idx
                    self.config.active_credential_index = idx
                    self._save_active_index(idx)
                    logger.info(f"Switched to S3 key #{idx + 1} ({candidate.credentials.label})")
                    return candidate.credentials

            # If all are exhausted, reset the one with oldest exhaustion
            oldest_idx = min(range(num_keys), key=lambda i: self._pool[i].exhausted_at or 0)
            self._current_index = oldest_idx
            self.config.active_credential_index = oldest_idx
            self._save_active_index(oldest_idx)
            self._pool[oldest_idx].is_exhausted = False
            logger.warning(f"All keys exhausted. Re-activating oldest key #{oldest_idx + 1}")
            return self._pool[oldest_idx].credentials

    async def update_key_quota(self, access_key: str, quota_data: Dict[str, Any]) -> None:
        """Update quota metrics from live /save/status/user response."""
        for item in self._pool:
            if item.credentials.access_key == access_key:
                item.daily_captures_used = quota_data.get("daily_captures", item.daily_captures_used)
                item.daily_captures_limit = quota_data.get("daily_captures_limit", item.daily_captures_limit)
                item.available_slots = quota_data.get("available", item.available_slots)

                if item.daily_captures_used >= item.daily_captures_limit:
                    item.is_exhausted = True
                    item.exhaustion_reason = f"Daily capture limit reached ({item.daily_captures_used}/{item.daily_captures_limit})"
                    item.exhausted_at = time.time()
                break

    def add_key(self, access_key: str, secret_key: str, label: str = "custom") -> None:
        """Dynamically add a new credential pair to the rotation pool."""
        creds = S3Credentials(access_key=access_key.strip(), secret_key=secret_key.strip(), label=label)
        self.config.credentials.append(creds)
        self._pool.append(KeyQuotaStatus(credentials=creds))
        logger.info(f"Added new S3 key ({label}) to rotation pool. Total keys: {len(self._pool)}")
