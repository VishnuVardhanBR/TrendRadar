# coding=utf-8
"""
Storage Manager - Unified storage backend management

Automatically selects appropriate storage backend based on environment and config
"""

import os
from typing import Optional

from trendradar.storage.base import StorageBackend, NewsData


# Storage manager singleton
_storage_manager: Optional["StorageManager"] = None


class StorageManager:
    """
    Storage Manager

    Features:
    - Auto-detect runtime environment (GitHub Actions / Docker / Local)
    - Select storage backend based on config (local / remote / auto)
    - Provide unified storage interface
    - Support pulling data from remote to local
    """

    def __init__(
        self,
        backend_type: str = "auto",
        data_dir: str = "output",
        enable_txt: bool = True,
        enable_html: bool = True,
        remote_config: Optional[dict] = None,
        local_retention_days: int = 0,
        remote_retention_days: int = 0,
        pull_enabled: bool = False,
        pull_days: int = 0,
        timezone: str = "America/New_York",
    ):
        """
        Initialize storage manager

        Args:
            backend_type: Storage backend type (local / remote / auto)
            data_dir: Local data directory
            enable_txt: Enable TXT snapshots
            enable_html: Enable HTML reports
            remote_config: Remote storage config (endpoint_url, bucket_name, access_key_id, etc.)
            local_retention_days: Local data retention days (0 = unlimited)
            remote_retention_days: Remote data retention days (0 = unlimited)
            pull_enabled: Enable auto-pull on startup
            pull_days: Pull last N days of data
            timezone: Timezone config (default: America/New_York)
        """
        self.backend_type = backend_type
        self.data_dir = data_dir
        self.enable_txt = enable_txt
        self.enable_html = enable_html
        self.remote_config = remote_config or {}
        self.local_retention_days = local_retention_days
        self.remote_retention_days = remote_retention_days
        self.pull_enabled = pull_enabled
        self.pull_days = pull_days
        self.timezone = timezone

        self._backend: Optional[StorageBackend] = None
        self._remote_backend: Optional[StorageBackend] = None

    @staticmethod
    def is_github_actions() -> bool:
        """Detect if running in GitHub Actions environment"""
        return os.environ.get("GITHUB_ACTIONS") == "true"

    @staticmethod
    def is_docker() -> bool:
        """Detect if running in Docker container"""
        # Method 1: Check /.dockerenv file
        if os.path.exists("/.dockerenv"):
            return True

        # Method 2: Check cgroup (Linux)
        try:
            with open("/proc/1/cgroup", "r") as f:
                return "docker" in f.read()
        except (FileNotFoundError, PermissionError):
            pass

        # Method 3: Check environment variable
        return os.environ.get("DOCKER_CONTAINER") == "true"

    def _resolve_backend_type(self) -> str:
        """Resolve actual backend type to use"""
        if self.backend_type == "auto":
            if self.is_github_actions():
                # GitHub Actions environment, check for remote storage config
                if self._has_remote_config():
                    return "remote"
                else:
                    print("[Storage Manager] GitHub Actions but no remote storage configured, using local")
                    return "local"
            else:
                return "local"
        return self.backend_type

    def _has_remote_config(self) -> bool:
        """Check if valid remote storage config exists"""
        # Check config or environment variables
        bucket_name = self.remote_config.get("bucket_name") or os.environ.get("S3_BUCKET_NAME")
        access_key = self.remote_config.get("access_key_id") or os.environ.get("S3_ACCESS_KEY_ID")
        secret_key = self.remote_config.get("secret_access_key") or os.environ.get("S3_SECRET_ACCESS_KEY")
        endpoint = self.remote_config.get("endpoint_url") or os.environ.get("S3_ENDPOINT_URL")

        # Debug logging
        has_config = bool(bucket_name and access_key and secret_key and endpoint)
        if not has_config:
            print(f"[Storage Manager] Remote storage config check failed:")
            print(f"  - bucket_name: {'configured' if bucket_name else 'not configured'}")
            print(f"  - access_key_id: {'configured' if access_key else 'not configured'}")
            print(f"  - secret_access_key: {'configured' if secret_key else 'not configured'}")
            print(f"  - endpoint_url: {'configured' if endpoint else 'not configured'}")

        return has_config

    def _create_remote_backend(self) -> Optional[StorageBackend]:
        """Create remote storage backend"""
        try:
            from trendradar.storage.remote import RemoteStorageBackend

            return RemoteStorageBackend(
                bucket_name=self.remote_config.get("bucket_name") or os.environ.get("S3_BUCKET_NAME", ""),
                access_key_id=self.remote_config.get("access_key_id") or os.environ.get("S3_ACCESS_KEY_ID", ""),
                secret_access_key=self.remote_config.get("secret_access_key") or os.environ.get("S3_SECRET_ACCESS_KEY", ""),
                endpoint_url=self.remote_config.get("endpoint_url") or os.environ.get("S3_ENDPOINT_URL", ""),
                region=self.remote_config.get("region") or os.environ.get("S3_REGION", ""),
                enable_txt=self.enable_txt,
                enable_html=self.enable_html,
                timezone=self.timezone,
            )
        except ImportError as e:
            print(f"[Storage Manager] Remote backend import failed: {e}")
            print("[Storage Manager] Please ensure boto3 is installed: pip install boto3")
            return None
        except Exception as e:
            print(f"[Storage Manager] Remote backend initialization failed: {e}")
            return None

    def get_backend(self) -> StorageBackend:
        """Get storage backend instance"""
        if self._backend is None:
            resolved_type = self._resolve_backend_type()

            if resolved_type == "remote":
                self._backend = self._create_remote_backend()
                if self._backend:
                    print(f"[Storage Manager] Using remote storage backend")
                else:
                    print("[Storage Manager] Falling back to local storage")
                    resolved_type = "local"

            if resolved_type == "local" or self._backend is None:
                from trendradar.storage.local import LocalStorageBackend

                self._backend = LocalStorageBackend(
                    data_dir=self.data_dir,
                    enable_txt=self.enable_txt,
                    enable_html=self.enable_html,
                    timezone=self.timezone,
                )
                print(f"[Storage Manager] Using local storage backend (data dir: {self.data_dir})")

        return self._backend

    def pull_from_remote(self) -> int:
        """
        Pull data from remote to local

        Returns:
            Number of files successfully pulled
        """
        if not self.pull_enabled or self.pull_days <= 0:
            return 0

        if not self._has_remote_config():
            print("[Storage Manager] Remote storage not configured, cannot pull")
            return 0

        # Create remote backend if not exists
        if self._remote_backend is None:
            self._remote_backend = self._create_remote_backend()

        if self._remote_backend is None:
            print("[Storage Manager] Cannot create remote backend, pull failed")
            return 0

        # Call pull method
        return self._remote_backend.pull_recent_days(self.pull_days, self.data_dir)

    def save_news_data(self, data: NewsData) -> bool:
        """Save news data"""
        return self.get_backend().save_news_data(data)

    def get_today_all_data(self, date: Optional[str] = None) -> Optional[NewsData]:
        """Get all data for today"""
        return self.get_backend().get_today_all_data(date)

    def get_latest_crawl_data(self, date: Optional[str] = None) -> Optional[NewsData]:
        """Get latest crawl data"""
        return self.get_backend().get_latest_crawl_data(date)

    def detect_new_titles(self, current_data: NewsData) -> dict:
        """Detect new titles"""
        return self.get_backend().detect_new_titles(current_data)

    def save_txt_snapshot(self, data: NewsData) -> Optional[str]:
        """Save TXT snapshot"""
        return self.get_backend().save_txt_snapshot(data)

    def save_html_report(self, html_content: str, filename: str, is_summary: bool = False) -> Optional[str]:
        """Save HTML report"""
        return self.get_backend().save_html_report(html_content, filename, is_summary)

    def is_first_crawl_today(self, date: Optional[str] = None) -> bool:
        """Check if this is first crawl today"""
        return self.get_backend().is_first_crawl_today(date)

    def cleanup(self) -> None:
        """Cleanup resources"""
        if self._backend:
            self._backend.cleanup()
        if self._remote_backend:
            self._remote_backend.cleanup()

    def cleanup_old_data(self) -> int:
        """
        Cleanup expired data

        Returns:
            Number of deleted date directories
        """
        total_deleted = 0

        # Cleanup local data
        if self.local_retention_days > 0:
            total_deleted += self.get_backend().cleanup_old_data(self.local_retention_days)

        # Cleanup remote data (if configured)
        if self.remote_retention_days > 0 and self._has_remote_config():
            if self._remote_backend is None:
                self._remote_backend = self._create_remote_backend()
            if self._remote_backend:
                total_deleted += self._remote_backend.cleanup_old_data(self.remote_retention_days)

        return total_deleted

    @property
    def backend_name(self) -> str:
        """Get current backend name"""
        return self.get_backend().backend_name

    @property
    def supports_txt(self) -> bool:
        """Whether TXT snapshots are supported"""
        return self.get_backend().supports_txt

    # === Push record methods ===

    def has_pushed_today(self, date: Optional[str] = None) -> bool:
        """
        Check if push was sent for specified date

        Args:
            date: Date string (YYYY-MM-DD), defaults to today

        Returns:
            Whether push was sent
        """
        return self.get_backend().has_pushed_today(date)

    def record_push(self, report_type: str, date: Optional[str] = None) -> bool:
        """
        Record a push

        Args:
            report_type: Report type
            date: Date string (YYYY-MM-DD), defaults to today

        Returns:
            Whether record was successful
        """
        return self.get_backend().record_push(report_type, date)


def get_storage_manager(
    backend_type: str = "auto",
    data_dir: str = "output",
    enable_txt: bool = True,
    enable_html: bool = True,
    remote_config: Optional[dict] = None,
    local_retention_days: int = 0,
    remote_retention_days: int = 0,
    pull_enabled: bool = False,
    pull_days: int = 0,
    timezone: str = "America/New_York",
    force_new: bool = False,
) -> StorageManager:
    """
    Get storage manager singleton

    Args:
        backend_type: Storage backend type
        data_dir: Local data directory
        enable_txt: Enable TXT snapshots
        enable_html: Enable HTML reports
        remote_config: Remote storage config
        local_retention_days: Local data retention days (0 = unlimited)
        remote_retention_days: Remote data retention days (0 = unlimited)
        pull_enabled: Enable auto-pull on startup
        pull_days: Pull last N days of data
        timezone: Timezone config (default: America/New_York)
        force_new: Force create new instance

    Returns:
        StorageManager instance
    """
    global _storage_manager

    if _storage_manager is None or force_new:
        _storage_manager = StorageManager(
            backend_type=backend_type,
            data_dir=data_dir,
            enable_txt=enable_txt,
            enable_html=enable_html,
            remote_config=remote_config,
            local_retention_days=local_retention_days,
            remote_retention_days=remote_retention_days,
            pull_enabled=pull_enabled,
            pull_days=pull_days,
            timezone=timezone,
        )

    return _storage_manager
