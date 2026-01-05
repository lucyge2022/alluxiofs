import json
import threading
import time
from typing import Any
from typing import Dict
from typing import Optional

import fsspec
from fsspec import filesystem

from alluxiofs.client.const import ALLUXIO_UFS_INFO_REFRESH_INTERVAL_MINUTES
from alluxiofs.client.core import AlluxioClient
from alluxiofs.client.log import setup_logger
from alluxiofs.client.log import TagAdapter
from alluxiofs.client.utils import convert_ufs_info_to
from alluxiofs.client.utils import get_protocol_from_path
from alluxiofs.client.utils import register_unregistered_ufs_to_fsspec


class UfsInfo:
    """
    Data class representing UFS information.
    """

    def __init__(
        self, alluxio_path: str, ufs_full_path: str, options: Dict[str, Any]
    ):
        self.alluxio_path = alluxio_path
        self.ufs_full_path = ufs_full_path
        self.options = options


class BaseUFSUpdater:
    """
    Base class for UFS Updater.
    """

    def __init__(self):
        self._cached_ufs: Optional[Dict[str, Any]] = {}
        self._path_map: Optional[Dict[str, str]] = {}
        self.logger = None

    def get_protocol_from_path(self, path):
        return get_protocol_from_path(path)

    def register_ufs_fallback(self, ufs_info_list: list):
        """
        Register under file systems (UFS) for fallback when accessed files fail in Alluxiofs.

        Args:
            ufs_info_list: List of UfsInfo objects containing UFS details
        """
        for ufs_info in ufs_info_list:
            protocol = self.get_protocol_from_path(
                ufs_info.ufs_full_path.lower()
            )
            if not protocol:
                if self.logger:
                    self.logger.warning(
                        f"Invalid protocol or path: {ufs_info.ufs_full_path}"
                    )
                continue
            register_unregistered_ufs_to_fsspec(protocol)
            if fsspec.get_filesystem_class(protocol) is None:
                raise ValueError(f"Unsupported protocol: {protocol}")
            else:
                target_options = ufs_info.options
                target_options = convert_ufs_info_to(protocol, target_options)
                self._cached_ufs[ufs_info.ufs_full_path] = filesystem(
                    protocol, **target_options
                )
                self._path_map[ufs_info.ufs_full_path] = ufs_info.alluxio_path
                self.logger.debug(
                    f"Registered UFS client for {ufs_info.ufs_full_path}"
                )

    def get_ufs_count(self):
        return len(self._cached_ufs) if self._cached_ufs else 0

    def get_ufs_from_cache(self, path: str):
        if not self._cached_ufs:
            return None
        for ufs_path in self._cached_ufs:
            if path.startswith(ufs_path):
                return self._cached_ufs[ufs_path]
        return None

    def get_alluxio_path_from_ufs_full_path(self, path: str):
        if not self._cached_ufs:
            return None
        for ufs_path in self._cached_ufs:
            if path.startswith(ufs_path):
                return path.replace(ufs_path, self._path_map[ufs_path], 1)
        return None

    def must_get_ufs_count(self):
        return self.get_ufs_count()

    def must_get_ufs_from_path(self, path: str):
        return self.get_ufs_from_cache(path)

    def must_get_alluxio_path_from_ufs_full_path(self, path: str):
        return self.get_alluxio_path_from_ufs_full_path(path)

    def start_updater(self):
        pass

    def stop_updater(self):
        pass


class UFSUpdater(BaseUFSUpdater):
    """
    Class responsible for periodically updating Ufs Info in the background.
    """

    def __init__(self, alluxio):
        super().__init__()
        assert isinstance(alluxio, AlluxioClient) or alluxio is None, (
            "alluxio must be an instance of AlluxioClient or None so that "
            "UFSUpdater can access the Alluxio configuration and correctly "
            "initialize background UFS info refresh; passing any other type "
            "will prevent proper setup of periodic updates and logging."
        )
        self.alluxio = alluxio
        self.config = alluxio.config if alluxio else None
        if self.alluxio:
            self.interval_seconds = (
                self.alluxio.config.ufs_info_refresh_interval_minutes * 60
            )
            base_logger = setup_logger(
                self.config.log_dir,
                self.config.log_level,
                self.__class__.__name__,
                self.config.log_tag_allowlist,
            )
            self.logger = TagAdapter(base_logger, {"tag": "[UFS_MANAGER]"})
        else:
            self.interval_seconds = (
                ALLUXIO_UFS_INFO_REFRESH_INTERVAL_MINUTES * 60
            )
            base_logger = setup_logger(
                class_name=self.__class__.__name__,
                log_tags=self.config.log_tag_allowlist
                if self.config
                else None,
            )
            self.logger = TagAdapter(base_logger, {"tag": "[UFS_MANAGER]"})

        # Lock to protect the shared variables _cached_ufs and _path_map
        self._lock = threading.RLock()
        self._init_event = threading.Event()
        # Thread control flag
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def _get_ufs_info_from_worker(self):
        """
        Original method: Fetch Ufs Info from the worker.
        """
        if self.alluxio:
            return self.alluxio.get_ufs_info_from_worker()
        else:
            return ""

    def _update_thread_target(self):
        """
        Target function for the background thread: periodically update data.
        """
        self.logger.debug(
            f"Background update thread started, updating every {self.interval_seconds} seconds..."
        )

        # Execute once immediately upon start
        self._execute_update()

        # Loop until the stop event is set
        while not self._stop_event.wait(self.interval_seconds):
            self._execute_update()

        self.logger.debug("Background update thread stopped.")

    def _execute_update(self):
        """
        Helper method to fetch data and update the cache safely.
        """
        try:
            ufs_info_json = self._get_ufs_info_from_worker()

            if ufs_info_json is not None:
                # Use lock to safely update the shared variable
                with self._lock:
                    ufs_info_list = self.parse_ufs_info(ufs_info_json)
                    self.register_ufs_fallback(ufs_info_list)
                self.logger.debug(
                    f"ufs's Info updated. Time: {time.strftime('%H:%M:%S')}"
                )
            else:
                self.logger.warning(
                    f"ufs's Info update failed, keeping previous result. Time: {time.strftime('%H:%M:%S')}"
                )
        except Exception as e:
            self.logger.error(
                f"Exception occurred during ufs's Info update: {e}"
            )
        finally:
            self._init_event.set()

    def start_updater(self):
        """
        Start the background thread.
        """
        if self._thread is None or not self._thread.is_alive():
            # daemon=True ensures the thread exits when the main program exits
            self._thread = threading.Thread(
                target=self._update_thread_target, daemon=True
            )
            self._thread.start()
            self.logger.debug("Ufs Info updater started.")
        else:
            self.logger.debug("Updater is already running.")

    def stop_updater(self):
        """
        Stop the background thread gracefully.
        """
        if self._thread and self._thread.is_alive():
            self._stop_event.set()  # Signal the thread to stop
            self._thread.join()  # Wait for the thread to finish
            self._stop_event.clear()  # Reset the event for the next start
            self.logger.debug("Ufs Info updater stopped.")

    def parse_ufs_info(self, ufs_info_str: str) -> list:
        """
        Parse UFS info from a JSON string.

        Args:
            ufs_info_str: JSON string containing UFS information

        Returns:
            List of UfsInfo objects, empty list if parsing fails or no data
        """
        if not ufs_info_str or not ufs_info_str.strip():
            self.logger.warning("Empty or None UFS info string provided")
            return []

        ufs_info_list = []

        try:
            ufs_info_json = json.loads(ufs_info_str)

            if not isinstance(ufs_info_json, dict):
                self.logger.error(
                    f"UFS info should be a JSON object, got {type(ufs_info_json)}"
                )
                return []

            for ufs_full_path, value in ufs_info_json.items():
                if not isinstance(value, dict):
                    self.logger.warning(
                        f"UFS config for {ufs_full_path} is not a dictionary, skipping"
                    )
                    continue

                alluxio_path = value.get("alluxio_path", "")
                options = {
                    k: v for k, v in value.items() if k != "alluxio_path"
                }

                if alluxio_path.endswith("/"):
                    alluxio_path = alluxio_path[:-1]
                if ufs_full_path.endswith("/"):
                    ufs_full_path = ufs_full_path[:-1]

                ufs_info = UfsInfo(
                    alluxio_path=alluxio_path,
                    ufs_full_path=ufs_full_path,
                    options=options,
                )
                ufs_info_list.append(ufs_info)

        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to parse UFS info JSON: {e}")
            return []
        except Exception as e:
            self.logger.error(f"Unexpected error parsing UFS info: {e}")
            return []

        return ufs_info_list

    def must_get_ufs_count(self):
        self._init_event.wait()
        return self.get_ufs_count()

    def get_ufs_count(self):
        with self._lock:
            return super().get_ufs_count()

    def must_get_ufs_from_path(self, path: str):
        self._init_event.wait()
        ufs = self.get_ufs_from_cache(path)
        if ufs is None:
            self.logger.error(
                f"No registered UFS found in alluxio python sdk for path: {path}"
            )
            raise ValueError(
                f"No registered UFS found in alluxio python sdk for path: {path}"
            )
        return ufs

    def get_ufs_from_cache(self, path: str):
        with self._lock:
            return super().get_ufs_from_cache(path)

    def must_get_alluxio_path_from_ufs_full_path(self, path: str):
        self._init_event.wait()
        return self.get_alluxio_path_from_ufs_full_path(path)

    def get_alluxio_path_from_ufs_full_path(self, path: str):
        with self._lock:
            return super().get_alluxio_path_from_ufs_full_path(path)


class LocalUFSUpdater(BaseUFSUpdater):
    def __init__(self, ufs_config: Dict[str, Any]):
        super().__init__()
        self.ufs_config = ufs_config
        base_logger = setup_logger(
            class_name=self.__class__.__name__,
        )
        self.logger = TagAdapter(base_logger, {"tag": "[UFS_MANAGER]"})
        self.register_ufs_fallback(self.parse_ufs_info())

    def parse_ufs_info(self) -> list:
        """
        Parse UFS info from the provided configuration.

        Returns:
            List of UfsInfo objects, empty list if parsing fails or no data
        """
        ufs_info_list = []

        for ufs_full_path, value in self.ufs_config.items():
            if not isinstance(value, dict):
                self.logger.warning(
                    f"UFS config for {ufs_full_path} is not a dictionary, skipping"
                )
                continue

            options = value.copy()

            if ufs_full_path.endswith("/"):
                ufs_full_path = ufs_full_path[:-1]

            ufs_info = UfsInfo(
                alluxio_path=ufs_full_path,
                ufs_full_path=ufs_full_path,
                options=options,
            )
            ufs_info_list.append(ufs_info)

        return ufs_info_list


class UFSManager:
    """
    Class responsible for managing Ufs Info.
    """

    def __init__(self, alluxio=None, config: Dict = None):
        self.ufs_updater: Optional[BaseUFSUpdater] = None
        if alluxio is not None:
            self.ufs_updater = UFSUpdater(alluxio)
        elif config is not None:
            self.ufs_updater = LocalUFSUpdater(config)

    def initialize_ufs_manager(self):
        """
        Initialize UFS Manager with UFS Updater.
        """
        if self.ufs_updater:
            self.ufs_updater.start_updater()

    def shutdown_ufs_manager(self):
        """
        Shutdown UFS Manager and stop the UFS Updater.
        """
        if self.ufs_updater:
            self.ufs_updater.stop_updater()

    def must_get_ufs_count(self):
        if self.ufs_updater:
            return self.ufs_updater.must_get_ufs_count()
        return 0

    def must_get_ufs_from_path(self, path: str):
        if self.ufs_updater:
            return self.ufs_updater.must_get_ufs_from_path(path)
        return None

    def must_get_alluxio_path_from_ufs_full_path(self, path: str):
        if self.ufs_updater:
            return self.ufs_updater.must_get_alluxio_path_from_ufs_full_path(
                path
            )
        return None

    def get_ufs_count(self):
        if self.ufs_updater:
            return self.ufs_updater.get_ufs_count()
        return 0

    def get_ufs_from_cache(self, path: str):
        if self.ufs_updater:
            return self.ufs_updater.get_ufs_from_cache(path)
        return None

    def get_alluxio_path_from_ufs_full_path(self, path: str):
        if self.ufs_updater:
            return self.ufs_updater.get_alluxio_path_from_ufs_full_path(path)
        return None
