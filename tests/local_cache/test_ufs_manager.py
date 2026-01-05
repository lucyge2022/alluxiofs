import json
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from alluxiofs.client.const import ALLUXIO_UFS_INFO_REFRESH_INTERVAL_MINUTES
from alluxiofs.client.ufs_manager import UfsInfo
from alluxiofs.client.ufs_manager import UFSUpdater


class TestUfsInfo:
    def test_init(self):
        info = UfsInfo("alluxio_path", "ufs_path", {"opt": "val"})
        assert info.alluxio_path == "alluxio_path"
        assert info.ufs_full_path == "ufs_path"
        assert info.options == {"opt": "val"}


class TestUFSUpdater:
    @pytest.fixture
    def mock_alluxio(self):
        from alluxiofs.client import AlluxioClient

        class MockAlluxioFileSystem(AlluxioClient):
            config = None

            def get_ufs_info_from_worker(self):
                pass

        alluxio = MagicMock(spec=MockAlluxioFileSystem)
        alluxio.config.ufs_info_refresh_interval_minutes = 10
        alluxio.config.log_dir = "/tmp"
        alluxio.config.log_level = "INFO"
        alluxio.config.log_tag_allowlist = []
        return alluxio

    def test_init_with_alluxio(self, mock_alluxio):
        updater = UFSUpdater(mock_alluxio)
        assert updater.interval_seconds == 600
        assert updater.alluxio == mock_alluxio
        assert updater._cached_ufs == {}
        assert updater._path_map == {}

    def test_init_without_alluxio(self):
        updater = UFSUpdater(None)
        assert (
            updater.interval_seconds
            == ALLUXIO_UFS_INFO_REFRESH_INTERVAL_MINUTES * 60
        )
        assert updater.alluxio is None

    def test_parse_ufs_info_valid(self, mock_alluxio):
        updater = UFSUpdater(mock_alluxio)
        json_str = json.dumps(
            {"s3://bucket/path": {"alluxio_path": "/mnt/s3", "key": "value"}}
        )
        result = updater.parse_ufs_info(json_str)
        assert len(result) == 1
        assert result[0].ufs_full_path == "s3://bucket/path"
        assert result[0].alluxio_path == "/mnt/s3"
        assert result[0].options == {"key": "value"}

    def test_parse_ufs_info_invalid_json(self, mock_alluxio):
        updater = UFSUpdater(mock_alluxio)
        result = updater.parse_ufs_info("invalid json")
        assert result == []

    def test_parse_ufs_info_empty(self, mock_alluxio):
        updater = UFSUpdater(mock_alluxio)
        assert updater.parse_ufs_info("") == []
        assert updater.parse_ufs_info(None) == []

    def test_parse_ufs_info_not_dict(self, mock_alluxio):
        updater = UFSUpdater(mock_alluxio)
        result = updater.parse_ufs_info("[]")
        assert result == []

    def test_parse_ufs_info_value_not_dict(self, mock_alluxio):
        updater = UFSUpdater(mock_alluxio)
        json_str = json.dumps({"path": "not a dict"})
        result = updater.parse_ufs_info(json_str)
        assert result == []

    @patch("alluxiofs.client.ufs_manager.UFSUpdater.get_protocol_from_path")
    @patch("alluxiofs.client.ufs_manager.register_unregistered_ufs_to_fsspec")
    @patch("alluxiofs.client.ufs_manager.filesystem")
    @patch("alluxiofs.client.ufs_manager.fsspec.get_filesystem_class")
    def test_register_ufs_fallback(
        self,
        mock_get_fs_class,
        mock_filesystem,
        mock_register,
        mock_get_protocol,
        mock_alluxio,
    ):
        updater = UFSUpdater(mock_alluxio)
        ufs_info = UfsInfo("/mnt/s3", "s3://bucket", {"access_key": "val"})

        mock_get_protocol.return_value = "s3"
        mock_get_fs_class.return_value = True  # Simulate supported protocol
        mock_fs_instance = MagicMock()
        mock_filesystem.return_value = mock_fs_instance

        updater.register_ufs_fallback([ufs_info])

        mock_get_protocol.assert_called_with("s3://bucket")
        mock_register.assert_called_with("s3")
        mock_filesystem.assert_called_with("s3", key="val")
        assert updater._cached_ufs["s3://bucket"] == mock_fs_instance
        assert updater._path_map["s3://bucket"] == "/mnt/s3"

    @patch("alluxiofs.client.ufs_manager.register_unregistered_ufs_to_fsspec")
    @patch("alluxiofs.client.ufs_manager.fsspec.get_filesystem_class")
    def test_register_ufs_fallback_unsupported(
        self, mock_get_fs_class, mock_register, mock_alluxio
    ):
        updater = UFSUpdater(mock_alluxio)
        ufs_info = UfsInfo("/mnt/unknown", "unknown://bucket", {})

        mock_get_fs_class.return_value = None  # Simulate unsupported protocol

        with pytest.raises(ValueError, match="Unsupported protocol"):
            updater.register_ufs_fallback([ufs_info])

    def test_get_ufs_from_cache(self, mock_alluxio):
        updater = UFSUpdater(mock_alluxio)
        updater._cached_ufs = {"s3://bucket": "fs_instance"}

        assert updater.get_ufs_from_cache("s3://bucket/file") == "fs_instance"
        assert updater.get_ufs_from_cache("hdfs://namenode/file") is None

    def test_get_alluxio_path_from_ufs_full_path(self, mock_alluxio):
        updater = UFSUpdater(mock_alluxio)
        updater._cached_ufs = {"s3://bucket": "fs_instance"}
        updater._path_map = {"s3://bucket": "/mnt/s3"}

        assert (
            updater.get_alluxio_path_from_ufs_full_path("s3://bucket/file")
            == "/mnt/s3/file"
        )
        assert (
            updater.get_alluxio_path_from_ufs_full_path("hdfs://namenode/file")
            is None
        )

    @patch("alluxiofs.client.ufs_manager.UFSUpdater.parse_ufs_info")
    @patch("alluxiofs.client.ufs_manager.UFSUpdater.register_ufs_fallback")
    def test_execute_update_success(
        self, mock_register, mock_parse, mock_alluxio
    ):
        updater = UFSUpdater(mock_alluxio)
        mock_alluxio.get_ufs_info_from_worker.return_value = '{"json": "data"}'
        mock_parse.return_value = ["parsed_info"]

        updater._execute_update()

        mock_parse.assert_called_with('{"json": "data"}')
        mock_register.assert_called_with(["parsed_info"])
        assert updater._init_event.is_set()

    def test_execute_update_fail_none(self, mock_alluxio):
        updater = UFSUpdater(mock_alluxio)
        mock_alluxio.get_ufs_info_from_worker.return_value = None

        updater._execute_update()
        # Should not raise exception, just log warning
        assert updater._init_event.is_set()

    def test_execute_update_exception(self, mock_alluxio):
        updater = UFSUpdater(mock_alluxio)
        mock_alluxio.get_ufs_info_from_worker.side_effect = Exception(
            "Network error"
        )

        updater._execute_update()
        # Should catch exception and log error
        assert updater._init_event.is_set()

    def test_start_stop_updater(self, mock_alluxio):
        updater = UFSUpdater(mock_alluxio)
        with patch("threading.Thread") as mock_thread_cls:
            mock_thread = MagicMock()
            mock_thread_cls.return_value = mock_thread

            updater.start_updater()
            mock_thread.start.assert_called_once()

            # Start again should do nothing
            updater.start_updater()
            mock_thread.start.assert_called_once()

            mock_thread.is_alive.return_value = True

            # Mock _stop_event to verify calls
            updater._stop_event = MagicMock()

            updater.stop_updater()

            updater._stop_event.set.assert_called_once()
            mock_thread.join.assert_called_once()
            updater._stop_event.clear.assert_called_once()

    def test_must_get_methods_wait(self, mock_alluxio):
        updater = UFSUpdater(mock_alluxio)

        # Mock _init_event.wait to return immediately
        updater._init_event.wait = MagicMock()

        updater._cached_ufs = {"s3://bucket": "fs"}
        assert updater.must_get_ufs_count() == 1
        updater._init_event.wait.assert_called()

        updater.get_ufs_from_cache = MagicMock(return_value="fs")
        assert updater.must_get_ufs_from_path("path") == "fs"

        updater.get_ufs_from_cache.return_value = None
        with pytest.raises(ValueError):
            updater.must_get_ufs_from_path("path")

        updater.get_alluxio_path_from_ufs_full_path = MagicMock(
            return_value="/path"
        )
        assert (
            updater.must_get_alluxio_path_from_ufs_full_path("path") == "/path"
        )


class TestLocalUFSUpdater:
    @patch("alluxiofs.client.ufs_manager.setup_logger")
    @patch(
        "alluxiofs.client.ufs_manager.LocalUFSUpdater.register_ufs_fallback"
    )
    def test_parse_ufs_info(self, mock_register, mock_logger):
        from alluxiofs.client.ufs_manager import LocalUFSUpdater

        ufs_config = {
            "s3://bucket1": {
                "access_key": "my_key",
                "secret_key": "my_secret",
                "endpoint": "http://s3.amazonaws.com",
                "ufs_mount_path": "/mnt/s3",
            },
            "hdfs://namenode": {
                "user": "hadoop",
                "ufs_mount_path": "/mnt/hdfs/",
            },
        }

        LocalUFSUpdater(ufs_config)

        # Verify parse_ufs_info result (it's called in __init__)
        # Since we mocked register_ufs_fallback, we can check what it was called with
        assert mock_register.call_count == 1
        args, _ = mock_register.call_args
        ufs_info_list = args[0]

        assert len(ufs_info_list) == 2

        # Check s3 info
        s3_info = next(
            info
            for info in ufs_info_list
            if info.ufs_full_path == "s3://bucket1"
        )
        assert s3_info.alluxio_path == "s3://bucket1"
        assert s3_info.options == {
            "access_key": "my_key",
            "secret_key": "my_secret",
            "endpoint": "http://s3.amazonaws.com",
            "ufs_mount_path": "/mnt/s3",
        }

        # Check hdfs info
        hdfs_info = next(
            info
            for info in ufs_info_list
            if info.ufs_full_path == "hdfs://namenode"
        )
        assert hdfs_info.alluxio_path == "hdfs://namenode"
        assert hdfs_info.options == {
            "user": "hadoop",
            "ufs_mount_path": "/mnt/hdfs/",
        }

    @patch("alluxiofs.client.ufs_manager.setup_logger")
    @patch(
        "alluxiofs.client.ufs_manager.LocalUFSUpdater.register_ufs_fallback"
    )
    def test_parse_ufs_info_invalid_config(self, mock_register, mock_logger):
        from alluxiofs.client.ufs_manager import LocalUFSUpdater

        ufs_config = {"s3://bucket1": "not a dict"}

        LocalUFSUpdater(ufs_config)

        args, _ = mock_register.call_args
        ufs_info_list = args[0]
        assert len(ufs_info_list) == 0


class TestUFSManager:
    @patch("alluxiofs.client.ufs_manager.UFSUpdater")
    def test_init_with_alluxio(self, mock_ufs_updater):
        from alluxiofs.client.ufs_manager import UFSManager

        mock_alluxio = MagicMock()
        manager = UFSManager(alluxio=mock_alluxio)

        mock_ufs_updater.assert_called_once_with(mock_alluxio)
        assert manager.ufs_updater == mock_ufs_updater.return_value

    @patch("alluxiofs.client.ufs_manager.LocalUFSUpdater")
    def test_init_with_config(self, mock_local_ufs_updater):
        from alluxiofs.client.ufs_manager import UFSManager

        config = {"s3://bucket": {"key": "val"}}
        manager = UFSManager(config=config)

        mock_local_ufs_updater.assert_called_once_with(config)
        assert manager.ufs_updater == mock_local_ufs_updater.return_value

    def test_init_with_none(self):
        from alluxiofs.client.ufs_manager import UFSManager

        manager = UFSManager()
        assert manager.ufs_updater is None

    def test_lifecycle_methods(self):
        from alluxiofs.client.ufs_manager import UFSManager

        mock_updater = MagicMock()
        manager = UFSManager()
        manager.ufs_updater = mock_updater

        manager.initialize_ufs_manager()
        mock_updater.start_updater.assert_called_once()

        manager.shutdown_ufs_manager()
        mock_updater.stop_updater.assert_called_once()

    def test_delegation_methods(self):
        from alluxiofs.client.ufs_manager import UFSManager

        mock_updater = MagicMock()
        manager = UFSManager()
        manager.ufs_updater = mock_updater

        # Test must_get_ufs_count
        mock_updater.must_get_ufs_count.return_value = 5
        assert manager.must_get_ufs_count() == 5
        mock_updater.must_get_ufs_count.assert_called_once()

        # Test must_get_ufs_from_path
        mock_updater.must_get_ufs_from_path.return_value = "ufs_instance"
        assert manager.must_get_ufs_from_path("path") == "ufs_instance"
        mock_updater.must_get_ufs_from_path.assert_called_once_with("path")

        # Test must_get_alluxio_path_from_ufs_full_path
        mock_updater.must_get_alluxio_path_from_ufs_full_path.return_value = (
            "/alluxio/path"
        )
        assert (
            manager.must_get_alluxio_path_from_ufs_full_path("path")
            == "/alluxio/path"
        )
        mock_updater.must_get_alluxio_path_from_ufs_full_path.assert_called_once_with(
            "path"
        )

        # Test get_ufs_count
        mock_updater.get_ufs_count.return_value = 3
        assert manager.get_ufs_count() == 3
        mock_updater.get_ufs_count.assert_called_once()

        # Test get_ufs_from_cache
        mock_updater.get_ufs_from_cache.return_value = "cached_ufs"
        assert manager.get_ufs_from_cache("path") == "cached_ufs"
        mock_updater.get_ufs_from_cache.assert_called_once_with("path")

        # Test get_alluxio_path_from_ufs_full_path
        mock_updater.get_alluxio_path_from_ufs_full_path.return_value = (
            "/cached/path"
        )
        assert (
            manager.get_alluxio_path_from_ufs_full_path("path")
            == "/cached/path"
        )
        mock_updater.get_alluxio_path_from_ufs_full_path.assert_called_once_with(
            "path"
        )

    def test_delegation_methods_no_updater(self):
        from alluxiofs.client.ufs_manager import UFSManager

        manager = UFSManager()
        assert manager.ufs_updater is None

        assert manager.must_get_ufs_count() == 0
        assert manager.must_get_ufs_from_path("path") is None
        assert manager.must_get_alluxio_path_from_ufs_full_path("path") is None
        assert manager.get_ufs_count() == 0
        assert manager.get_ufs_from_cache("path") is None
        assert manager.get_alluxio_path_from_ufs_full_path("path") is None
