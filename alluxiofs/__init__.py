import fsspec

from alluxiofs.client import AlluxioClient
from alluxiofs.checkpoint import CheckpointReader, CheckpointWriter
from alluxiofs.core import AlluxioFileSystem
from alluxiofs.telemetry import AlluxioTelemetry, instrument_alluxio_client
from alluxiofs.torch_dataset import AlluxioDataset, AlluxioIterableDataset

fsspec.register_implementation("alluxiofs", AlluxioFileSystem, clobber=True)

__all__ = [
    "AlluxioFileSystem",
    "AlluxioClient",
    "CheckpointWriter",
    "CheckpointReader",
    "AlluxioDataset",
    "AlluxioIterableDataset",
    "AlluxioTelemetry",
    "instrument_alluxio_client",
]
