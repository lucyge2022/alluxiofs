from setuptools import find_packages
from setuptools import setup
from setuptools_rust import Binding
from setuptools_rust import RustExtension

setup(
    name="alluxiofs",
    version="1.0.1.dev1",
    description="Alluxio Fsspec provides Alluxio filesystem spec implementation.",
    url="https://github.com/fsspec/alluxiofs",
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    rust_extensions=[
        RustExtension(
            "alluxiofs.alluxiocommon.alluxiocommon",
            "alluxiofs/alluxiocommon/Cargo.toml",
            binding=Binding.PyO3,
        )
    ],
    packages=find_packages(exclude=["tests", "tests.*"]),
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        # Alluxio fs dependencies
        "fsspec",
        # Alluxio client dependencies
        "aiohttp",
        "decorator",
        "humanfriendly",
        "requests",
        "etcd3",
        "mmh3",
        "sortedcontainers",
        "protobuf>=3.20.0,<3.21.0",
    ],
    extras_require={
        "tests": [
            "pytest",
            "pytest-aiohttp",
            "ray",
            "pyarrow",
        ]
    },
    python_requires=">=3.8",
    maintainer="Lu Qiu",
    maintainer_email="luqiujob@gmail.com",
    entry_points={
        "fsspec.specs": [
            "alluxio=alluxiofs.AlluxioFileSystem",
        ],
    },
)
