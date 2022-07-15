import json
from abc import ABC, abstractmethod
from base64 import b64decode
from typing import Any, Dict, Optional

from google.cloud.storage.blob import Blob
from google.cloud.storage.client import Client


class File(ABC):
    """
    Abstract base class defining the interface for different concrete file classes to
    implement.
    """

    @abstractmethod
    def __init__(
        self,
        name: str,
    ) -> None:
        self.filename = name

    @property
    @abstractmethod
    def md5sum(self) -> str:
        raise NotImplementedError

    @property
    @abstractmethod
    def size(self) -> int:
        raise NotImplementedError

    @abstractmethod
    def read(self, num_bytes: Optional[int] = None) -> bytes:
        raise NotImplementedError


class GSFile(File):
    """
    Wrapper around GCS blob class to better map to portal metadata and provide a read()
    interface for in-memory transfer to s3
    """

    SCHEME = "gs://"

    def __init__(
        self,
        name: str,
        client: Optional[Client] = None,
    ) -> None:
        """
        Initializes self.pos to 0 for keeping track of number of bytes read from file.
        """
        super().__init__(name)
        self.pos = 0
        self._blob: Optional[Blob] = None
        self._client: Optional[Client] = client

    @property
    def blob(self) -> Blob:
        if self._blob is None:
            blob = Blob.from_string(self.filename, client=self.client)
            blob.reload()
            self._blob = blob
        return self._blob

    @property
    def client(self) -> Client:
        if self._client is None:
            client = Client()
            self._client = client
        return self._client

    @property
    def md5sum(self) -> str:
        """
        Returns md5sum of the file in hex. Need to wrap around gcloud API's md5sums,
        which are returned as base64, to match IGVF portal md5sums.
        """
        return self.b64_to_hex(self.blob.md5_hash)

    @property
    def size(self) -> int:
        return self.blob.size

    @staticmethod
    def b64_to_hex(value: str) -> str:
        return b64decode(value).hex()

    def read(self, num_bytes: Optional[int] = None) -> bytes:
        """
        `Blob.download_as_string()` takes `start` and `end` kwargs to specify a byte
        range. These are 0-indexed and inclusive of endpoints. If the position is
        greater than or equal to the size of the object then we treat that as EOF and
        return an empty byte string `b''`. As per Python convention, when read() is
        called with no read size then the remainder of the file is returned.
        See https://googleapis.dev/python/storage/latest/blobs.html#google.cloud.Blob.download_as_string
        """
        if self.pos >= self.size:
            read_bytes = b""
        else:
            if num_bytes is None:
                read_bytes = self.blob.download_as_string(start=self.pos)
                self.pos += len(read_bytes)
            else:
                read_bytes = self.blob.download_as_string(
                    start=self.pos, end=self.pos + num_bytes - 1
                )
                self.pos += num_bytes
        return read_bytes
