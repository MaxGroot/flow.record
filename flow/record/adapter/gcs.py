import logging
from fnmatch import fnmatch
from typing import Iterator, Union

import google.cloud.storage.client as gcs_client
import google.cloud.storage.fileio as gcs_io

from flow.record.adapter import AbstractReader
from flow.record.base import Record, RecordAdapter
from flow.record.selector import CompiledSelector, Selector

__usage__ = """
Google Cloud Storage adapter
---
Read usage: rdump gcs://[PROJECT-ID]:[BUCKET-ID]?path=[PATH]
[PROJECT-ID]: Google Cloud Project ID
[BUCKET-ID]: Bucket ID
[path]: Path to look for files, with support for glob-pattern matching
"""

log = logging.getLogger(__name__)

GLOB_CHARACTERS = "*?[]"


class GcsReader(AbstractReader):
    def __init__(self, uri: str, path: str, selector: Union[None, Selector, CompiledSelector] = None, **kwargs) -> None:
        self.selector = selector
        project_name = uri[: uri.find(":")]
        bucket_name = uri[uri.find(":") + 1 :]

        self.gcs = gcs_client.Client(project=project_name)
        self.bucket = self.gcs.bucket(bucket_name)

        # GCS Doesn't support iterating blobs using a glob pattern, so we have to do that ourselves.
        # To split the path prefix from the glob-specific stuff, we have to find the first place where
        # the glob starts. We'll then go through all files that match the path prefix before the glob,
        # and do fnmatch ourselves to check whether any given blob matches with our glob.
        lowest_pos = min([path.find(char) if path.find(char) >= 0 else float("inf") for char in GLOB_CHARACTERS])
        if lowest_pos == float("inf"):
            # No glob character was found
            self.glob = None
            self.prefix = path
        else:
            # Split the glob and the prefix
            self.prefix = path[:lowest_pos]
            self.glob = path[lowest_pos:]

    def __iter__(self) -> Iterator[Record]:
        blobs = self.gcs.list_blobs(bucket_or_name=self.bucket, prefix=self.prefix)
        for blob in blobs:
            if blob.size == 0:
                continue
            if self.glob and not fnmatch(blob.name, self.glob):
                continue
            blobreader = gcs_io.BlobReader(blob)

            reader = RecordAdapter(blobreader, out=False)
            for record in reader:
                yield record

    def close(self) -> None:
        self.gcs.close()
