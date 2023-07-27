from __future__ import annotations

from io import BytesIO
from typing import Any, Generator
from unittest import mock

from flow.record import Record, RecordAdapter, RecordDescriptor, RecordStreamWriter
from flow.record.adapter.gcs import GcsReader


def generate_records(amount) -> Generator[Record, Any, None]:
    TestRecordWithFooBar = RecordDescriptor(
        "test/record",
        [
            ("string", "name"),
            ("string", "foo"),
            ("varint", "idx"),
        ],
    )
    for i in range(amount):
        yield TestRecordWithFooBar(name=f"record{i}", foo="bar", idx=i)


class MockClient:
    def __init__(self, project: str) -> None:
        self.project = project
        self._blobs = {}

    def bucket(self, bucket_name: str) -> str:
        return bucket_name

    def list_blobs(self, bucket_or_name: str, prefix: str) -> Generator[MockBlob, Any, None]:
        for blob_name, blob_data in self._blobs.items():
            if blob_name.startswith(prefix):
                yield MockBlob(name=blob_name, data=blob_data)

    def close(self):
        pass

    def _add_data_as_blob(self, name: str, data: bytes) -> None:
        self._blobs[name] = data

    def _add_records_as_blob(self, name, records: list[Record]) -> None:
        fp = BytesIO()
        writer = RecordStreamWriter(fp)
        for record in records:
            writer.write(record)
        self._add_data_as_blob(name, fp.getbuffer())


class MockBlob:
    def __init__(self, name: str, data: bytes) -> None:
        self.name = name
        self.data = data
        self.size = len(data)


class MockBlobReader(BytesIO):
    def __init__(self, blob: MockBlob) -> None:
        super().__init__(blob.data)


@mock.patch("google.cloud.storage.client.Client", MockClient)
def test_gcs_uri_and_path():
    adapter = RecordAdapter("gcs://test-project:test-bucket?path=/path/to/records/*/*.avro")

    assert isinstance(adapter, GcsReader)

    assert adapter.gcs.project == "test-project"
    assert adapter.bucket == "test-bucket"
    assert adapter.prefix == "/path/to/records/"
    assert adapter.glob == "/path/to/records/*/*.avro"


@mock.patch("google.cloud.storage.client.Client", MockClient)
@mock.patch("google.cloud.storage.fileio.BlobReader", MockBlobReader)
def test_gcs_reader_glob():
    adapter = RecordAdapter("gcs://test-project:test-bucket?path=/path/to/records/*/results/*.records")
    adapter.gcs.close = mock.MagicMock()
    # Add mock records
    test_records = list(generate_records(10))
    adapter.gcs._add_records_as_blob("/path/to/records/subfolder/results/tests.records", test_records)
    adapter.gcs._add_records_as_blob("/path/to/records/subfolder2/donotselect/false.records", test_records)

    found_records = list(adapter)

    assert len(found_records) == 10
    for idx, record in enumerate(adapter):
        assert record.foo == "bar"
        assert record == test_records[idx]

    adapter.close()
    adapter.gcs.close.assert_called()


@mock.patch("google.cloud.storage.client.Client", MockClient)
@mock.patch("google.cloud.storage.fileio.BlobReader", MockBlobReader)
def test_gcs_reader_empty_file():
    adapter = RecordAdapter("gcs://test-project:test-bucket?path=/path/to/records/emptyfile.records")
    adapter.gcs._add_data_as_blob("/path/to/records/emptyfile.records", b"")

    found_records = list(adapter)
    assert len(found_records) == 0


@mock.patch("google.cloud.storage.client.Client", MockClient)
@mock.patch("google.cloud.storage.fileio.BlobReader", MockBlobReader)
def test_gcs_reader_selector():
    adapter = RecordAdapter("gcs://test-project:test-bucket?path=/test.records", selector="r.idx >= 3")
    assert isinstance(adapter, GcsReader)
    # Add mock records
    test_records = list(generate_records(10))
    adapter.gcs._add_records_as_blob("/test.records", test_records)

    found_records = list(adapter)
    assert len(found_records) == 5
    assert found_records[0].idx == 5
    assert found_records[4].idx == 9
