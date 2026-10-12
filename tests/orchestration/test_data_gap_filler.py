"""Tests for the WebPODA data gap filler asset and sensor."""

import csv
import datetime
import logging
from unittest import mock

import imap_data_access
import pytest
from botocore.exceptions import ClientError
from dagster import (
    AssetKey,
    DagsterInstance,
    Failure,
    RunConfig,
    build_sensor_context,
    materialize,
)
from imap_data_access import webpoda
from imap_data_access.io import IMAPDataAccessError

from sds_data_manager.orchestration import data_gap_filler
from sds_data_manager.orchestration.data_gap_filler import (
    ASSET_NAME,
    DataGapFillerConfig,
    UploadedFile,
    find_uploaded_files,
    get_default_window,
    resolve_instruments,
    resolve_window,
)
from sds_data_manager.orchestration.imap_dagster import defs

BUCKET_NAME = "test-data-bucket"
WEBPODA_TOKEN = "fake-webpoda-token"  # noqa: S105 (a fake token)
API_KEY = "fake-api-key"
# When the production L0 files were uploaded, long before any test run
PRODUCTION_UPLOAD_TIME = datetime.datetime(2026, 7, 13, tzinfo=datetime.timezone.utc)
PRODUCTION_CONTENT = b"packets received on time"
NEW_CONTENT = b"packets received on time and packets received late"
REPOINT_ROWS = [
    {"repoint_id": "1", "repoint_end_utc": "2026-07-09 01:00:00.000"},
    {"repoint_id": "2", "repoint_end_utc": "2026-07-10 01:00:00.000"},
]
LIBRARY_LOGGER = logging.getLogger("imap_data_access.webpoda")


def get_packet_binary_data_sctime(instrument):
    """Log a line for an APID, like the webpoda function with this name does."""
    LIBRARY_LOGGER.info(f"Querying {instrument} APID 1 by SCT")


def l0_path(instrument, minor_version, repointing=None):
    """Get the path of an L0 file for 2026-07-09 in the current DATA_DIR."""
    return imap_data_access.ScienceFilePath.generate_from_inputs(
        instrument=instrument,
        data_level="l0",
        descriptor="raw",
        start_time="20260709",
        repointing=repointing,
        major_version=1,
        minor_version=minor_version,
    ).construct_path()


def write_file(path, content):
    """Write content to path, creating its directory."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return path


class FakeS3Client:
    """Stand-in for the data bucket that tracks when each file was written."""

    def __init__(self):
        """Start with an empty bucket."""
        self.objects = {}

    def put(self, path, last_modified=None):
        """Add a file in the current DATA_DIR, with the upload API's S3 key."""
        key = path.relative_to(imap_data_access.config["DATA_DIR"]).as_posix()
        self.objects[key] = last_modified or datetime.datetime.now(
            datetime.timezone.utc
        )

    def head_object(self, Bucket, Key):  # noqa: N803 (the boto3 argument names)
        """Get the LastModified time of an object, like S3 does."""
        assert Bucket == BUCKET_NAME
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")
        return {"LastModified": self.objects[Key]}

    def head_bucket(self, Bucket):  # noqa: N803 (the boto3 argument name)
        """Check that the bucket exists."""
        if Bucket != BUCKET_NAME:
            raise ClientError({"Error": {"Code": "404"}}, "HeadBucket")
        return {}


class FakeWebPODA:
    """Stand-in for the imap_data_access.webpoda data gap checker.

    ``outcomes`` maps instruments to what checking their first day (or pointing)
    finds: "changed" data, data for a day with no L0 file yet ("without_l0"),
    the "same" data as production, data that fails to upload ("upload_error"),
    or an "exception". Every instrument finds the "same" data by default.
    """

    def __init__(self, s3_client):
        """Record uploads in s3_client."""
        self.s3_client = s3_client
        self.outcomes = {}
        self.calls = {}
        self.repoint_rows = REPOINT_ROWS

    def download_daily_data(self, **kwargs):
        """Check the days in a time range, like webpoda.download_daily_data."""
        self._check(repointing=None, **kwargs)

    def download_repointing_data(self, **kwargs):
        """Check the pointings in a time range, like the webpoda function."""
        self._check(repointing=1, **kwargs)

    def get_repoint_file(self):
        """Download the latest repoint table, or return None if there isn't one."""
        if not self.repoint_rows:
            return None
        path = imap_data_access.config["DATA_DIR"] / "imap_2026_190_01.repoint.csv"
        with open(path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=self.repoint_rows[0])
            writer.writeheader()
            writer.writerows(self.repoint_rows)
        return path

    def _check(self, instrument, repointing, **kwargs):
        self.calls[instrument] = {**kwargs, "config": dict(imap_data_access.config)}
        outcome = self.outcomes.get(instrument, "same")
        if outcome == "exception":
            raise IMAPDataAccessError("WebPODA is unavailable")
        get_packet_binary_data_sctime(instrument)
        LIBRARY_LOGGER.info("-" * 40)

        minor_version = 1
        if outcome in ("changed", "same"):
            # The latest production file is downloaded to compare against
            production_file = write_file(
                l0_path(instrument, 1, repointing), PRODUCTION_CONTENT
            )
            self.s3_client.put(production_file, PRODUCTION_UPLOAD_TIME)
            minor_version = 2
        content = PRODUCTION_CONTENT if outcome == "same" else NEW_CONTENT
        new_file = write_file(l0_path(instrument, minor_version, repointing), content)
        LIBRARY_LOGGER.info(f"Saving binary data to {new_file.name}")
        if outcome == "same":
            LIBRARY_LOGGER.info(f"{new_file.name} is a duplicate")
        elif outcome == "upload_error":
            LIBRARY_LOGGER.error(f"Failed to upload {new_file.name} to the server")
        else:
            self.s3_client.put(new_file)


@pytest.fixture
def fake_s3(monkeypatch):
    """Replace the data bucket with a FakeS3Client."""
    s3_client = FakeS3Client()
    monkeypatch.setattr(data_gap_filler, "S3_CLIENT", s3_client)
    return s3_client


@pytest.fixture
def credentials(monkeypatch):
    """Mock the WebPODA token secret and the IMAP API key parameter."""
    secrets_manager_client = mock.Mock()
    secrets_manager_client.get_secret_value.return_value = {
        "SecretString": WEBPODA_TOKEN
    }
    ssm_client = mock.Mock()
    ssm_client.get_parameter.return_value = {"Parameter": {"Value": API_KEY}}
    monkeypatch.setattr(
        data_gap_filler, "SECRETS_MANAGER_CLIENT", secrets_manager_client
    )
    monkeypatch.setattr(data_gap_filler, "SSM_CLIENT", ssm_client)
    return secrets_manager_client, ssm_client


@pytest.fixture
def fake_webpoda(monkeypatch, fake_s3, credentials):
    """Replace the imap_data_access.webpoda data gap checker with a FakeWebPODA."""
    fake = FakeWebPODA(fake_s3)
    monkeypatch.setattr(webpoda, "download_daily_data", fake.download_daily_data)
    monkeypatch.setattr(
        webpoda, "download_repointing_data", fake.download_repointing_data
    )
    monkeypatch.setattr(webpoda, "get_repoint_file", fake.get_repoint_file)
    return fake


@pytest.fixture
def dagster_instance():
    """Provide an in-memory Dagster instance."""
    return DagsterInstance.ephemeral()


def run_data_gap_filler(instance, **config):
    """Materialize the data gap filler asset with the given run config."""
    return materialize(
        [data_gap_filler.webpoda_data_gap_filler],
        instance=instance,
        run_config=RunConfig(ops={ASSET_NAME: DataGapFillerConfig(**config)}),
        raise_on_error=False,
    )


def get_run_logs(instance, run_id):
    """Get the (level, message) of every log message of a run."""
    return [
        (entry.level, entry.user_message)
        for entry in instance.all_logs(run_id)
        if not entry.is_dagster_event
    ]


def get_failure(result):
    """Get the Failure that the asset raised."""
    (failure_event,) = result.get_step_failure_events()
    return failure_event.step_failure_data.user_failure_data


def test_get_default_window():
    """Test that runs check a month of data ending a week ago by default."""
    assert get_default_window(datetime.date(2026, 10, 12)) == (
        datetime.date(2026, 9, 5),
        datetime.date(2026, 10, 5),
    )
    assert get_default_window() == get_default_window(
        datetime.datetime.now(datetime.timezone.utc).date()
    )


def test_resolve_window():
    """Test the dates a run checks, which default to the default window."""
    assert resolve_window(DataGapFillerConfig()) == get_default_window()
    assert resolve_window(DataGapFillerConfig(end_date="20260712")) == (
        datetime.date(2026, 6, 12),
        datetime.date(2026, 7, 12),
    )
    assert resolve_window(
        DataGapFillerConfig(start_date="20260709", end_date="20260709")
    ) == (datetime.date(2026, 7, 9), datetime.date(2026, 7, 9))

    with pytest.raises(Failure, match="Invalid start_date '2026-07-09'"):
        resolve_window(DataGapFillerConfig(start_date="2026-07-09"))
    with pytest.raises(Failure, match="start_date 2026-07-12 is after end_date"):
        resolve_window(DataGapFillerConfig(start_date="20260712", end_date="20260709"))


def test_resolve_instruments():
    """Test that every instrument is checked by default."""
    assert resolve_instruments(None) == sorted(webpoda.INSTRUMENT_APIDS)
    assert resolve_instruments(["swe", "mag", "swe"]) == ["mag", "swe"]
    with pytest.raises(Failure, match=r"Unknown instrument\(s\) \['bad'\]"):
        resolve_instruments(["mag", "bad"])


def test_sensor_requests_weekly_run(monkeypatch):
    """Test that the sensor requests a run for the default window once a week."""
    sensor = defs.get_sensor_def("webpoda_data_gap_filler_sensor")
    assert sensor.minimum_interval_seconds == 7 * 24 * 60 * 60
    assert sensor.asset_selection.resolve(defs.resolve_asset_graph()) == {
        AssetKey(ASSET_NAME)
    }

    monkeypatch.setattr(
        data_gap_filler,
        "get_default_window",
        lambda: (datetime.date(2026, 9, 5), datetime.date(2026, 10, 5)),
    )
    run_request = sensor(build_sensor_context())

    # The same window always gets the same run key, so it is only run once
    assert run_request.run_key == "webpoda_data_gap_filler_20260905_20261005"
    assert run_request.run_config["ops"] == {
        ASSET_NAME: {"config": {"start_date": "20260905", "end_date": "20261005"}}
    }


def test_find_uploaded_files(fake_s3, tmp_path):
    """Test that only files written to the bucket during the check count."""
    started_at = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0)
    with data_gap_filler._imap_data_access_config(DATA_DIR=tmp_path):
        # A new version of a production file
        production_file = write_file(l0_path("mag", 1), PRODUCTION_CONTENT)
        fake_s3.put(production_file, PRODUCTION_UPLOAD_TIME)
        new_file = write_file(l0_path("mag", 2), NEW_CONTENT)
        fake_s3.put(new_file, started_at)
        # A duplicate of a production file, which isn't uploaded
        fake_s3.put(
            write_file(l0_path("swe", 1), PRODUCTION_CONTENT), PRODUCTION_UPLOAD_TIME
        )
        write_file(l0_path("swe", 2), PRODUCTION_CONTENT)
        # A file for a pointing without an L0 file yet
        pointing_file = write_file(l0_path("hi", 1, repointing=1), NEW_CONTENT)
        fake_s3.put(pointing_file)
        # Not an IMAP file
        write_file(tmp_path / "notes.txt", b"notes")

        uploaded_files = find_uploaded_files(tmp_path, BUCKET_NAME, started_at)

    assert uploaded_files == [
        UploadedFile(pointing_file.name, len(NEW_CONTENT)),
        UploadedFile(
            new_file.name,
            len(NEW_CONTENT),
            replaced_name=production_file.name,
            replaced_size=len(PRODUCTION_CONTENT),
        ),
    ]


def test_find_uploaded_files_in_s3(monkeypatch, s3_client, tmp_path):
    """Test finding uploaded files in a (mocked) S3 bucket."""
    monkeypatch.setattr(data_gap_filler, "S3_CLIENT", s3_client)
    started_at = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(
        hours=1
    )
    with data_gap_filler._imap_data_access_config(DATA_DIR=tmp_path):
        uploaded_file = write_file(l0_path("mag", 1), NEW_CONTENT)
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=uploaded_file.relative_to(tmp_path).as_posix(),
            Body=NEW_CONTENT,
        )
        # A duplicate that wasn't uploaded
        write_file(l0_path("swe", 2), PRODUCTION_CONTENT)

        uploaded_files = find_uploaded_files(tmp_path, BUCKET_NAME, started_at)

    assert uploaded_files == [UploadedFile(uploaded_file.name, len(NEW_CONTENT))]


def test_data_gap_filler_logs_new_data(
    dagster_instance, fake_webpoda, credentials, monkeypatch
):
    """Test that new data is uploaded, logged and recorded in the metadata."""
    monkeypatch.setenv("SSM_API_KEY_PARAMETER", "/test/api-key")
    fake_webpoda.outcomes = {"mag": "changed", "hit": "without_l0"}
    original_config = dict(imap_data_access.config)
    library_logger = logging.getLogger("imap_data_access")
    original_level = library_logger.level
    original_handlers = list(library_logger.handlers)

    result = run_data_gap_filler(
        dagster_instance,
        start_date="20260709",
        end_date="20260712",
        instruments=["swe", "mag", "hit"],
    )

    assert result.success
    metadata = result.asset_materializations_for_node(ASSET_NAME)[0].metadata
    assert metadata["start_date"].value == "20260709"
    assert metadata["end_date"].value == "20260712"
    assert metadata["query_mode"].value == "sct"
    assert metadata["instruments_checked"].value == ["hit", "mag", "swe"]
    assert metadata["new_data_found"].value is True
    assert metadata["num_uploaded_files"].value == 2
    assert metadata["uploaded_files"].value == {
        "hit": ["imap_hit_l0_raw_20260709_v001.0001.pkts"],
        "mag": ["imap_mag_l0_raw_20260709_v001.0002.pkts"],
    }
    assert "errors" not in metadata

    hit_upload = (
        f"imap_hit_l0_raw_20260709_v001.0001.pkts "
        f"({webpoda.format_size(len(NEW_CONTENT))}), no previous L0 file"
    )
    mag_upload = (
        f"imap_mag_l0_raw_20260709_v001.0002.pkts "
        f"({webpoda.format_size(len(NEW_CONTENT))}), replacing "
        f"imap_mag_l0_raw_20260709_v001.0001.pkts "
        f"({webpoda.format_size(len(PRODUCTION_CONTENT))})"
    )
    logs = get_run_logs(dagster_instance, result.run_id)
    assert (logging.WARNING, f"[hit] NEW DATA FOUND, uploaded {hit_upload}") in logs
    assert (logging.WARNING, f"[mag] NEW DATA FOUND, uploaded {mag_upload}") in logs
    assert (logging.INFO, "[swe] No new data found") in logs
    assert logs[-1] == (
        logging.WARNING,
        "WebPODA data gap filler summary for spacecraft dates 2026-07-09 to "
        "2026-07-12:\n"
        "NEW DATA FOUND, uploaded 2 new L0 file(s) for 2 instrument(s):\n"
        f"  hit: {hit_upload}\n"
        f"  mag: {mag_upload}\n"
        "No new data: swe",
    )
    # The library's logs are forwarded, except the per-APID and separator lines
    messages = [message for _, message in logs]
    assert "[swe] imap_swe_l0_raw_20260709_v001.0002.pkts is a duplicate" in messages
    assert not any("APID" in message for message in messages)
    assert not any(message.endswith("---") for message in messages)

    # Each instrument is checked by spacecraft time and uploads what changed
    assert fake_webpoda.calls.keys() == {"hit", "mag", "swe"}
    data_dirs = set()
    for call in fake_webpoda.calls.values():
        assert call["start_time"] == datetime.datetime(2026, 7, 9)
        assert call["end_time"] == datetime.datetime(2026, 7, 12, 23, 59, 59, 999999)
        assert call["query_by_ert"] is False
        assert call["upload_to_sdc"] is True
        assert call["config"]["WEBPODA_TOKEN"] == WEBPODA_TOKEN
        assert call["config"]["API_KEY"] == API_KEY
        data_dirs.add(call["config"]["DATA_DIR"])
    # in temporary data directories that are cleaned up
    assert len(data_dirs) == 3
    assert not any(data_dir.exists() for data_dir in data_dirs)
    _, ssm_client = credentials
    ssm_client.get_parameter.assert_called_once_with(
        Name="/test/api-key", WithDecryption=True
    )

    # The global imap_data_access config and logger are restored
    assert imap_data_access.config == original_config
    assert library_logger.level == original_level
    assert library_logger.handlers == original_handlers


def test_data_gap_filler_no_new_data(dagster_instance, fake_webpoda):
    """Test a run that doesn't find new data."""
    result = run_data_gap_filler(
        dagster_instance,
        start_date="20260709",
        end_date="20260712",
        instruments=["mag", "swe"],
    )

    assert result.success
    metadata = result.asset_materializations_for_node(ASSET_NAME)[0].metadata
    assert metadata["new_data_found"].value is False
    assert metadata["num_uploaded_files"].value == 0
    assert metadata["uploaded_files"].value == {}
    logs = get_run_logs(dagster_instance, result.run_id)
    assert (logging.INFO, "[mag] No new data found") in logs
    assert (logging.INFO, "[swe] No new data found") in logs
    assert logs[-1] == (
        logging.INFO,
        "WebPODA data gap filler summary for spacecraft dates 2026-07-09 to "
        "2026-07-12:\nNo new data found.\nNo new data: mag, swe",
    )
    assert not any(level >= logging.WARNING for level, _ in logs)


def test_data_gap_filler_checks_pointings(dagster_instance, fake_webpoda):
    """Test that ENA instruments are checked by pointing."""
    fake_webpoda.outcomes = {"hi": "changed"}

    result = run_data_gap_filler(
        dagster_instance,
        start_date="20260709",
        end_date="20260712",
        instruments=["hi", "mag"],
    )

    assert result.success
    for call in fake_webpoda.calls.values():
        assert call["start_time"] == datetime.datetime(2026, 7, 9)
        assert call["end_time"] == datetime.datetime(2026, 7, 12, 23, 59, 59, 999999)
        assert call["query_by_ert"] is False
        assert call["upload_to_sdc"] is True
    assert fake_webpoda.calls["hi"]["repoint_data"] == REPOINT_ROWS
    assert "repoint_data" not in fake_webpoda.calls["mag"]
    metadata = result.asset_materializations_for_node(ASSET_NAME)[0].metadata
    assert metadata["uploaded_files"].value == {
        "hi": ["imap_hi_l0_raw_20260709-repoint00001_v001.0002.pkts"]
    }
    logs = get_run_logs(dagster_instance, result.run_id)
    assert (
        logging.INFO,
        "Using repoint table imap_2026_190_01.repoint.csv (2 repointings)",
    ) in logs


def test_data_gap_filler_without_repoint_table(dagster_instance, fake_webpoda):
    """Test that the other instruments are still checked without a repoint table."""
    fake_webpoda.repoint_rows = None
    fake_webpoda.outcomes = {"mag": "changed"}

    result = run_data_gap_filler(
        dagster_instance,
        start_date="20260709",
        end_date="20260712",
        instruments=["hi", "mag"],
    )

    assert not result.success
    assert fake_webpoda.calls.keys() == {"mag"}
    failure = get_failure(result)
    assert failure.description.startswith("The data gap check failed for: hi.")
    assert failure.metadata["errors"].value == {
        "hi": [
            "Could not get the repoint table: No repoint table was ingested in the "
            "last two weeks."
        ]
    }
    assert failure.metadata["uploaded_files"].value == {
        "mag": ["imap_mag_l0_raw_20260709_v001.0002.pkts"]
    }


def test_data_gap_filler_errors(dagster_instance, fake_webpoda):
    """Test that errors don't stop the other instruments from being checked."""
    fake_webpoda.outcomes = {
        "hit": "exception",
        "mag": "changed",
        "swe": "upload_error",
    }

    result = run_data_gap_filler(
        dagster_instance,
        start_date="20260709",
        end_date="20260712",
        instruments=["hit", "mag", "swapi", "swe"],
    )

    assert not result.success
    assert fake_webpoda.calls.keys() == {"hit", "mag", "swapi", "swe"}
    failure = get_failure(result)
    assert failure.description == (
        "The data gap check failed for: hit, swe. Any new data that was found is "
        "still listed in the metadata and the logs."
    )
    assert failure.metadata["errors"].value == {
        "hit": ["IMAPDataAccessError: WebPODA is unavailable"],
        "swe": [
            "Failed to upload imap_swe_l0_raw_20260709_v001.0001.pkts to the server"
        ],
    }
    assert failure.metadata["new_data_found"].value is True
    assert failure.metadata["uploaded_files"].value == {
        "mag": ["imap_mag_l0_raw_20260709_v001.0002.pkts"]
    }

    logs = get_run_logs(dagster_instance, result.run_id)
    assert (logging.ERROR, "[hit] Data gap check finished with errors") in logs
    assert (logging.ERROR, "[swe] Data gap check finished with errors") in logs
    assert (logging.INFO, "[swapi] No new data found") in logs
    summary = next(
        message
        for _, message in logs
        if message.startswith("WebPODA data gap filler summary")
    )
    assert summary.endswith(
        "No new data: swapi\n"
        "ERRORS, so some data may not have been checked or uploaded:\n"
        "  hit: IMAPDataAccessError: WebPODA is unavailable\n"
        "  swe: Failed to upload imap_swe_l0_raw_20260709_v001.0001.pkts to the "
        "server"
    )
    assert (logging.ERROR, summary) in logs


def test_data_gap_filler_upload_check_error(dagster_instance, fake_webpoda, fake_s3):
    """Test that a failure to check what was uploaded is reported."""
    fake_s3.head_object = mock.Mock(
        side_effect=ClientError({"Error": {"Code": "403"}}, "HeadObject")
    )

    result = run_data_gap_filler(
        dagster_instance,
        start_date="20260709",
        end_date="20260709",
        instruments=["mag"],
    )

    assert not result.success
    (error,) = get_failure(result).metadata["errors"].value["mag"]
    assert error.startswith("Could not check what was uploaded:")


@pytest.mark.parametrize(
    ("config", "description"),
    [
        ({"instruments": ["bad"]}, "Unknown instrument(s) ['bad']"),
        ({"start_date": "July 9"}, "Invalid start_date 'July 9', expected YYYYMMDD."),
    ],
)
def test_data_gap_filler_invalid_config(
    dagster_instance, fake_webpoda, config, description
):
    """Test that an invalid run config fails before anything is checked."""
    result = run_data_gap_filler(dagster_instance, **config)

    assert not result.success
    assert get_failure(result).description.startswith(description)
    assert not fake_webpoda.calls


def test_data_gap_filler_missing_credentials(
    dagster_instance, fake_webpoda, credentials
):
    """Test that the run fails before checking anything without a WebPODA token."""
    secrets_manager_client, _ = credentials
    secrets_manager_client.get_secret_value.side_effect = ClientError(
        {"Error": {"Code": "ResourceNotFoundException"}}, "GetSecretValue"
    )

    result = run_data_gap_filler(dagster_instance, instruments=["mag"])

    assert not result.success
    assert get_failure(result).description.startswith(
        "Could not read the WebPODA token from the 'webpoda-token' Secrets Manager "
        "secret"
    )
    assert not fake_webpoda.calls


def test_data_gap_filler_without_bucket(dagster_instance, fake_webpoda, monkeypatch):
    """Test that the run fails before checking anything without a data bucket."""
    monkeypatch.delenv("S3_BUCKET")

    result = run_data_gap_filler(dagster_instance, instruments=["mag"])

    assert not result.success
    assert get_failure(result).description == (
        "The S3_BUCKET environment variable is not set."
    )
    assert not fake_webpoda.calls


def test_data_gap_filler_missing_bucket(
    dagster_instance, fake_webpoda, monkeypatch, s3_client
):
    """Test that the run fails if the data bucket doesn't exist.

    Otherwise nothing would ever look like it was uploaded.
    """
    monkeypatch.setattr(data_gap_filler, "S3_CLIENT", s3_client)
    monkeypatch.setenv("S3_BUCKET", "missing-bucket")

    result = run_data_gap_filler(dagster_instance, instruments=["mag"])

    assert not result.success
    assert get_failure(result).description.startswith(
        "Could not access the data bucket 'missing-bucket'"
    )
    assert not fake_webpoda.calls
