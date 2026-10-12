"""Fill L0 data gaps by re-querying WebPODA by Spacecraft Time (SCT).

The packet downloader lambda turns the packets WebPODA received recently (by
Earth Received Time) into L0 files, so packets that reach WebPODA late can be
missing from L0 files that already exist. This asset uses the
``imap_data_access.webpoda`` data gap checker to re-query WebPODA by Spacecraft
Time (SCT) for every day (or pointing, for the ENA instruments) in a date range.
It compares the result against the latest L0 file and only uploads it, as a new
L0 minor version, if the data changed or no L0 file exists yet. This is the
same as running
``imap-data-access webpoda --query-mode sct --upload-to-sdc ...`` for each
instrument.

``webpoda_data_gap_filler_sensor`` requests a run once a week that checks a
month of data ending a week ago (see ``config.DATA_GAP_FILLER_*``). Each run
logs, and records in its materialization metadata, every new L0 file it
uploaded.
"""

import contextlib
import csv
import datetime
import logging
import os
import tempfile
from dataclasses import dataclass, field
from pathlib import Path

import boto3
import imap_data_access
from botocore.exceptions import BotoCoreError, ClientError
from dagster import (
    AssetExecutionContext,
    Config,
    Failure,
    MaterializeResult,
    MetadataValue,
    RunConfig,
    RunRequest,
    SensorEvaluationContext,
    asset,
    sensor,
)
from imap_data_access import webpoda
from imap_data_access.file_validation import generate_imap_file_path

# Dagster requires the asset's run config parameter to be named "config"
from sds_data_manager.orchestration import config as orchestration_config

S3_CLIENT = boto3.client("s3", region_name="us-west-2")
SECRETS_MANAGER_CLIENT = boto3.client("secretsmanager", region_name="us-west-2")
SSM_CLIENT = boto3.client("ssm", region_name="us-west-2")

ASSET_NAME = "webpoda_data_gap_filler"
# The same credentials that the packet downloader lambda uses
WEBPODA_TOKEN_SECRET_ID = "webpoda-token"  # noqa: S105 (the secret's name)
DEFAULT_SSM_API_KEY_PARAMETER = "/imap-sdc/batch-jobs/api-key"
DATE_FORMAT = "%Y%m%d"
# imap_data_access.webpoda functions that log a line for every APID they query.
# These are not forwarded to the Dagster logs, so the useful lines stay readable.
_PER_APID_LOG_FUNCTIONS = frozenset(
    {"get_packet_binary_data_sctime", "get_packet_times_ert"}
)
_S3_NOT_FOUND_CODES = frozenset({"404", "NoSuchKey", "NotFound"})


class DataGapFillerConfig(Config):
    """Run config of the data gap filler. Every field is optional.

    Attributes
    ----------
    start_date : str, optional
        The first spacecraft (SCT) date to check, YYYYMMDD. Defaults to
        ``config.DATA_GAP_FILLER_WINDOW`` before ``end_date``.
    end_date : str, optional
        The last spacecraft (SCT) date to check (inclusive), YYYYMMDD. Defaults
        to ``config.DATA_GAP_FILLER_DELAY`` before today (UTC).
    instruments : list of str, optional
        The instruments to check. Defaults to every instrument in
        ``webpoda.INSTRUMENT_APIDS``, like the packet downloader lambda.
    """

    start_date: str | None = None
    end_date: str | None = None
    instruments: list[str] | None = None


@dataclass
class UploadedFile:
    """An L0 file that the data gap filler uploaded."""

    name: str
    size: int
    # The latest L0 file before this upload, if there was one
    replaced_name: str | None = None
    replaced_size: int | None = None

    def describe(self) -> str:
        """Describe the upload in one line."""
        description = f"{self.name} ({webpoda.format_size(self.size)})"
        if self.replaced_name is None:
            return f"{description}, no previous L0 file"
        return (
            f"{description}, replacing {self.replaced_name} "
            f"({webpoda.format_size(self.replaced_size)})"
        )


@dataclass
class InstrumentResult:
    """The outcome of checking one instrument for data gaps."""

    instrument: str
    uploaded_files: list[UploadedFile] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


def get_default_window(
    today: datetime.date | None = None,
) -> tuple[datetime.date, datetime.date]:
    """Get the first and last spacecraft dates (inclusive) to check by default.

    Parameters
    ----------
    today : datetime.date, optional
        The current date. Defaults to today (UTC).

    Returns
    -------
    tuple[datetime.date, datetime.date]
        The ``config.DATA_GAP_FILLER_WINDOW`` of dates that ends
        ``config.DATA_GAP_FILLER_DELAY`` before today. With the default settings
        that is 2026-09-05 to 2026-10-05 on 2026-10-12.
    """
    if today is None:
        today = datetime.datetime.now(datetime.timezone.utc).date()
    end_date = today - orchestration_config.DATA_GAP_FILLER_DELAY
    return end_date - orchestration_config.DATA_GAP_FILLER_WINDOW, end_date


def _parse_date(value: str, name: str) -> datetime.date:
    """Parse a YYYYMMDD date from the run config."""
    try:
        return datetime.datetime.strptime(value, DATE_FORMAT).date()
    except ValueError as e:
        raise Failure(
            description=f"Invalid {name} '{value}', expected YYYYMMDD."
        ) from e


def resolve_window(
    run_config: DataGapFillerConfig,
) -> tuple[datetime.date, datetime.date]:
    """Get the first and last spacecraft dates (inclusive) a run checks."""
    _, default_end_date = get_default_window()
    end_date = (
        _parse_date(run_config.end_date, "end_date")
        if run_config.end_date
        else default_end_date
    )
    start_date = (
        _parse_date(run_config.start_date, "start_date")
        if run_config.start_date
        else end_date - orchestration_config.DATA_GAP_FILLER_WINDOW
    )
    if start_date > end_date:
        raise Failure(
            description=f"start_date {start_date} is after end_date {end_date}."
        )
    return start_date, end_date


def resolve_instruments(instruments: list[str] | None) -> list[str]:
    """Get the sorted instruments a run checks, validating any requested ones."""
    valid_instruments = sorted(webpoda.INSTRUMENT_APIDS)
    if not instruments:
        return valid_instruments
    invalid_instruments = sorted(set(instruments) - set(valid_instruments))
    if invalid_instruments:
        raise Failure(
            description=f"Unknown instrument(s) {invalid_instruments}. Valid "
            f"instruments are {valid_instruments}."
        )
    return sorted(set(instruments))


def _get_credentials() -> tuple[str, str]:
    """Get the WebPODA token and the IMAP API key that uploads need."""
    try:
        response = SECRETS_MANAGER_CLIENT.get_secret_value(
            SecretId=WEBPODA_TOKEN_SECRET_ID
        )
        webpoda_token = response["SecretString"]
    except (BotoCoreError, ClientError, KeyError) as e:
        raise Failure(
            description="Could not read the WebPODA token from the "
            f"'{WEBPODA_TOKEN_SECRET_ID}' Secrets Manager secret: {e!r}"
        ) from e

    parameter_name = os.environ.get(
        "SSM_API_KEY_PARAMETER", DEFAULT_SSM_API_KEY_PARAMETER
    )
    try:
        response = SSM_CLIENT.get_parameter(Name=parameter_name, WithDecryption=True)
        api_key = response["Parameter"]["Value"]
    except (BotoCoreError, ClientError, KeyError) as e:
        raise Failure(
            description="Could not read the IMAP API key from the "
            f"'{parameter_name}' SSM parameter: {e!r}"
        ) from e
    return webpoda_token, api_key


@contextlib.contextmanager
def _imap_data_access_config(**overrides):
    """Temporarily override values in the global ``imap_data_access.config``."""
    original = {key: imap_data_access.config.get(key) for key in overrides}
    imap_data_access.config.update(overrides)
    try:
        yield
    finally:
        imap_data_access.config.update(original)


class _DagsterLogForwarder(logging.Handler):
    """Forward ``imap_data_access`` log records to the Dagster run logs."""

    def __init__(self, context: AssetExecutionContext, prefix: str):
        super().__init__(level=logging.INFO)
        self.context = context
        self.prefix = prefix
        # Errors that the library logs instead of raising, e.g. a failed upload
        self.errors: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        """Forward a record, except per-APID query and "-----" separator lines."""
        try:
            message = record.getMessage()
            if record.levelno >= logging.ERROR:
                self.errors.append(message)
            elif record.funcName in _PER_APID_LOG_FUNCTIONS or not message.strip("- "):
                return
            self.context.log.log(record.levelno, f"{self.prefix} {message}")
        except Exception:
            self.handleError(record)


@contextlib.contextmanager
def _forward_library_logs(context: AssetExecutionContext, prefix: str):
    """Forward ``imap_data_access`` INFO and higher logs to the Dagster run logs."""
    library_logger = logging.getLogger("imap_data_access")
    original_level = library_logger.level
    handler = _DagsterLogForwarder(context, prefix)
    library_logger.addHandler(handler)
    library_logger.setLevel(logging.INFO)
    try:
        yield handler
    finally:
        library_logger.removeHandler(handler)
        library_logger.setLevel(original_level)


def _get_repoint_data(context: AssetExecutionContext) -> list[dict]:
    """Download the latest repoint table, which defines the ENA pointings."""
    with (
        tempfile.TemporaryDirectory(prefix="repoint_") as data_dir,
        _imap_data_access_config(DATA_DIR=Path(data_dir)),
        _forward_library_logs(context, "[repoint]"),
    ):
        repoint_file = webpoda.get_repoint_file()
        if repoint_file is None:
            raise ValueError("No repoint table was ingested in the last two weeks.")
        with open(repoint_file, newline="") as f:
            repoint_data = list(csv.DictReader(f))
    context.log.info(
        f"Using repoint table {repoint_file.name} ({len(repoint_data)} repointings)"
    )
    return repoint_data


def find_uploaded_files(
    data_dir: Path, bucket: str, started_at: datetime.datetime
) -> list[UploadedFile]:
    """Find the L0 files in ``data_dir`` that were uploaded since ``started_at``.

    ``imap_data_access.webpoda`` doesn't report what it uploaded, and it leaves
    the production files it downloads for comparison, and the new files that
    turn out to be duplicates, in its data directory too. So a file only counts
    as uploaded if it is now in the data bucket and was written there after
    ``started_at``.

    Parameters
    ----------
    data_dir : pathlib.Path
        The data directory of the check, which must be the current
        ``imap_data_access.config["DATA_DIR"]``.
    bucket : str
        The SDC data bucket that files are uploaded to.
    started_at : datetime.datetime
        When the check started (timezone aware).

    Returns
    -------
    list[UploadedFile]
        The uploaded files, sorted by name.
    """
    files = sorted(path for path in data_dir.rglob("*") if path.is_file())
    uploaded_files = []
    for path in files:
        try:
            # The upload API builds the S3 key the same way
            s3_key = (
                generate_imap_file_path(path.name)
                .construct_path()
                .relative_to(data_dir)
                .as_posix()
            )
        except ValueError:
            # Not an IMAP file, so it wasn't uploaded
            continue
        try:
            response = S3_CLIENT.head_object(Bucket=bucket, Key=s3_key)
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") in _S3_NOT_FOUND_CODES:
                # A new file that matched production, or a failed upload
                continue
            raise
        if response["LastModified"] < started_at:
            # A production file that was downloaded to compare against
            continue

        # Find the production file that this is a new version of, if any.
        # L0 file names are "{logical name}_{version}.pkts".
        logical_name = path.name.rsplit("_", 1)[0]
        replaced = max(
            (
                other
                for other in files
                if other != path and other.name.rsplit("_", 1)[0] == logical_name
            ),
            key=lambda other: other.name,
            default=None,
        )
        uploaded_files.append(
            UploadedFile(
                name=path.name,
                size=path.stat().st_size,
                replaced_name=replaced.name if replaced else None,
                replaced_size=replaced.stat().st_size if replaced else None,
            )
        )
    return uploaded_files


def _check_instrument(
    context: AssetExecutionContext,
    instrument: str,
    start_time: datetime.datetime,
    end_time: datetime.datetime,
    repoint_data: list[dict] | None,
    bucket: str,
) -> InstrumentResult:
    """Check one instrument for data gaps, uploading new or changed L0 files."""
    prefix = f"[{instrument}]"
    result = InstrumentResult(instrument)
    context.log.info(
        f"{prefix} Checking spacecraft time {start_time} to {end_time} for new data"
    )
    # S3 LastModified times have a resolution of one second
    started_at = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0)
    # Use a new data directory for each instrument to limit the disk space used
    with (
        tempfile.TemporaryDirectory(prefix=f"{instrument}_") as data_dir,
        _imap_data_access_config(DATA_DIR=Path(data_dir)),
        _forward_library_logs(context, prefix) as library_logs,
    ):
        try:
            if instrument in imap_data_access.REPOINT_DEPENDENT_INSTRUMENTS:
                webpoda.download_repointing_data(
                    instrument=instrument,
                    start_time=start_time,
                    end_time=end_time,
                    repoint_data=repoint_data,
                    upload_to_sdc=True,
                    query_by_ert=False,
                )
            else:
                webpoda.download_daily_data(
                    instrument=instrument,
                    start_time=start_time,
                    end_time=end_time,
                    upload_to_sdc=True,
                    query_by_ert=False,
                )
        except Exception as e:
            context.log.exception(f"{prefix} Data gap check failed: {e!r}")
            result.errors.append(f"{type(e).__name__}: {e}")
        result.errors.extend(library_logs.errors)

        # Also report anything that was uploaded before a failure
        try:
            result.uploaded_files = find_uploaded_files(
                Path(data_dir), bucket, started_at
            )
        except (BotoCoreError, ClientError) as e:
            context.log.exception(f"{prefix} Could not check what was uploaded: {e!r}")
            result.errors.append(f"Could not check what was uploaded: {e}")

    for uploaded_file in result.uploaded_files:
        context.log.warning(
            f"{prefix} NEW DATA FOUND, uploaded {uploaded_file.describe()}"
        )
    if result.errors:
        context.log.error(f"{prefix} Data gap check finished with errors")
    elif not result.uploaded_files:
        context.log.info(f"{prefix} No new data found")
    return result


def _summarize(
    context: AssetExecutionContext,
    start_date: datetime.date,
    end_date: datetime.date,
    results: list[InstrumentResult],
) -> dict:
    """Log a summary of the run and return the materialization metadata."""
    new_data = [result for result in results if result.uploaded_files]
    failed = [result for result in results if result.errors]
    no_new_data = [
        result.instrument
        for result in results
        if not result.uploaded_files and not result.errors
    ]
    num_uploaded_files = sum(len(result.uploaded_files) for result in new_data)

    lines = [
        "WebPODA data gap filler summary for spacecraft dates "
        f"{start_date} to {end_date}:"
    ]
    if new_data:
        lines.append(
            f"NEW DATA FOUND, uploaded {num_uploaded_files} new L0 file(s) for "
            f"{len(new_data)} instrument(s):"
        )
        lines.extend(
            f"  {result.instrument}: {uploaded_file.describe()}"
            for result in new_data
            for uploaded_file in result.uploaded_files
        )
    else:
        lines.append("No new data found.")
    lines.append(f"No new data: {', '.join(no_new_data) or 'none'}")
    if failed:
        lines.append("ERRORS, so some data may not have been checked or uploaded:")
        lines.extend(
            f"  {result.instrument}: {error}"
            for result in failed
            for error in result.errors
        )
    level = logging.INFO
    if failed:
        level = logging.ERROR
    elif new_data:
        level = logging.WARNING
    context.log.log(level, "\n".join(lines))

    metadata = {
        "start_date": start_date.strftime(DATE_FORMAT),
        "end_date": end_date.strftime(DATE_FORMAT),
        "query_mode": "sct",
        "instruments_checked": MetadataValue.json(
            [result.instrument for result in results]
        ),
        "new_data_found": bool(new_data),
        "num_uploaded_files": num_uploaded_files,
        "uploaded_files": MetadataValue.json(
            {
                result.instrument: [f.name for f in result.uploaded_files]
                for result in new_data
            }
        ),
    }
    if failed:
        metadata["errors"] = MetadataValue.json(
            {result.instrument: result.errors for result in failed}
        )
    return metadata


@asset(
    name=ASSET_NAME,
    description="Re-queries WebPODA by spacecraft time (SCT) and uploads a new L0 "
    "minor version wherever the data differs from the latest L0 file.",
)
def webpoda_data_gap_filler(
    context: AssetExecutionContext, config: DataGapFillerConfig
) -> MaterializeResult:
    """Check a range of spacecraft dates for data missing from the L0 files.

    Every instrument is checked even if another one fails. The run fails at the
    end if any instrument could not be checked completely.
    """
    start_date, end_date = resolve_window(config)
    instruments = resolve_instruments(config.instruments)
    bucket = os.environ.get("S3_BUCKET")
    if not bucket:
        raise Failure(description="The S3_BUCKET environment variable is not set.")
    try:
        # Uploads are found by looking for them in the bucket. A missing bucket
        # would look like nothing was uploaded, because HEAD requests only
        # return a 404 for both.
        S3_CLIENT.head_bucket(Bucket=bucket)
    except (BotoCoreError, ClientError) as e:
        raise Failure(
            description=f"Could not access the data bucket '{bucket}': {e!r}"
        ) from e
    webpoda_token, api_key = _get_credentials()

    # Naive datetimes, because the library compares them to naive pointing times
    start_time = datetime.datetime.combine(start_date, datetime.time.min)
    end_time = datetime.datetime.combine(end_date, datetime.time.max)
    context.log.info(
        f"Checking WebPODA spacecraft time (SCT) data from {start_date} to "
        f"{end_date} against the latest L0 files on "
        f"{imap_data_access.config['DATA_ACCESS_URL']} for: {', '.join(instruments)}"
    )

    results = []
    with _imap_data_access_config(WEBPODA_TOKEN=webpoda_token, API_KEY=api_key):
        repoint_data = None
        repoint_error = None
        if set(instruments) & set(imap_data_access.REPOINT_DEPENDENT_INSTRUMENTS):
            try:
                repoint_data = _get_repoint_data(context)
            except Exception as e:
                context.log.exception(f"Could not get the repoint table: {e!r}")
                repoint_error = f"Could not get the repoint table: {e}"

        for instrument in instruments:
            if (
                repoint_error
                and instrument in imap_data_access.REPOINT_DEPENDENT_INSTRUMENTS
            ):
                context.log.error(f"[{instrument}] Skipped, {repoint_error}")
                results.append(InstrumentResult(instrument, errors=[repoint_error]))
                continue
            results.append(
                _check_instrument(
                    context, instrument, start_time, end_time, repoint_data, bucket
                )
            )

    metadata = _summarize(context, start_date, end_date, results)
    failed = [result.instrument for result in results if result.errors]
    if failed:
        raise Failure(
            description=f"The data gap check failed for: {', '.join(failed)}. Any "
            "new data that was found is still listed in the metadata and the logs.",
            metadata=metadata,
        )
    return MaterializeResult(metadata=metadata)


@sensor(
    name=f"{ASSET_NAME}_sensor",
    asset_selection=[webpoda_data_gap_filler],
    minimum_interval_seconds=int(
        orchestration_config.DATA_GAP_FILLER_INTERVAL.total_seconds()
    ),
    description="Requests a WebPODA data gap filler run once a week.",
)
def webpoda_data_gap_filler_sensor(context: SensorEvaluationContext) -> RunRequest:
    """Request a data gap filler run over the default window once a week.

    The run key includes the window, so evaluating the sensor again on the same
    day doesn't request a duplicate run.
    """
    start_date, end_date = get_default_window()
    start, end = start_date.strftime(DATE_FORMAT), end_date.strftime(DATE_FORMAT)
    context.log.info(
        f"Requesting a WebPODA data gap filler run for spacecraft dates {start} to "
        f"{end}"
    )
    return RunRequest(
        run_key=f"{ASSET_NAME}_{start}_{end}",
        run_config=RunConfig(
            ops={ASSET_NAME: DataGapFillerConfig(start_date=start, end_date=end)}
        ),
    )


assets = [webpoda_data_gap_filler]
sensors = [webpoda_data_gap_filler_sensor]
