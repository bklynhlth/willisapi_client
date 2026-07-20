import pandas as pd
from tqdm import tqdm
from datetime import datetime
import requests
import mimetypes
import os
from concurrent.futures import ThreadPoolExecutor

# Register extensions that Python's default mimetypes table doesn't know about.
# The server signs presigned URLs with these types, so the client must match.
mimetypes.add_type("application/vnd.apache.parquet", ".parquet")

from willisapi_client.timer import measure
from willisapi_client.willisapi_client import WillisapiClient
from willisapi_client.logging_setup import logger as logger
from willisapi_client.services.metadata.utils import (
    MetadataValidation,
    ProcessedMetadataValidation,
    UploadUtils,
    find_files_with_pattern,
    get_last_n_directories,
)
from willisapi_client.services.metadata.archive import (
    archive_metadata_csv,
    finalize_metadata_csv,
)

VALID_SCORE_TYPES = ["rater", "reviewer"]


def _put_file_to_s3(file_presigned: dict):
    """Upload a single file to its presigned S3 URL.

    Returns an error string if the upload fails, otherwise None. Presigned
    URLs from a single recording are all signed at once and share one expiry
    window, so callers should run these concurrently to fit within it.
    """
    presigned = file_presigned.get("presigned")
    checksum = file_presigned.get("checksum")
    recording = file_presigned.get("recording")
    try:
        content_type, _ = mimetypes.guess_type(recording)
        if not content_type:
            content_type = "text/csv"
        with open(recording, "rb") as f:
            response = requests.put(
                presigned,
                data=f,
                headers={
                    "x-amz-checksum-sha256": checksum,
                    "x-amz-sdk-checksum-algorithm": "SHA256",
                    "Content-Type": content_type,
                },
            )
        if response.status_code != 200:
            return (
                f"S3 upload failed with status code {response.status_code} "
                f"for file {recording}: {response.text}"
            )
    except Exception as ex:
        return str(ex)
    return None


@measure
def upload(api_key: str, csv_path: str, **kwargs):

    force_upload = kwargs.get("force_upload", False)
    csv = MetadataValidation(csv_path=csv_path, force_upload=force_upload)
    if csv.load_and_validate():
        logger.info(f'{datetime.now().strftime("%H:%M:%S")}: csv check passed')
        csv.create_final_csv()

        wc = WillisapiClient(env=kwargs.get("env"))
        url = wc.get_upload_url()
        headers = wc.get_headers()
        headers["Authorization"] = f"token {api_key}"
        logger.info(f'{datetime.now().strftime("%H:%M:%S")}: beginning upload')

        # Archive the source metadata CSV and open a tracking record.
        archive_record_id = archive_metadata_csv(
            api_key,
            csv_path,
            int(csv.transformed_df.shape[0]),
            upload_type="data",
            env=kwargs.get("env"),
        )

        results = []
        for index, row in tqdm(
            csv.transformed_df.iterrows(), total=csv.transformed_df.shape[0]
        ):
            u = UploadUtils(row)
            valid, err = u.validate_row()
            result_row = row.to_dict()
            if valid:
                payload = u.generate_payload()
                res = u.post(api_key, url, headers, payload)
                if res.get("upload_status") == "Success":
                    result_row["upload_status"] = "Success"
                    result_row["error"] = None

                    # Handle S3 upload if presigned URL is provided
                    presigned = res.get("response", {}).get("presigned")
                    if presigned:
                        try:
                            content_type, _ = mimetypes.guess_type(
                                payload.get("filename")
                            )
                            if not content_type:
                                content_type = "application/octet-stream"
                            with open(row.file_path, "rb") as f:
                                response = requests.put(
                                    presigned,
                                    data=f,
                                    headers={
                                        "x-amz-checksum-sha256": payload.get(
                                            "checksum"
                                        ),
                                        "x-amz-sdk-checksum-algorithm": "SHA256",
                                        "Content-Type": content_type,
                                    },
                                )
                            if response.status_code == 200:
                                result_row["upload_status"] = "Success"
                            else:
                                result_row["upload_status"] = "Failed"
                                result_row["error"] = (
                                    f"S3 upload failed with status code {response.status_code}: {response.text}"
                                )
                        except Exception as ex:
                            result_row["upload_status"] = "Failed"
                            result_row["error"] = str(ex)
                    else:
                        result_row["upload_status"] = "Failed"
                        result_row["error"] = (
                            "Collect recording upload URL not received"
                        )
                else:
                    result_row["upload_status"] = "Failed"
                    result_row["error"] = res.get("error")
            else:
                result_row["upload_status"] = "Failed"
                result_row["error"] = f"{err}"
            results.append(result_row)

        successful_rows = sum(1 for r in results if r.get("upload_status") == "Success")
        finalize_metadata_csv(
            api_key,
            archive_record_id,
            "successful",
            successful_rows,
            len(results) - successful_rows,
            env=kwargs.get("env"),
        )

        results_df = pd.DataFrame(results)
        return results_df
    else:
        logger.error(f'{datetime.now().strftime("%H:%M:%S")}: csv check failed')
        logger.error(csv.errors)
        return None


@measure
def processed_upload(api_key: str, csv_path: str, output_path: str, **kwargs):

    score_type = kwargs.get("score_type", "rater")
    if score_type not in VALID_SCORE_TYPES:
        logger.error(
            f"Invalid score_type '{score_type}'. Allowed values: {', '.join(VALID_SCORE_TYPES)}"
        )
        return None

    force_upload = kwargs.get("force_upload", False)
    csv = ProcessedMetadataValidation(
        csv_path=csv_path, force_upload=force_upload, score_type=score_type
    )
    if csv.load_and_validate():
        logger.info(f'{datetime.now().strftime("%H:%M:%S")}: csv check passed')
        csv.create_final_csv()

        wc = WillisapiClient(env=kwargs.get("env"))
        url = wc.get_processed_upload_url()
        headers = wc.get_headers()
        headers["Authorization"] = f"token {api_key}"
        logger.info(f'{datetime.now().strftime("%H:%M:%S")}: beginning upload')

        # Archive the source processed-data metadata CSV and open a tracking record.
        archive_record_id = archive_metadata_csv(
            api_key,
            csv_path,
            int(csv.transformed_df.shape[0]),
            upload_type="processed_data",
            env=kwargs.get("env"),
        )

        results = []
        for index, row in tqdm(
            csv.transformed_df.iterrows(), total=csv.transformed_df.shape[0]
        ):
            u = UploadUtils(row)
            valid, err = u.validate_processed_data_row()
            result_row = row.to_dict()
            if valid:
                files = []
                if score_type != "reviewer":
                    recording_val = getattr(row, "recording", None)
                    filename = (
                        os.path.basename(recording_val).split(".")[0]
                        if recording_val
                        else None
                    )
                    for file in (
                        find_files_with_pattern(output_path, filename)
                        if filename
                        else []
                    ):
                        key, error = get_last_n_directories(file, n=2)
                        if error:
                            continue
                        checksum = u.calculate_file_checksum(file)
                        files.append(
                            {
                                "index": index,
                                "recording": file,
                                "key": key,
                                "checksum": checksum,
                            }
                        )
                payload = u.generate_processed_payload(files, score_type=score_type)
                res = u.post(api_key, url, headers, payload)
                if res.get("upload_status") == "Success":
                    result_row["upload_status"] = "Success"
                    result_row["error"] = None

                    # Upload every file for this recording concurrently. All
                    # presigned URLs in this response share one short expiry
                    # window, so uploading them sequentially risks the later
                    # files expiring (403). A thread pool sized to the CPU
                    # count keeps them within the window (I/O-bound work).
                    files_to_upload = res.get("response", [])
                    s3_errors = []
                    if files_to_upload:
                        max_workers = min(len(files_to_upload), os.cpu_count() or 1)
                        with ThreadPoolExecutor(max_workers=max_workers) as executor:
                            for error in executor.map(
                                _put_file_to_s3, files_to_upload
                            ):
                                if error:
                                    s3_errors.append(error)
                    if s3_errors:
                        result_row["upload_status"] = "Failed"
                        result_row["error"] = "\n".join(s3_errors)
                else:
                    result_row["upload_status"] = "Failed"
                    result_row["error"] = res.get("error")
            else:
                result_row["upload_status"] = "Failed"
                result_row["error"] = f"{err}"
            results.append(result_row)

        successful_rows = sum(1 for r in results if r.get("upload_status") == "Success")
        finalize_metadata_csv(
            api_key,
            archive_record_id,
            "successful",
            successful_rows,
            len(results) - successful_rows,
            env=kwargs.get("env"),
        )

        results_df = pd.DataFrame(results)
        return results_df
    else:
        logger.error(f'{datetime.now().strftime("%H:%M:%S")}: csv check failed')
        logger.error(csv.errors)
        return None
