import os
import requests

from willisapi_client.willisapi_client import WillisapiClient
from willisapi_client.logging_setup import logger as logger


def _archive_headers(api_key):
    return {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"token {api_key}",
    }


def archive_metadata_csv(api_key, csv_path, total_rows, upload_type, env=None):
    """Archive the source metadata CSV alongside the data upload.

    Creates a server-side tracking row, then PUTs the raw CSV to the returned
    presigned S3 URL. Returns the tracking ``record_id`` on success (so the
    caller can finalize it with row counts), or ``None`` on failure.

    All failures are logged and swallowed — archiving must never abort the
    actual data upload.
    """
    try:
        wc = WillisapiClient(env=env)
        payload = {
            "filename": os.path.basename(csv_path),
            "total_rows": total_rows,
            "upload_type": upload_type,
        }
        res = requests.post(
            wc.get_csv_archive_url(), headers=_archive_headers(api_key), json=payload
        )
        if res.status_code != 201:
            logger.warning(f"CSV archive init failed: {res.status_code} {res.text}")
            return None

        body = res.json()
        record_id = body.get("record_id")
        presigned = body.get("presigned_url")
        if not presigned:
            logger.warning("CSV archive: no presigned URL returned")
            return None

        with open(csv_path, "rb") as f:
            put_res = requests.put(
                presigned, data=f, headers={"Content-Type": "text/csv"}
            )
        if put_res.status_code not in (200, 204):
            logger.warning(f"CSV archive S3 upload failed: {put_res.status_code}")
            finalize_metadata_csv(api_key, record_id, "failed", 0, 0, env=env)
            return None

        return record_id
    except Exception as ex:
        logger.warning(f"CSV archive error: {ex}")
        return None


def finalize_metadata_csv(
    api_key, record_id, status, successful_rows, failed_rows, env=None
):
    """Report the CSV archive outcome and parsed row counts to the server."""
    if not record_id:
        return
    try:
        wc = WillisapiClient(env=env)
        payload = {
            "status": status,
            "successful_rows": successful_rows,
            "failed_rows": failed_rows,
        }
        res = requests.patch(
            wc.get_csv_archive_finalize_url(record_id),
            headers=_archive_headers(api_key),
            json=payload,
        )
        if res.status_code != 200:
            logger.warning(f"CSV archive finalize failed: {res.status_code} {res.text}")
    except Exception as ex:
        logger.warning(f"CSV archive finalize error: {ex}")
