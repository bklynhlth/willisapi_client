import base64
import hashlib
import json
import math
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

from willisapi_client.logging_setup import logger as logger
from willisapi_client.services.metadata.archive import archive_metadata_csv
from willisapi_client.services.metadata.upload import (
    _put_file_to_s3,
    _put_part_to_s3,
)
from willisapi_client.services.metadata.utils import build_retry_session
from willisapi_client.timer import measure
from willisapi_client.willisapi_client import WillisapiClient, authorization_header

ACTIONS = {
    "c": "c",
    "create": "c",
    "u": "u",
    "update": "u",
    "u.ow": "u.ow",
    "update.overwrite": "u.ow",
    "u.dcr": "u.dcr",
    "update.dcr": "u.dcr",
    "rc": "rc",
    "reconcile": "rc",
}
# The API accepts these; u.dcr and rc are in the spec but not live yet.
SUPPORTED_ACTIONS = ("c", "u", "u.ow")

# A file of PART_SIZE bytes or more is uploaded in parts of that size, all
# presigned up front and sent in parallel; smaller files use one presigned URL.
# S3 allows 10,000 parts, so a file over PART_SIZE * 10,000 gets larger parts.
PART_SIZE = 50 * 1024**2
MAX_PARTS = 10_000
DEFAULT_WORKERS = 8

NOTE_EXTENSIONS = (".txt", ".pdf", ".jpeg", ".jpg", ".png")
RECORDING_EXTENSIONS = (".wav", ".mp3", ".mp4", ".mov")

# The spec's "Required fields by action" table, for the fields the API
# enforces per (subject, event, form) record. Dotted paths into the merged
# SubjectData/StudyEventData/FormData.
REQUIRED_ALWAYS = ("SubjectKey", "SiteRef.LocationOID")
REQUIRED_BY_ACTION = {
    "c": (
        "StudyEventOID",
        "InstanceName",
        "FormOID",
        "FormLanguage",
        "FormDateTime.ActualStart",
        "FormDateTime.ActualEnd",
        "Signature.UserRef.UserOID",
        "Signature.LocationRef.LocationOID",
        "Signature.DateTimeStamp",
        "ItemGroupData",
    ),
    "u": (),
    "u.ow": ("StudyEventOID",),
}


def _failed(message, errors=None):
    """A summary in the API's response shape, for failures before/around the call."""
    return {
        "status": "FAILED",
        "code": "400",
        "message": message,
        "records": [],
        "errors": errors or [],
    }


def _public(summary: dict) -> dict:
    """The summary as the caller sees it: the per-file detail is the API's and the
    client's business, and what it means for each record is in ``records``."""
    summary.pop("attachments", None)
    return summary


class COAUploadUtils:
    """Reading, checking and posting the ODM JSON of ``coa_upload``."""

    def __init__(self, data: dict):
        self.data = data

    @staticmethod
    def get(data, path):
        for key in path.split("."):
            if not isinstance(data, dict):
                return None
            data = data.get(key)
        return data

    def records(self):
        """Yield ``(subject, event, form)``; event/form are None when absent."""
        for clinical in self.data.get("ClinicalData") or []:
            for subject in clinical.get("SubjectData") or []:
                events = subject.get("StudyEventData") or []
                if not events:
                    yield subject, None, None
                for event in events:
                    forms = event.get("FormData") or []
                    if not forms:
                        yield subject, event, None
                    for form in forms:
                        yield subject, event, form

    def validate(self, action: str) -> list:
        """Every problem found, so they can be fixed in one go."""
        errors = []
        admin = self.data.get("AdminData")
        admin = admin if isinstance(admin, dict) else {}
        if not admin.get("StudyOID"):
            errors.append("AdminData.StudyOID is required.")
        locations = admin.get("Location")
        if not isinstance(locations, list) or not locations:
            errors.append("AdminData.Location is required.")
        else:
            for position, location in enumerate(locations):
                location = location if isinstance(location, dict) else {}
                missing = [k for k in ("OID", "Name", "Number") if not location.get(k)]
                if missing:
                    errors.append(
                        f"AdminData.Location[{position}] is missing: {', '.join(missing)}."
                    )
        if not isinstance(self.data.get("ClinicalData"), list):
            errors.append("ClinicalData must be a list.")
            return errors

        for subject, event, form in self.records():
            merged = {**subject, **(event or {}), **(form or {})}
            label = (
                f"SubjectKey {subject.get('SubjectKey')}"
                + (f" / {event.get('StudyEventOID')}" if event else "")
                + (f" / {form.get('FormOID')}" if form else "")
            )
            for path in REQUIRED_ALWAYS + REQUIRED_BY_ACTION[action]:
                if not self.get(merged, path):
                    errors.append(f"{label}: {path} is required for action '{action}'.")
            if action == "u" and form is not None:
                errors.append(f"{label}: FormData is not allowed for action 'u'.")
        return errors

    def attachment_paths(self) -> list:
        """``(ItemOID, path)`` for every note and recording named in the JSON."""
        found = []
        for _, _, form in self.records():
            for group in (form or {}).get("ItemGroupData") or []:
                if group.get("ItemGroupOID") != "IG.ATTACHMENTS":
                    continue
                for item in group.get("ItemData") or []:
                    oid = str(item.get("ItemOID", ""))
                    if oid.startswith(("IT.NOTES_PATH.", "IT.RECORDING_PATH.")):
                        found.append((oid, item.get("Value")))
        return found

    def attachment_paths_by_record(self) -> dict:
        """``{(record position, ItemOID): path}``, the way the API numbers its records.

        Matching an issued file back to its path by checksum would confuse two
        files with the same content; the API names each by record and ItemOID.
        """
        found = {}
        for position, (_, _, form) in enumerate(self.records()):
            for group in (form or {}).get("ItemGroupData") or []:
                if group.get("ItemGroupOID") != "IG.ATTACHMENTS":
                    continue
                for item in group.get("ItemData") or []:
                    found[(position, str(item.get("ItemOID", "")))] = item.get("Value")
        return found

    @staticmethod
    def check_attachment(oid: str, path) -> str:
        """An error message when the file can't be uploaded, else ''."""
        allowed = (
            NOTE_EXTENSIONS
            if oid.startswith("IT.NOTES_PATH.")
            else RECORDING_EXTENSIONS
        )
        if not path or not str(path).lower().endswith(allowed):
            return f"{oid}: file must be one of {', '.join(allowed)}."
        if not os.path.isfile(path):
            return f"{oid}: file not found: {path}"
        return ""

    @staticmethod
    def calculate_part_checksums(file_path: str, part_size: int) -> list:
        """Base64 SHA-256 of each ``part_size`` slice of the file, read in 1 MiB steps."""
        digests = []
        with open(file_path, "rb") as f:
            while True:
                digest, left = hashlib.sha256(), part_size
                while left:
                    chunk = f.read(min(left, 1024 * 1024))
                    if not chunk:
                        break
                    digest.update(chunk)
                    left -= len(chunk)
                if left == part_size:
                    break
                digests.append(digest.digest())
        return [base64.b64encode(d).decode("utf-8") for d in digests]

    @staticmethod
    def composite_checksum(part_checksums: list) -> str:
        """What S3 reports for the assembled file: SHA-256 of the part digests, plus ``-<parts>``."""
        raw = b"".join(base64.b64decode(c) for c in part_checksums)
        return base64.b64encode(hashlib.sha256(raw).digest()).decode() + (
            f"-{len(part_checksums)}"
        )

    @staticmethod
    def calculate_file_checksum(file_path: str) -> str:
        """Base64 SHA-256, the form the API signs presigned URLs with."""
        digest = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                digest.update(chunk)
        return base64.b64encode(digest.digest()).decode("utf-8")


@measure
def coa_upload(api_key: str, json_path: str, action: str = "c", **kwargs) -> dict:
    """Upload COA data (ODM JSON) and its notes/recordings to Willis.

    ``action``: ``c``/``create`` (new data), ``u``/``update`` (participant
    status, visit dates), ``u.ow``/``update.overwrite`` (replace existing data).
    Returns the API's summary: ``{status, code, message, records: [...]}`` plus
    ``session_id`` (the upload as a whole); each record has its own ``record_id``.
    A record with files is SUCCESS only once the API has confirmed them in S3; a
    COA whose files are missing is FAILED and must be sent again with ``u.ow``
    (not ``c``).
    """
    normalized = ACTIONS.get(str(action).strip().lower())
    if normalized is None or normalized not in SUPPORTED_ACTIONS:
        return _failed(
            f"Unsupported action '{action}'. Supported: c/create, u/update, "
            "u.ow/update.overwrite."
        )

    try:
        with open(json_path) as f:
            data = json.load(f)
    except (OSError, ValueError) as ex:
        return _failed(f"Could not read '{json_path}' as JSON: {ex}")
    if not isinstance(data, dict):
        return _failed("The JSON file must contain an object.")

    utils = COAUploadUtils(data)
    errors = utils.validate(normalized)
    paths = {}
    for oid, path in utils.attachment_paths():
        error = utils.check_attachment(oid, path)
        if error:
            errors.append(error)
        else:
            paths[path] = os.path.getsize(path)
    # One part size for the call, large enough for the biggest file.
    part_size = max(PART_SIZE, math.ceil(max(paths.values(), default=0) / MAX_PARTS))
    checksums, part_checksums = {}, {}
    if not errors:
        for path, size in paths.items():
            if size >= PART_SIZE:
                part_checksums[path] = utils.calculate_part_checksums(path, part_size)
                checksums[path] = utils.composite_checksum(part_checksums[path])
            else:
                checksums[path] = utils.calculate_file_checksum(path)
    if errors:
        logger.error(f'{datetime.now().strftime("%H:%M:%S")}: json check failed')
        return _failed("The JSON failed validation.", errors)
    logger.info(f'{datetime.now().strftime("%H:%M:%S")}: json check passed')

    env = kwargs.get("env")
    # The JSON goes to S3 first; the API reads it back from there.
    record_id = archive_metadata_csv(
        api_key,
        json_path,
        0,
        upload_type="coa_json",
        env=env,
        content_type="application/json",
    )
    if not record_id:
        return _failed("Could not upload the JSON to Willis.")

    wc = WillisapiClient(env=env)
    headers = wc.get_headers()
    headers["Authorization"] = authorization_header(api_key)
    try:
        response = build_retry_session().post(
            wc.get_coa_upload_url(),
            headers=headers,
            json={
                "action": normalized,
                "record_id": record_id,
                "checksums": checksums,
                **(
                    {"part_size": part_size, "part_checksums": part_checksums}
                    if part_checksums
                    else {}
                ),
            },
        )
        summary = response.json()
    except Exception as ex:
        return _failed(f"COA upload request failed: {ex}")
    if not isinstance(summary, dict) or response.status_code not in (
        200,
        202,
        207,
        400,
    ):
        return _failed(
            f"COA upload request failed with status {response.status_code}.",
            [response.text],
        )
    if "records" not in summary:
        # A request-level rejection ({"error": ...} or field errors).
        return _failed("COA upload was rejected.", [json.dumps(summary)])

    # Send each file to its presigned URL(s). A multipart file's part URLs are
    # all issued at once; every part and every small file goes through one pool.
    path_by_checksum = {checksum: path for path, checksum in checksums.items()}
    path_by_record = utils.attachment_paths_by_record()
    uploads = []  # (attachment, [upload, ...])
    for attachment in summary.get("attachments") or []:
        path = path_by_record.get(
            (attachment.get("record"), attachment.get("item_oid"))
        ) or path_by_checksum.get(attachment.get("checksum"))
        if path is None:
            continue
        attachment["file"] = path
        if attachment.get("multipart"):
            parts = attachment["multipart"]["parts"]
            digests = part_checksums[path]
            size = os.path.getsize(path)
            step = attachment["multipart"]["part_size"]
            stop = threading.Event()  # set by the first part that fails
            uploads.append(
                (
                    attachment,
                    [
                        (
                            _put_part_to_s3,
                            {
                                "number": part["number"],
                                "presigned": part["presigned"],
                                "checksum": digests[part["number"] - 1],
                                "recording": path,
                                "stop": stop,
                                "offset": (part["number"] - 1) * step,
                                "length": min(step, size - (part["number"] - 1) * step),
                            },
                        )
                        for part in parts
                    ],
                )
            )
        elif attachment.get("presigned"):
            uploads.append(
                (
                    attachment,
                    [
                        (
                            _put_file_to_s3,
                            {
                                "presigned": attachment["presigned"],
                                "checksum": attachment["checksum"],
                                "recording": path,
                            },
                        )
                    ],
                )
            )
    if uploads:
        tasks = [task for _, group in uploads for task in group]
        workers = min(len(tasks), kwargs.get("workers") or DEFAULT_WORKERS)
        with ThreadPoolExecutor(max_workers=workers) as executor:
            outcomes = list(executor.map(lambda t: t[0](t[1]), tasks))
        position = 0
        failed = 0
        for attachment, group in uploads:
            errors_found = [e for e in outcomes[position : position + len(group)] if e]
            # The part that failed, not the ones skipped because of it, first.
            errors_found.sort(key=lambda e: "skipped" in e)
            position += len(group)
            attachment["upload_status"] = "Failed" if errors_found else "Success"
            attachment["error"] = "; ".join(errors_found[:3]) or None
            attachment.pop("presigned", None)
            attachment.pop("multipart", None)
            failed += bool(errors_found)
        if failed:
            logger.error(f"{failed} file(s) failed to upload")
            for attachment, _ in uploads:
                if attachment.get("error"):
                    logger.error(f"{attachment['file']}: {attachment['error']}")

    if any(r.get("status") == "PENDING" for r in summary["records"]):
        # A record is SUCCESS only once its files are confirmed in S3. Ask the
        # API to check them, including after a failed PUT, so it marks the COA
        # incomplete (to be sent again with u.ow) instead of leaving it pending.
        try:
            confirmed = build_retry_session().patch(
                wc.get_coa_upload_confirm_url(record_id), headers=headers
            )
            result = confirmed.json()
        except Exception as ex:
            summary["message"] = f"Could not confirm the uploaded files: {ex}"
            return _public(summary)
        if confirmed.status_code not in (200, 207, 400) or "records" not in result:
            summary["message"] = (
                f"Could not confirm the uploaded files (status {confirmed.status_code})."
            )
            summary["errors"] = [confirmed.text]
            return _public(summary)
        for key in ("status", "code", "message", "records"):
            summary[key] = result[key]
    return _public(summary)
