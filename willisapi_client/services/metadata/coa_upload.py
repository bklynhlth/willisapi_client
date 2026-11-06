import base64
import hashlib
import json
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

from willisapi_client.logging_setup import logger as logger
from willisapi_client.services.metadata.archive import archive_metadata_csv
from willisapi_client.services.metadata.upload import _put_file_to_s3
from willisapi_client.services.metadata.utils import build_retry_session
from willisapi_client.timer import measure
from willisapi_client.willisapi_client import WillisapiClient

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
    ``attachments`` with each file's upload outcome.
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
    checksums = {}
    for oid, path in utils.attachment_paths():
        error = utils.check_attachment(oid, path)
        if error:
            errors.append(error)
        elif path not in checksums:
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
    headers["Authorization"] = f"token {api_key}"
    try:
        response = build_retry_session().post(
            wc.get_coa_upload_url(),
            headers=headers,
            json={"action": normalized, "record_id": record_id, "checksums": checksums},
        )
        summary = response.json()
    except Exception as ex:
        return _failed(f"COA upload request failed: {ex}")
    if not isinstance(summary, dict) or response.status_code not in (200, 207, 400):
        return _failed(
            f"COA upload request failed with status {response.status_code}.",
            [response.text],
        )
    if "records" not in summary:
        # A request-level rejection ({"error": ...} or field errors).
        return _failed("COA upload was rejected.", [json.dumps(summary)])

    # Send each file to its presigned URL. They share one short expiry window,
    # so upload them concurrently.
    path_by_checksum = {checksum: path for path, checksum in checksums.items()}
    pending = [
        (
            attachment,
            {
                "presigned": attachment["presigned"],
                "checksum": attachment["checksum"],
                "recording": path_by_checksum[attachment["checksum"]],
            },
        )
        for attachment in summary.get("attachments") or []
        if attachment.get("presigned")
        and attachment.get("checksum") in path_by_checksum
    ]
    if pending:
        with ThreadPoolExecutor(
            max_workers=min(len(pending), os.cpu_count() or 1)
        ) as executor:
            outcomes = list(executor.map(_put_file_to_s3, [f for _, f in pending]))
        for (attachment, file), error in zip(pending, outcomes):
            attachment["upload_status"] = "Failed" if error else "Success"
            attachment["error"] = error
            attachment["file"] = file["recording"]
            attachment.pop("presigned", None)
        failed = [e for e in outcomes if e]
        if failed:
            summary["message"] = (
                f"{summary.get('message', '')} {len(failed)} file(s) failed to upload."
            ).strip()
    return summary
