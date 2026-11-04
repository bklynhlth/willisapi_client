import json
import sys
from unittest.mock import MagicMock, patch

from willisapi_client.services.metadata.coa_upload import coa_upload

# The package re-exports the function ``coa_upload``, which shadows this module
# as an attribute; Python 3.9's ``patch("pkg.mod.name")`` resolves by getattr and
# would land on the function, so patch the module object itself.
MODULE = sys.modules["willisapi_client.services.metadata.coa_upload"]


def odm(form=True, attachments=()):
    form_data = {
        "FormOID": "F.MADRS",
        "FormLanguage": "en-US",
        "FormDateTime": {"ActualStart": "2026-09-08T09:15:00Z", "ActualEnd": "x"},
        "Signature": {
            "UserRef": {"UserOID": "jane@clinic.com"},
            "LocationRef": {"LocationOID": "loc"},
            "DateTimeStamp": "2026-09-08T10:15:05Z",
        },
        "ItemGroupData": [
            {
                "ItemGroupOID": "IG.ITEMS",
                "ItemData": [{"ItemOID": "IT.01.A", "Value": "3"}],
            },
            {
                "ItemGroupOID": "IG.ATTACHMENTS",
                "ItemData": [{"ItemOID": o, "Value": v} for o, v in attachments],
            },
        ],
    }
    return {
        "AdminData": {
            "StudyOID": "ACM001",
            "Location": [{"OID": "loc", "Name": "Acme", "Number": "101"}],
        },
        "ClinicalData": [
            {
                "SubjectData": [
                    {
                        "SubjectKey": "001",
                        "SiteRef": {"LocationOID": "loc"},
                        "StudyEventData": [
                            {
                                "StudyEventOID": "SE.SCREENING",
                                "InstanceName": "screening",
                                "FormData": [form_data] if form else [],
                            }
                        ],
                    }
                ]
            }
        ],
    }


def write(tmp_path, data):
    path = tmp_path / "coa.json"
    path.write_text(json.dumps(data))
    return str(path)


class TestCOAUpload:
    def test_unsupported_action(self, tmp_path):
        res = coa_upload("key", write(tmp_path, odm()), action="rc")
        assert res["status"] == "FAILED"
        assert "Unsupported action" in res["message"]

    def test_unreadable_json(self, tmp_path):
        res = coa_upload("key", str(tmp_path / "missing.json"))
        assert res["status"] == "FAILED"

    def test_all_validation_errors_are_returned_together(self, tmp_path):
        data = odm()
        data["AdminData"]["Location"] = [{"OID": "loc"}]
        form = data["ClinicalData"][0]["SubjectData"][0]["StudyEventData"][0][
            "FormData"
        ][0]
        form.pop("FormLanguage")
        form.pop("Signature")
        res = coa_upload("key", write(tmp_path, data))
        assert res["status"] == "FAILED"
        joined = " ".join(res["errors"])
        assert "Location[0] is missing: Name, Number" in joined
        assert "FormLanguage is required" in joined
        assert "Signature.UserRef.UserOID is required" in joined

    def test_update_rejects_form_data(self, tmp_path):
        res = coa_upload("key", write(tmp_path, odm()), action="u")
        assert any("FormData is not allowed" in e for e in res["errors"])

    def test_missing_attachment_file_fails_before_any_request(self, tmp_path):
        data = odm(attachments=[("IT.RECORDING_PATH.0", str(tmp_path / "a.mp4"))])
        with patch.object(MODULE, "archive_metadata_csv") as archive:
            res = coa_upload("key", write(tmp_path, data))
        assert any("file not found" in e for e in res["errors"])
        archive.assert_not_called()

    @patch.object(MODULE, "_put_file_to_s3", return_value=None)
    @patch.object(MODULE, "build_retry_session")
    @patch.object(MODULE, "archive_metadata_csv", return_value="rec-1")
    def test_success_archives_json_posts_and_uploads_files(
        self, archive, session, put, tmp_path
    ):
        recording = tmp_path / "a.mp4"
        recording.write_bytes(b"audio")
        data = odm(attachments=[("IT.RECORDING_PATH.0", str(recording))])
        response = MagicMock(status_code=200)
        response.json.return_value = {
            "status": "SUCCESS",
            "code": "200",
            "message": "Upload completed successfully.",
            "records": [{"status": "SUCCESS"}],
            "attachments": [{"checksum": "CHK", "presigned": "https://s3"}],
        }
        session.return_value.post.return_value = response
        checksum = (
            __import__("base64")
            .b64encode(__import__("hashlib").sha256(b"audio").digest())
            .decode()
        )
        response.json.return_value["attachments"][0]["checksum"] = checksum

        res = coa_upload("key", write(tmp_path, data), action="create")

        assert archive.call_args.kwargs["upload_type"] == "coa_json"
        assert archive.call_args.kwargs["content_type"] == "application/json"
        sent = session.return_value.post.call_args.kwargs["json"]
        assert sent == {
            "action": "c",
            "record_id": "rec-1",
            "checksums": {str(recording): checksum},
        }
        put.assert_called_once()
        assert put.call_args.args[0]["recording"] == str(recording)
        assert res["status"] == "SUCCESS"
        assert res["attachments"][0]["upload_status"] == "Success"
        assert "presigned" not in res["attachments"][0]

    @patch.object(MODULE, "build_retry_session")
    @patch.object(MODULE, "archive_metadata_csv", return_value="rec-1")
    def test_request_level_rejection_is_reported(self, archive, session, tmp_path):
        response = MagicMock(status_code=400)
        response.json.return_value = {"record_id": ["Upload record not found."]}
        session.return_value.post.return_value = response
        res = coa_upload("key", write(tmp_path, odm()))
        assert res["status"] == "FAILED"
        assert "Upload record not found" in res["errors"][0]
