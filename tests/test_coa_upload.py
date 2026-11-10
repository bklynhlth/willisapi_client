import json
import sys
from unittest.mock import MagicMock, patch

from willisapi_client.services.metadata.coa_upload import COAUploadUtils, coa_upload
from willisapi_client.services.metadata.upload import _FileSlice, _put_part_to_s3

# The package re-exports the function ``coa_upload``, which shadows this module
# as an attribute; Python 3.9's ``patch("pkg.mod.name")`` resolves by getattr and
# would land on the function, so patch the module object itself.
MODULE = sys.modules["willisapi_client.services.metadata.coa_upload"]


def checksum_of(path):
    import base64
    import hashlib

    return base64.b64encode(hashlib.sha256(path.read_bytes()).digest()).decode()


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
        assert res["code"] == "400"
        assert res["message"] == "Invalid request. Please check the request payload."
        assert "Unsupported action" in res["errors"][0]

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
        assert "attachments" not in res

    @patch.object(MODULE, "build_retry_session")
    @patch.object(MODULE, "archive_metadata_csv", return_value="rec-1")
    def test_request_level_rejection_is_reported(self, archive, session, tmp_path):
        response = MagicMock(status_code=400)
        response.json.return_value = {
            "status": "FAILED",
            "code": "400",
            "message": "Invalid request. Please check the request payload.",
            "errors": ["record_id: Upload record not found."],
        }
        session.return_value.post.return_value = response
        res = coa_upload("key", write(tmp_path, odm()))
        assert res["status"] == "FAILED"
        assert res["code"] == "400"
        assert "Upload record not found" in res["errors"][0]

    @patch.object(MODULE, "build_retry_session")
    @patch.object(MODULE, "archive_metadata_csv", return_value="rec-1")
    def test_a_response_that_is_not_json_is_a_503(self, archive, session, tmp_path):
        response = MagicMock(status_code=502, text="<html>bad gateway</html>")
        response.json.side_effect = ValueError
        session.return_value.post.return_value = response
        res = coa_upload("key", write(tmp_path, odm()))
        assert res["code"] == "503"
        assert res["message"] == (
            "The service is temporarily unavailable. Please try again later."
        )


class TestConfirmation:
    def _pending(self, session, recording, patch_response):
        post = MagicMock(status_code=202)
        post.json.return_value = {
            "status": "PENDING",
            "code": "202",
            "message": "Data saved. Upload the attachments, then confirm.",
            "records": [{"status": "PENDING"}],
            "attachments": [{"checksum": checksum_of(recording), "presigned": "u"}],
        }
        session.return_value.post.return_value = post
        session.return_value.patch.return_value = patch_response

    @patch.object(MODULE, "_put_file_to_s3", return_value=None)
    @patch.object(MODULE, "build_retry_session")
    @patch.object(MODULE, "archive_metadata_csv", return_value="rec-1")
    def test_pending_records_are_confirmed_after_the_files_are_sent(
        self, archive, session, put, tmp_path
    ):
        recording = tmp_path / "a.mp4"
        recording.write_bytes(b"audio")
        confirmed = MagicMock(status_code=200)
        confirmed.json.return_value = {
            "status": "SUCCESS",
            "code": "200",
            "message": "Upload completed successfully.",
            "records": [{"status": "SUCCESS"}],
        }
        self._pending(session, recording, confirmed)
        data = odm(attachments=[("IT.RECORDING_PATH.0", str(recording))])

        res = coa_upload("key", write(tmp_path, data))

        assert session.return_value.patch.call_args.args[0].endswith(
            "metadata/coa-upload/rec-1"
        )
        assert res["status"] == "SUCCESS"
        assert res["records"] == [{"status": "SUCCESS"}]
        assert "attachments" not in res

    @patch.object(MODULE, "_put_file_to_s3", return_value="boom")
    @patch.object(MODULE, "build_retry_session")
    @patch.object(MODULE, "archive_metadata_csv", return_value="rec-1")
    def test_a_failed_put_is_still_confirmed_so_the_api_marks_the_coa_incomplete(
        self, archive, session, put, tmp_path
    ):
        recording = tmp_path / "a.mp4"
        recording.write_bytes(b"audio")
        confirmed = MagicMock(status_code=400)
        confirmed.json.return_value = {
            "status": "FAILED",
            "code": "400",
            "message": "Data upload failed.",
            "records": [{"status": "FAILED", "errors": ["file was not uploaded"]}],
        }
        self._pending(session, recording, confirmed)
        data = odm(attachments=[("IT.RECORDING_PATH.0", str(recording))])

        res = coa_upload("key", write(tmp_path, data))

        session.return_value.patch.assert_called_once()
        assert res["status"] == "FAILED"
        assert res["code"] == "400"
        assert res["records"][0]["errors"] == ["file was not uploaded"]
        assert "attachments" not in res


class TestMultipart:
    def test_part_checksums_and_the_composite_s3_reports(self, tmp_path):
        import base64
        import hashlib

        f = tmp_path / "big.mp4"
        f.write_bytes(b"0123456789")

        digests = COAUploadUtils.calculate_part_checksums(str(f), 4)

        assert digests == [
            base64.b64encode(hashlib.sha256(chunk).digest()).decode()
            for chunk in (b"0123", b"4567", b"89")
        ]
        raw = b"".join(base64.b64decode(d) for d in digests)
        assert (
            COAUploadUtils.composite_checksum(digests)
            == base64.b64encode(hashlib.sha256(raw).digest()).decode() + "-3"
        )

    def test_file_slice_streams_its_window_and_rewinds(self, tmp_path):
        f = tmp_path / "x.bin"
        f.write_bytes(b"0123456789")
        part = _FileSlice(str(f), 4, 4)

        assert len(part) == 4
        assert part.read(3) == b"456"
        assert part.read() == b"7"
        assert part.read() == b""
        part.seek(0)
        assert part.read() == b"4567"
        part.close()

    @patch.object(MODULE, "_put_part_to_s3", return_value=None)
    @patch.object(MODULE, "build_retry_session")
    @patch.object(MODULE, "archive_metadata_csv", return_value="rec-1")
    def test_a_big_file_is_sent_as_parts_in_one_pool_and_confirmed(
        self, archive, session, put_part, tmp_path
    ):
        recording = tmp_path / "a.mp4"
        recording.write_bytes(b"0123456789")
        data = odm(attachments=[("IT.RECORDING_PATH.0", str(recording))])
        post = MagicMock(status_code=202)

        def issue(*args, **kwargs):
            sent = kwargs["json"]
            post.json.return_value = {
                "status": "PENDING",
                "code": "202",
                "message": "m",
                "records": [{"status": "PENDING"}],
                "attachments": [
                    {
                        "checksum": sent["checksums"][str(recording)],
                        "multipart": {
                            "upload_id": "up",
                            "part_size": sent["part_size"],
                            "parts": [
                                {"number": n, "presigned": f"https://s3/{n}"}
                                for n in (1, 2, 3)
                            ],
                        },
                    }
                ],
            }
            return post

        session.return_value.post.side_effect = issue
        confirmed = MagicMock(status_code=200)
        confirmed.json.return_value = {
            "status": "SUCCESS",
            "code": "200",
            "message": "ok",
            "records": [{"status": "SUCCESS"}],
        }
        session.return_value.patch.return_value = confirmed

        with patch.object(MODULE, "PART_SIZE", 4):
            res = coa_upload("key", write(tmp_path, data), workers=2)

        sent = session.return_value.post.call_args.kwargs["json"]
        assert sent["part_size"] == 4
        assert len(sent["part_checksums"][str(recording)]) == 3
        assert sent["checksums"][str(recording)].endswith("-3")
        windows = sorted(
            (c.args[0]["number"], c.args[0]["offset"], c.args[0]["length"])
            for c in put_part.call_args_list
        )
        assert windows == [(1, 0, 4), (2, 4, 4), (3, 8, 2)]
        assert res["status"] == "SUCCESS"
        assert "attachments" not in res

    @patch.object(MODULE, "_put_part_to_s3", side_effect=[None, "part 2 failed", None])
    @patch.object(MODULE, "build_retry_session")
    @patch.object(MODULE, "archive_metadata_csv", return_value="rec-1")
    def test_one_failed_part_fails_the_file_but_is_still_confirmed(
        self, archive, session, put_part, tmp_path
    ):
        recording = tmp_path / "a.mp4"
        recording.write_bytes(b"0123456789")
        data = odm(attachments=[("IT.RECORDING_PATH.0", str(recording))])
        post = MagicMock(status_code=202)

        def issue(*args, **kwargs):
            post.json.return_value = {
                "status": "PENDING",
                "code": "202",
                "message": "m",
                "records": [{"status": "PENDING"}],
                "attachments": [
                    {
                        "checksum": kwargs["json"]["checksums"][str(recording)],
                        "multipart": {
                            "part_size": 4,
                            "parts": [
                                {"number": n, "presigned": "u"} for n in (1, 2, 3)
                            ],
                        },
                    }
                ],
            }
            return post

        session.return_value.post.side_effect = issue
        confirmed = MagicMock(status_code=400)
        confirmed.json.return_value = {
            "status": "FAILED",
            "code": "400",
            "message": "x",
            "records": [{"status": "FAILED"}],
        }
        session.return_value.patch.return_value = confirmed

        with patch.object(MODULE, "PART_SIZE", 4):
            res = coa_upload("key", write(tmp_path, data), workers=1)

        session.return_value.patch.assert_called_once()
        assert "attachments" not in res
        assert res["status"] == "FAILED"


class TestStopAfterFailedPart:
    def test_remaining_parts_are_skipped_once_one_fails(self, tmp_path):
        import threading

        f = tmp_path / "x.bin"
        f.write_bytes(b"0123456789")
        stop = threading.Event()
        part = {
            "number": 1,
            "presigned": "https://s3/1",
            "checksum": "c",
            "recording": str(f),
            "offset": 0,
            "length": 4,
            "stop": stop,
        }
        upload_module = sys.modules["willisapi_client.services.metadata.upload"]
        with patch.object(upload_module, "build_retry_session") as session:
            session.return_value.put.return_value = MagicMock(
                status_code=403, text="denied"
            )
            first = _put_part_to_s3(part)
            second = _put_part_to_s3({**part, "number": 2})

        assert "status code 403" in first
        assert stop.is_set()
        assert "skipped" in second
        assert session.return_value.put.call_count == 1


class TestSameContentFiles:
    @patch.object(MODULE, "_put_file_to_s3", return_value=None)
    @patch.object(MODULE, "build_retry_session")
    @patch.object(MODULE, "archive_metadata_csv", return_value="rec-1")
    def test_files_with_identical_content_are_each_sent_from_their_own_path(
        self, archive, session, put, tmp_path
    ):
        first, second = tmp_path / "a.txt", tmp_path / "b.txt"
        first.write_bytes(b"same")
        second.write_bytes(b"same")
        data = odm(attachments=[("IT.NOTES_PATH.1", str(first))])
        other = odm(attachments=[("IT.NOTES_PATH.1", str(second))])
        subjects = other["ClinicalData"][0]["SubjectData"]
        subjects[0]["SubjectKey"] = "P2"
        data["ClinicalData"][0]["SubjectData"].append(subjects[0])
        checksum = checksum_of(first)
        post = MagicMock(status_code=202)
        post.json.return_value = {
            "status": "PENDING",
            "code": "202",
            "message": "m",
            "records": [{"status": "PENDING"}, {"status": "PENDING"}],
            "attachments": [
                {
                    "record": n,
                    "item_oid": "IT.NOTES_PATH.1",
                    "checksum": checksum,
                    "presigned": "u",
                }
                for n in (0, 1)
            ],
        }
        session.return_value.post.return_value = post
        confirmed = MagicMock(status_code=200)
        confirmed.json.return_value = {
            "status": "SUCCESS",
            "code": "200",
            "message": "ok",
            "records": [{"status": "SUCCESS"}, {"status": "SUCCESS"}],
        }
        session.return_value.patch.return_value = confirmed

        coa_upload("key", write(tmp_path, data))

        sent = sorted(c.args[0]["recording"] for c in put.call_args_list)
        assert sent == sorted([str(first), str(second)])
