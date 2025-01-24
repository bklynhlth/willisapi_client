from willisapi_client.services.upload.csv_validation import CSVValidation

from unittest.mock import patch


class TestCSVValidation:
    def test_directory(self):
        csv = CSVValidation(file_path="/")
        assert csv._is_valid() == False

    def test_incorrect_mp4_file(self):
        csv = CSVValidation(file_path="/video.mp4")
        assert csv._is_valid() == False

    def test_incorrect_csv_file(self):
        csv = CSVValidation(file_path="/metadata.csv")
        assert csv._is_valid() == False

    @patch(
        "willisapi_client.services.upload.csv_validation.CSVValidation._is_valid_headers"
    )
    @patch("willisapi_client.services.upload.csv_validation.CSVValidation._is_file")
    def test_correct_csv_file(self, mocked_headers, mocked_file):
        mocked_headers.return_value = True
        mocked_file.return_value = True
        csv = CSVValidation(file_path="/metadata.csv")
        assert csv._is_valid() == True
        assert csv._is_file() == True
        assert csv._is_valid_file_ext() == True
        assert csv.get_filename() == "metadata.csv"

    @patch(
        "willisapi_client.services.upload.csv_validation.CSVValidation._is_valid_headers"
    )
    @patch("willisapi_client.services.upload.csv_validation.CSVValidation._is_file")
    @patch(
        "willisapi_client.services.upload.csv_validation.CSVValidation._is_file_path_valid"
    )
    def test_csv_row_validation_success(
        self, mocked_upload_file, mocked_file, mocked_headers
    ):
        mocked_headers.return_value = True
        mocked_file.return_value = True
        mocked_upload_file.return_value = (True, "")
        csv = CSVValidation(file_path="/data.csv")
        assert csv._is_valid() == True
        row = {
            "study_id_ext": "study_id_ext",
            "file_path": "/video.mp4",
            "workflow": "speech_characteristics",
            "pt_id_external": "qwerty",
            "language": "en-US",
            "age": 30,
            "sex": "M",
            "race": "Asian",
            "arm": "Study Arm",
        }
        is_valid, _ = csv.validate_row(row)
        assert is_valid == True

    @patch(
        "willisapi_client.services.upload.csv_validation.CSVValidation._is_valid_headers"
    )
    @patch("willisapi_client.services.upload.csv_validation.CSVValidation._is_file")
    @patch(
        "willisapi_client.services.upload.csv_validation.CSVValidation._is_file_path_valid"
    )
    def test_csv_row_validation_fail_empty_project(
        self, mocked_upload_file, mocked_file, mocked_headers
    ):
        mocked_headers.return_value = True
        mocked_file.return_value = True
        mocked_upload_file.return_value = (True, "")
        csv = CSVValidation(file_path="/metadata.csv")
        assert csv._is_valid() == True
        row = {
            "study_id_ext": "",
            "file_path": "/video.mp4",
            "workflow": "speech_characteristics",
            "pt_id_external": "qwerty",
        }
        is_valid, _ = csv.validate_row(row)
        assert is_valid == False

    @patch(
        "willisapi_client.services.upload.csv_validation.CSVValidation._is_valid_headers"
    )
    @patch("willisapi_client.services.upload.csv_validation.CSVValidation._is_file")
    @patch(
        "willisapi_client.services.upload.csv_validation.CSVValidation._is_file_path_valid"
    )
    def test_csv_row_validation_failed_incorrect_file(
        self, mocked_upload_file, mocked_file, mocked_headers
    ):
        mocked_headers.return_value = True
        mocked_file.return_value = True
        mocked_upload_file.return_value = (False, "Error")
        csv = CSVValidation(file_path="/metadata.csv")
        assert csv._is_valid() == True
        row = {
            "study_id_ext": "study_id_ext",
            "file_path": "",
            "workflow": "speech_characteristics",
            "pt_id_external": "qwerty"
        }
        is_valid, _ = csv.validate_row(row)
        assert is_valid == False

    @patch(
        "willisapi_client.services.upload.csv_validation.CSVValidation._is_valid_headers"
    )
    @patch("willisapi_client.services.upload.csv_validation.CSVValidation._is_file")
    @patch(
        "willisapi_client.services.upload.csv_validation.CSVValidation._is_file_path_valid"
    )
    def test_csv_row_validation_failed_incorrect_wfts(
        self, mocked_upload_file, mocked_file, mocked_headers
    ):
        mocked_headers.return_value = True
        mocked_file.return_value = True
        mocked_upload_file.return_value = (True, None)
        csv = CSVValidation(file_path="/metadata.csv")
        assert csv._is_valid() == True
        row = {
            "study_id_ext": "study_id_ext",
            "file_path": "file.mp4",
            "workflow": "wrong_tag",
            "pt_id_external": "qwerty",
        }
        is_valid, _ = csv.validate_row(row)
        assert is_valid == False

    @patch(
        "willisapi_client.services.upload.csv_validation.CSVValidation._is_valid_headers"
    )
    @patch("willisapi_client.services.upload.csv_validation.CSVValidation._is_file")
    @patch(
        "willisapi_client.services.upload.csv_validation.CSVValidation._is_file_path_valid"
    )
    def test_csv_row_validation_dynamic_wfts(
        self, mocked_upload_file, mocked_file, mocked_headers
    ):
        mocked_headers.return_value = True
        mocked_file.return_value = True
        mocked_upload_file.return_value = (True, None)
        csv = CSVValidation(file_path="/metadata.csv")
        assert csv._is_valid() == True
        row = {
            "study_id_ext": "study_id_ext",
            "file_path": "file.mp4",
            "workflow": "scale_abc,speech_transcription_aws_dummy",
            "pt_id_external": "qwerty",
            "language": "en-US",
            "age": 30,
            "sex": "M",
            "race": "Asian",
            "arm": "Study Arm",
        }
        is_valid, _ = csv.validate_row(row)
        assert is_valid == True

    @patch(
        "willisapi_client.services.upload.csv_validation.CSVValidation._is_valid_headers"
    )
    @patch("willisapi_client.services.upload.csv_validation.CSVValidation._is_file")
    @patch(
        "willisapi_client.services.upload.csv_validation.CSVValidation._is_file_path_valid"
    )
    def test_csv_row_validation_failed_empty_pt_id(
        self, mocked_upload_file, mocked_file, mocked_headers
    ):
        mocked_headers.return_value = True
        mocked_file.return_value = True
        mocked_upload_file.return_value = (True, None)
        csv = CSVValidation(file_path="/metadata.csv")
        assert csv._is_valid() == True
        row = {
            "study_id_ext": "study_id_ext",
            "file_path": "file.mp4",
            "workflow": "speech_characteristics",
            "pt_id_external": "",
        }
        is_valid, _ = csv.validate_row(row)
        assert is_valid == False

    @patch(
        "willisapi_client.services.upload.csv_validation.CSVValidation._is_valid_headers"
    )
    @patch("willisapi_client.services.upload.csv_validation.CSVValidation._is_file")
    @patch(
        "willisapi_client.services.upload.csv_validation.CSVValidation._is_file_path_valid"
    )
    def test_csv_row_validation_failed_empty_language_code(
        self, mocked_upload_file, mocked_file, mocked_headers
    ):
        mocked_headers.return_value = True
        mocked_file.return_value = True
        mocked_upload_file.return_value = (True, None)
        csv = CSVValidation(file_path="/metadata.csv")
        assert csv._is_valid() == True
        row = {
            "study_id_ext": "study_id_ext",
            "file_path": "file.mp4",
            "workflow": "speech_characteristics",
            "pt_id_external": "",
            "language": None,
        }
        is_valid, _ = csv.validate_row(row)
        assert is_valid == False

    @patch(
        "willisapi_client.services.upload.csv_validation.CSVValidation._is_valid_headers"
    )
    @patch("willisapi_client.services.upload.csv_validation.CSVValidation._is_file")
    @patch(
        "willisapi_client.services.upload.csv_validation.CSVValidation._is_file_path_valid"
    )
    def test_csv_row_validation_passed_non_empty_language_code(
        self, mocked_upload_file, mocked_file, mocked_headers
    ):
        mocked_headers.return_value = True
        mocked_file.return_value = True
        mocked_upload_file.return_value = (True, None)
        csv = CSVValidation(file_path="/metadata.csv")
        assert csv._is_valid() == True
        row = {
            "study_id_ext": "study_id_ext",
            "file_path": "file.mp4",
            "workflow": "speech_characteristics",
            "pt_id_external": "",
            "language": "random",
        }
        is_valid, _ = csv.validate_row(row)
        assert is_valid == False

    @patch(
        "willisapi_client.services.upload.csv_validation.CSVValidation._is_valid_headers"
    )
    @patch("willisapi_client.services.upload.csv_validation.CSVValidation._is_file")
    @patch(
        "willisapi_client.services.upload.csv_validation.CSVValidation._is_file_path_valid"
    )
    def test_csv_row_validation_fail_empty_age(
        self, mocked_upload_file, mocked_file, mocked_headers
    ):
        mocked_headers.return_value = True
        mocked_file.return_value = True
        mocked_upload_file.return_value = (True, None)
        csv = CSVValidation(file_path="/metadata.csv")
        assert csv._is_valid() == True
        row = {
            "study_id_ext": "study_id_ext",
            "file_path": "file.mp4",
            "workflow": "speech_characteristics",
            "pt_id_external": "qwerty",
            "language": "en-US",
            "age": "",
        }
        is_valid, _ = csv.validate_row(row)
        assert is_valid == False

    @patch(
        "willisapi_client.services.upload.csv_validation.CSVValidation._is_valid_headers"
    )
    @patch("willisapi_client.services.upload.csv_validation.CSVValidation._is_file")
    @patch(
        "willisapi_client.services.upload.csv_validation.CSVValidation._is_file_path_valid"
    )
    def test_csv_row_validation_fail_empty_sex_field(
        self, mocked_upload_file, mocked_file, mocked_headers
    ):
        mocked_headers.return_value = True
        mocked_file.return_value = True
        mocked_upload_file.return_value = (True, None)
        csv = CSVValidation(file_path="/metadata.csv")
        assert csv._is_valid() == True
        row = {
            "study_id_ext": "study_id_ext",
            "file_path": "file.mp4",
            "workflow": "speech_characteristics",
            "pt_id_external": "qwerty",
            "language": "en-US",
            "age": 30,
            "sex": "",
        }
        is_valid, _ = csv.validate_row(row)
        assert is_valid == False

    @patch(
        "willisapi_client.services.upload.csv_validation.CSVValidation._is_valid_headers"
    )
    @patch("willisapi_client.services.upload.csv_validation.CSVValidation._is_file")
    @patch(
        "willisapi_client.services.upload.csv_validation.CSVValidation._is_file_path_valid"
    )
    def test_csv_row_validation_fail_empty_race(
        self, mocked_upload_file, mocked_file, mocked_headers
    ):
        mocked_headers.return_value = True
        mocked_file.return_value = True
        mocked_upload_file.return_value = (True, "")
        csv = CSVValidation(file_path="/metadata.csv")
        assert csv._is_valid() == True
        row = {
            "study_id_ext": "study_id_ext",
            "file_path": "/video.mp4",
            "workflow": "speech_characteristics",
            "pt_id_external": "qwerty",
            "language": "en-US",
            "age": 30,
            "sex": "M",
            "race": "",
        }
        is_valid, _ = csv.validate_row(row)
        assert is_valid == False

    @patch(
        "willisapi_client.services.upload.csv_validation.CSVValidation._is_valid_headers"
    )
    @patch("willisapi_client.services.upload.csv_validation.CSVValidation._is_file")
    @patch(
        "willisapi_client.services.upload.csv_validation.CSVValidation._is_file_path_valid"
    )
    def test_csv_row_validation_fail_empty_arm(
        self, mocked_upload_file, mocked_file, mocked_headers
    ):
        mocked_headers.return_value = True
        mocked_file.return_value = True
        mocked_upload_file.return_value = (True, "")
        csv = CSVValidation(file_path="/metadata.csv")
        assert csv._is_valid() == True
        row = {
            "study_id_ext": "study_id_ext",
            "file_path": "/video.mp4",
            "workflow": "speech_characteristics",
            "pt_id_external": "qwerty",
            "language": "en-US",
            "age": 30,
            "sex": "M",
            "race": "Asian",
            "arm": "",
        }
        is_valid, _ = csv.validate_row(row)
        assert is_valid == False
