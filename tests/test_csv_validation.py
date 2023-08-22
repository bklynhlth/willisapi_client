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

    @patch('willisapi_client.services.upload.csv_validation.CSVValidation._is_valid_headers')
    @patch('willisapi_client.services.upload.csv_validation.CSVValidation._is_file')
    def test_correct_csv_file(self, mocked_headers, mocked_file):
        mocked_headers.return_value = True
        mocked_file.return_value = True
        csv = CSVValidation(file_path="/metadata.csv")
        assert csv._is_valid() == True
        assert csv._is_file() == True
        assert csv._is_valid_file_ext() == True
        assert csv.get_filename() == "metadata.csv"

    @patch('willisapi_client.services.upload.csv_validation.CSVValidation._is_valid_headers')
    @patch('willisapi_client.services.upload.csv_validation.CSVValidation._is_file')
    @patch('willisapi_client.services.upload.csv_validation.CSVValidation._is_file_path_valid')
    def test_csv_row_validation_success(self, mocked_upload_file, mocked_file, mocked_headers):
        mocked_headers.return_value = True
        mocked_file.return_value = True
        mocked_upload_file.return_value = (True, "")
        csv = CSVValidation(file_path="/data.csv")
        assert csv._is_valid() == True
        row = {
            "project_name": "project_name",
            "file_path": "/video.mp4",
            "workflow_tags": "speech_characteristics",
            "pt_id_external": "qwerty",
            "time_collected": "2023-02-02",
        }  
        is_valid, _ = csv.validate_row(row)
        assert is_valid == True

    @patch('willisapi_client.services.upload.csv_validation.CSVValidation._is_valid_headers')
    @patch('willisapi_client.services.upload.csv_validation.CSVValidation._is_file')
    @patch('willisapi_client.services.upload.csv_validation.CSVValidation._is_file_path_valid')
    def test_csv_row_validation_fail_empty_project(self, mocked_upload_file, mocked_file, mocked_headers):
        mocked_headers.return_value = True
        mocked_file.return_value = True
        mocked_upload_file.return_value = (True, "")
        csv = CSVValidation(file_path="/metadata.csv")
        assert csv._is_valid() == True
        row = {
            "project_name": "",
            "file_path": "/video.mp4",
            "workflow_tags": "speech_characteristics",
            "pt_id_external": "qwerty",
            "time_collected": "2023-02-02",
        }  
        is_valid, _ = csv.validate_row(row)
        assert is_valid == False
        
    @patch('willisapi_client.services.upload.csv_validation.CSVValidation._is_valid_headers')
    @patch('willisapi_client.services.upload.csv_validation.CSVValidation._is_file')
    @patch('willisapi_client.services.upload.csv_validation.CSVValidation._is_file_path_valid')
    def test_csv_row_validation_failed_incorrect_file(self, mocked_upload_file, mocked_file, mocked_headers):
        mocked_headers.return_value = True
        mocked_file.return_value = True
        mocked_upload_file.return_value = (False, "Error")
        csv = CSVValidation(file_path="/metadata.csv")
        assert csv._is_valid() == True
        row = {
            "project_name": "project_name",
            "file_path": "",
            "workflow_tags": "speech_characteristics",
            "pt_id_external": "qwerty",
            "time_collected": "2023-02-02",
        }  
        is_valid, _ = csv.validate_row(row)
        assert is_valid == False

    @patch('willisapi_client.services.upload.csv_validation.CSVValidation._is_valid_headers')
    @patch('willisapi_client.services.upload.csv_validation.CSVValidation._is_file')
    @patch('willisapi_client.services.upload.csv_validation.CSVValidation._is_file_path_valid')
    def test_csv_row_validation_failed_incorrect_wfts(self, mocked_upload_file, mocked_file, mocked_headers):
        mocked_headers.return_value = True
        mocked_file.return_value = True
        mocked_upload_file.return_value = (True, None)
        csv = CSVValidation(file_path="/metadata.csv")
        assert csv._is_valid() == True
        row = {
            "project_name": "project_name",
            "file_path": "file.mp4",
            "workflow_tags": "wrong_tag",
            "pt_id_external": "qwerty",
            "time_collected": "2023-02-02",
        }  
        is_valid, _ = csv.validate_row(row)
        assert is_valid == False

    @patch('willisapi_client.services.upload.csv_validation.CSVValidation._is_valid_headers')
    @patch('willisapi_client.services.upload.csv_validation.CSVValidation._is_file')
    @patch('willisapi_client.services.upload.csv_validation.CSVValidation._is_file_path_valid')
    def test_csv_row_validation_dynamic_wfts(self, mocked_upload_file, mocked_file, mocked_headers):
        mocked_headers.return_value = True
        mocked_file.return_value = True
        mocked_upload_file.return_value = (True, None)
        csv = CSVValidation(file_path="/metadata.csv")
        assert csv._is_valid() == True
        row = {
            "project_name": "project_name",
            "file_path": "file.mp4",
            "workflow_tags": "scale_abc,speech_transcription_dummy",
            "pt_id_external": "qwerty",
            "time_collected": "2023-02-02",
        }  
        is_valid, _ = csv.validate_row(row)
        assert is_valid == True

    @patch('willisapi_client.services.upload.csv_validation.CSVValidation._is_valid_headers')
    @patch('willisapi_client.services.upload.csv_validation.CSVValidation._is_file')
    @patch('willisapi_client.services.upload.csv_validation.CSVValidation._is_file_path_valid')
    def test_csv_row_validation_failed_empty_pt_id(self, mocked_upload_file, mocked_file, mocked_headers):
        mocked_headers.return_value = True
        mocked_file.return_value = True
        mocked_upload_file.return_value = (True, None)
        csv = CSVValidation(file_path="/metadata.csv")
        assert csv._is_valid() == True
        row = {
            "project_name": "project_name",
            "file_path": "file.mp4",
            "workflow_tags": "speech_characteristics",
            "pt_id_external": "",
            "time_collected": "2023-02-02",
        }  
        is_valid, _ = csv.validate_row(row)
        assert is_valid == False

    @patch('willisapi_client.services.upload.csv_validation.CSVValidation._is_valid_headers')
    @patch('willisapi_client.services.upload.csv_validation.CSVValidation._is_file')
    @patch('willisapi_client.services.upload.csv_validation.CSVValidation._is_file_path_valid')
    def test_csv_row_validation_failed_incorrect_time_collected(self, mocked_upload_file, mocked_file, mocked_headers):
        mocked_headers.return_value = True
        mocked_file.return_value = True
        mocked_upload_file.return_value = (True, None)
        csv = CSVValidation(file_path="/metadata.csv")
        assert csv._is_valid() == True
        row = {
            "project_name": "project_name",
            "file_path": "file.mp4",
            "workflow_tags": "speech_characteristics",
            "pt_id_external": "",
            "time_collected": "20-02-02",
        }  
        is_valid, _ = csv.validate_row(row)
        assert is_valid == False