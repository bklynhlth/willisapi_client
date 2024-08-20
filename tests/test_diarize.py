from unittest.mock import patch

from willisapi_client.services.diarize.willisdiarize import (
    willis_diarize,
)
from willisapi_client.logging_setup import logger as logger
import logging


class TestDiarizeFunction:
    def setup(self):
        self.key = "dummy"
        self.file_path = "file.json"

    @patch(
        "willisapi_client.services.diarize.diarize_utils.DiarizeUtils.decode_response"
    )
    @patch(
        "willisapi_client.services.diarize.diarize_utils.DiarizeUtils.request_diarize"
    )
    @patch(
        "willisapi_client.services.diarize.diarize_utils.DiarizeUtils.read_json_file"
    )
    @patch(
        "willisapi_client.services.diarize.diarize_utils.DiarizeUtils.is_valid_file_path"
    )
    def test_willis_diarize_function_success(
        self, mock_file_path, mock_json, mock_api_res, mock_decoded_res, caplog
    ):
        mock_file_path.return_value = True
        mock_json.return_value = {"Correct Transcription"}
        mock_api_res.return_value = {
            "status_code": 200,
            "data": "Encoded Response",
        }
        mock_decoded_res.return_value = {"Correct Transcription"}
        with caplog.at_level(logging.INFO):
            res = willis_diarize(self.key, self.file_path)

        assert res == {"Correct Transcription"}

    @patch(
        "willisapi_client.services.diarize.diarize_utils.DiarizeUtils.request_diarize"
    )
    @patch(
        "willisapi_client.services.diarize.diarize_utils.DiarizeUtils.read_json_file"
    )
    @patch(
        "willisapi_client.services.diarize.diarize_utils.DiarizeUtils.is_valid_file_path"
    )
    def test_willis_diarize_function_invalid_json(
        self, mock_file_path, mock_json, mock_api_res, caplog
    ):
        mock_file_path.return_value = False
        mock_json.return_value = {}
        mock_api_res.return_value = {
            "status_code": 200,
            "data": "Encoded Response",
        }

        with caplog.at_level(logging.INFO):
            res = willis_diarize(self.key, self.file_path)

        assert "Input file type is incorrect. We only accept JSON files" in caplog.text
