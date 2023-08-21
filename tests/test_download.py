from unittest.mock import patch
import pandas as pd
import json

from willisapi_client.services.download.download_handler import download

class TestDownloadFunction:
    def setup(self):
        self.key = "dummy"
        self.project_name = "project"

    @patch('willisapi_client.services.download.download_utils.DownloadUtils.request')
    def test_download_failed(self, mocked_data):
        mocked_data.return_value = {}
        data = download(self.key, self.project_name)
        assert data.empty == True

    @patch('willisapi_client.services.download.download_utils.DownloadUtils.request')
    def test_download_unauthorised(self, mocked_data):
        mocked_data.return_value = {
            "status_code": 403
        }
        data = download(self.key, self.project_name)
        assert data.empty == True

    @patch('willisapi_client.services.download.download_utils.DownloadUtils.request')
    def test_download_missing_auth(self, mocked_data):
        mocked_data.return_value = {
            "status_code": 401
        }
        data = download(self.key, self.project_name)
        assert data.empty == True

    @patch('willisapi_client.services.download.download_utils.DownloadUtils.request')
    def test_download_500_status(self, mocked_data):
        mocked_data.return_value = {
            "status_code": 500
        }
        data = download(self.key, self.project_name)
        assert data.empty == True

    @patch('willisapi_client.services.download.download_utils.DownloadUtils.request')
    def test_download_no_items_from_api(self, mocked_data):
        mocked_data.return_value = {
            "status_code": 200,
            "items": {},
        }
        data = download(self.key, self.project_name)
        assert data.empty == True

    @patch('willisapi_client.services.download.download_utils.DownloadUtils.request')
    def test_download_success(self, mocked_data):
        with open('tests/test.json') as json_file:
            data = json.load(json_file)
        mocked_data.return_value = data
        data = download(self.key, self.project_name)
        assert data.empty == False
        assert data.filename.tolist()[0] == 'test_video.mp4'
        assert data.pt_id_external.tolist()[0] == 'pt_id_external'
        assert len(data.index) == 1


    