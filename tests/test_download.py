from unittest.mock import patch
import pandas as pd

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
        mocked_data = {}
        # mocked_data.return_value = {
        #     "status_code": 200,
        #     "items": {
        #         "project": [
        #             {
        #                 "project_Id": "id",
        #                 "project_name": "dev_testing",
        #                 "participant": [
        #                     {
        #                         "participant_Id": "pt_id",
        #                         "results": [
        #                             {
        #                                 "file_name": "video.mp4",
        #                                 "file_type": None,
        #                                 "timestamp": None,
        #                                 "measures":{
        #                                     "emotional_expressivity_summary": [{'measure1': [1,2]}],
        #                                     "facial_expressivity_summary": [{'measure2': [1,2]}],
        #                                     "vocal_acoustics_summary": [{'measure3': [1,2]}],
        #                                     "speech_characteristics_summary": [{'measure4': [1,2]}],
        #                                 }
        #                             }
        #                         ]
        #                     }
        #                 ]
        #             }
        #         ]
        #     },
        # }
        data = download(self.key, self.project_name)
        assert data.empty == True
    