from unittest.mock import patch

from willisapi_client.services.diarize.willisdiarize import (
    willis_diarize_call_remaining,
)


class TestDiarizeCallsFunction:
    def setup(self):
        self.key = "dummy"

    @patch("willisapi_client.services.diarize.diarize_utils.DiarizeUtils.request")
    def test_willisdiarize_remaining_calls_failed(self, mocked_data):
        mocked_data.return_value = {}
        res = willis_diarize_call_remaining(self.key)
        assert res == None

    @patch("willisapi_client.services.diarize.diarize_utils.DiarizeUtils.request")
    def test_willisdiarize_remaining_calls_missing_auth(self, mocked_data):
        mocked_data.return_value = {"status_code": 401, "message": "message"}
        res = willis_diarize_call_remaining(self.key)
        assert res == "message"

    @patch("willisapi_client.services.diarize.diarize_utils.DiarizeUtils.request")
    def test_willisdiarize_remaining_calls_success(self, mocked_data):
        mocked_data.return_value = {
            "status_code": 401,
            "message": "Your account has 10 WillisDiarize API calls remaining.",
        }
        res = willis_diarize_call_remaining(self.key)
        assert res == "Your account has 10 WillisDiarize API calls remaining."
