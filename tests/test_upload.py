from unittest.mock import patch
import pandas as pd

from willisapi_client.services.upload.multipart_upload_handler import upload
from willisapi_client.services.upload.csv_validation import CSVValidation
class TestUpload:
    def setup(self):
        self.key = "dummy key"
        self.metadata = "data.csv"
        self.df_row = ['dev_testing', 'video.mp4', 'tags', 'qwerty', '2023-22-22',]
        self.df_cols = ['project_name', 'file_path', 'workflow_tags', 'pt_id_external', 'time_collected']
        self.response_df_cols = ['filename', 'upload_status', 'upload_message']

    @patch('willisapi_client.services.upload.csv_validation.CSVValidation.validate_row')
    @patch('willisapi_client.services.upload.csv_validation.CSVValidation.get_dataframe')
    @patch('willisapi_client.services.upload.csv_validation.CSVValidation._is_valid')
    def test_upload_row_validation_failed(self, mock_valid_csv, mocked_df, mock_row_validation):
        mock_valid_csv.return_value = True
        mocked_df.return_value = pd.DataFrame([self.df_row], columns=self.df_cols)
        mock_row_validation.return_value = False, "Err"
        df = upload(self.key, self.metadata)
        num = len(df[df['upload_status']=="fail"])
        assert num == 1
        assert list(df.columns) == self.response_df_cols
        assert df.iloc[0].filename == 'video.mp4'
        assert df.iloc[0].upload_status == 'fail'

    @patch('willisapi_client.services.upload.upload_utils.UploadUtils.upload')
    @patch('willisapi_client.services.upload.csv_validation.CSVValidation.validate_row')
    @patch('willisapi_client.services.upload.csv_validation.CSVValidation.get_dataframe')
    @patch('willisapi_client.services.upload.csv_validation.CSVValidation._is_valid')
    def test_upload_success(self, mock_valid_csv, mocked_df, mock_row_validation, mocked_upload):
        mock_valid_csv.return_value = True
        mocked_df.return_value = pd.DataFrame([self.df_row], columns=self.df_cols)
        mock_row_validation.return_value = True, None
        mocked_upload.return_value = True
        df = upload(self.key, self.metadata)
        num = len(df[df['upload_status']=="success"])
        assert num == 1
        assert list(df.columns) == self.response_df_cols
        assert df.iloc[0].filename == 'video.mp4'
        assert df.iloc[0].upload_status == 'success'
        assert df.iloc[0].upload_message == None        
