# website:   https://www.brooklyn.health
import pandas as pd
from tqdm import tqdm
from datetime import datetime

from willisapi_client.willisapi_client import WillisapiClient
from willisapi_client.services.upload.csv_validation import CSVValidation
from willisapi_client.services.upload.upload_utils import UploadUtils
from willisapi_client.logging_setup import logger as logger
from willisapi_client.timer import measure

@measure
def upload(key, data):
    """
    ---------------------------------------------------------------------------------------------------
    Function: upload

    Description: This function upload data using willis upload API

    Parameters:
    ----------
    key: AWS access id token
    data: Path to metadata csv file

    Returns:
    ----------
    summary: Returns upload summary pandas Dataframe
    ---------------------------------------------------------------------------------------------------
    """
    csv = CSVValidation(file_path=data)
    if csv._is_valid():
        logger.info(f'{datetime.now().strftime("%H:%M:%S")}: csv check passed')
        dataframe = csv.get_dataframe()
        wc = WillisapiClient()
        url = wc.get_upload_url()
        headers = wc.get_headers()
        headers['Authorization'] = key
        summary = []
        logger.info(f'{datetime.now().strftime("%H:%M:%S")}: beginning upload')
        for index, row in tqdm(dataframe.iterrows(), total=dataframe.shape[0]):
            (is_valid_row, error) = csv.validate_row(row)
            if is_valid_row:
                uploaded = UploadUtils.upload(index, row, url, headers)
                if uploaded:
                    summary.append([row.file_path, "success", None])
                else:
                    summary.append([row.file_path, "fail", None])
            else:
                summary.append([row.file_path, "fail", error])

        res_df = pd.DataFrame(summary, columns=['filename', 'upload_status', 'upload_message'])
        number_of_files_uploaded = len(res_df[res_df['upload_status']=="success"])
        number_of_files_failed = len(res_df[res_df['upload_status']=="fail"])
        UploadUtils.summary_logs(number_of_files_uploaded, number_of_files_failed)
        return res_df
