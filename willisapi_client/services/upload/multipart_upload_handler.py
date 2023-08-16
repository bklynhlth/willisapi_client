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

    This function to upload data using willis upload API

    Parameters:
    ............
    key: str
        Temporary access token
    data: str
        Path to data csv file

    Returns:
    ............
    summary : pandas Dataframe
        upload summary

    ---------------------------------------------------------------------------------------------------
    """
    csv = CSVValidation(file_path=data)
    if csv._is_valid():
        logger.info(f'{datetime.now().strftime("%H:%M:%S")}: CSV check passed')
        dataframe = csv.df
        wc = WillisapiClient()
        url = wc.get_upload_url()
        headers = wc.get_headers()
        headers['Authorization'] = key
        summary = []
        logger.info(f'{datetime.now().strftime("%H:%M:%S")}: Beginning upload for metadata CSV {data}\n')
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

        res_df = pd.DataFrame(summary, columns=['Filename', 'Update Status', 'Upload Message'])
        number_of_files_uploaded = len(res_df[res_df['Update Status']=="success"])
        number_of_files_failed = len(res_df[res_df['Update Status']=="fail"])
        UploadUtils.summary_logs(number_of_files_uploaded, number_of_files_failed)
        return res_df