# website:   https://www.brooklyn.health
import pandas as pd
import asyncio
import sys
from datetime import datetime

from willisapi_client.willisapi_client import WillisapiClient
from willisapi_client.services.upload.csv_validation import CSVValidation
from willisapi_client.services.upload.upload_utils import UploadUtils

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
        print(f'{datetime.now().strftime("%H:%M:%S")}: CSV check passed')
        dataframe = csv.df
        wc = WillisapiClient()
        url = wc.get_upload_url()
        headers = wc.get_headers()
        headers['Authorization'] = key
        summary = []
        print(f'{datetime.now().strftime("%H:%M:%S")}: Beginning upload for metadata CSV {data}\n')
        for index, row in dataframe.iterrows():
            if csv.validate_row(row):
                uploaded = UploadUtils.upload(row, url, headers)
                print(f"progress - {100 * (index+1)/len(dataframe)}%")
                if uploaded:
                    summary.append([row.file_path, "success"])
                else:
                    summary.append([row.file_path, "fail"])
            else:
                print(f"Data Validation failed for row {row.tolist()}")
        return pd.DataFrame(summary, columns=['Filename', 'Update Status'])