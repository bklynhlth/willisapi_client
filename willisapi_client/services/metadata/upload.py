import pandas as pd
from tqdm import tqdm
from datetime import datetime
import requests
import mimetypes
import os

from willisapi_client.timer import measure
from willisapi_client.willisapi_client import WillisapiClient
from willisapi_client.logging_setup import logger as logger
from willisapi_client.services.metadata.utils import (
    MetadataValidation,
    ProcessedMetadataValidation,
    UploadUtils,
    find_files_with_pattern,
    get_last_n_directories,
)


@measure
def upload(api_key: str, csv_path: str, **kwargs):

    force_upload = kwargs.get("force_upload", False)
    csv = MetadataValidation(csv_path=csv_path, force_upload=force_upload)
    if csv.load_and_validate():
        logger.info(f'{datetime.now().strftime("%H:%M:%S")}: csv check passed')
        csv.create_final_csv()

        wc = WillisapiClient(env=kwargs.get("env"))
        url = wc.get_upload_url()
        headers = wc.get_headers()
        headers["Authorization"] = f"token {api_key}"
        logger.info(f'{datetime.now().strftime("%H:%M:%S")}: beginning upload')

        results = []
        for index, row in tqdm(
            csv.transformed_df.iterrows(), total=csv.transformed_df.shape[0]
        ):
            u = UploadUtils(row)
            valid, err = u.validate_row()
            result_row = row.to_dict()
            if valid:
                payload = u.generate_payload()
                res = u.post(api_key, url, headers, payload)
                if res.get("upload_status") == "Success":
                    result_row["upload_status"] = "Success"
                    result_row["error"] = None

                    # Handle S3 upload if presigned URL is provided
                    presigned = res.get("response", {}).get("presigned")
                    if presigned:
                        try:
                            content_type, _ = mimetypes.guess_type(
                                payload.get("filename")
                            )
                            if not content_type:
                                content_type = "application/octet-stream"
                            with open(row.file_path, "rb") as f:
                                response = requests.put(
                                    presigned,
                                    data=f,
                                    headers={
                                        "x-amz-checksum-sha256": payload.get(
                                            "checksum"
                                        ),
                                        "x-amz-sdk-checksum-algorithm": "SHA256",
                                        "Content-Type": content_type,
                                    },
                                )
                            if response.status_code == 200:
                                result_row["upload_status"] = "Success"
                            else:
                                result_row["upload_status"] = "Failed"
                                result_row["error"] = (
                                    f"S3 upload failed with status code {response.status_code}"
                                )
                        except Exception as ex:
                            result_row["upload_status"] = "Failed"
                            result_row["error"] = str(ex)
                    else:
                        result_row["upload_status"] = "Failed"
                        result_row["error"] = (
                            "Collect recording upload URL not received"
                        )
                else:
                    result_row["upload_status"] = "Failed"
                    result_row["error"] = res.get("error")
            else:
                result_row["upload_status"] = "Failed"
                result_row["error"] = f"{err}"
            results.append(result_row)

        results_df = pd.DataFrame(results)
        return results_df
    else:
        logger.error(f'{datetime.now().strftime("%H:%M:%S")}: csv check failed')
        logger.error(csv.errors)
        return None


@measure
def processed_upload(api_key: str, csv_path: str, output_path: str, **kwargs):

    force_upload = kwargs.get("force_upload", False)
    csv = ProcessedMetadataValidation(csv_path=csv_path, force_upload=force_upload)
    if csv.load_and_validate():
        logger.info(f'{datetime.now().strftime("%H:%M:%S")}: csv check passed')
        csv.create_final_csv()

        wc = WillisapiClient(env=kwargs.get("env"))
        url = wc.get_processed_upload_url()
        headers = wc.get_headers()
        headers["Authorization"] = f"token {api_key}"
        logger.info(f'{datetime.now().strftime("%H:%M:%S")}: beginning upload')

        results = []
        for index, row in tqdm(
            csv.transformed_df.iterrows(), total=csv.transformed_df.shape[0]
        ):
            u = UploadUtils(row)
            valid, err = u.validate_processed_data_row()
            result_row = row.to_dict()
            if valid:
                filename = os.path.basename(row.recording).split(".")[0]
                files = []
                for file in find_files_with_pattern(output_path, filename):
                    key, error = get_last_n_directories(file, n=2)
                    if error:
                        continue
                    checksum = u.calculate_file_checksum(file)
                    files.append(
                        {
                            "index": index,
                            "recording": file,
                            "key": key,
                            "checksum": checksum,
                        }
                    )
                payload = u.generate_processed_payload(files)
                res = u.post(api_key, url, headers, payload)
                if res.get("upload_status") == "Success":
                    result_row["upload_status"] = "Success"
                    result_row["error"] = None

                    # Handle S3 upload if presigned URL is provided
                    for file_presigned in res.get("response", []):
                        presigned = file_presigned.get("presigned")
                        checksum = file_presigned.get("checksum")
                        recording = file_presigned.get("recording")
                        index = file_presigned.get("index")
                        try:
                            content_type, _ = mimetypes.guess_type(recording)
                            if not content_type:
                                content_type = "text/csv"
                            with open(recording, "rb") as f:
                                response = requests.put(
                                    presigned,
                                    data=f,
                                    headers={
                                        "x-amz-checksum-sha256": checksum,
                                        "x-amz-sdk-checksum-algorithm": "SHA256",
                                        "Content-Type": content_type,
                                    },
                                )
                            if response.status_code == 200:
                                result_row["upload_status"] = "Success"
                            else:
                                result_row["upload_status"] = "Failed"
                                if result_row["error"] is None:
                                    result_row["error"] = ""
                                result_row["error"] = (
                                    result_row["error"]
                                    + "\n"
                                    + f"S3 upload failed with status code {response.status_code} for file {recording}"
                                )
                        except Exception as ex:
                            result_row["upload_status"] = "Failed"
                            if result_row["error"] is None:
                                result_row["error"] = ""
                            result_row["error"] = result_row["error"] + "\n" + str(ex)
                else:
                    result_row["upload_status"] = "Failed"
                    result_row["error"] = res.get("error")
            else:
                result_row["upload_status"] = "Failed"
                result_row["error"] = f"{err}"
            results.append(result_row)

        results_df = pd.DataFrame(results)
        return results_df
    else:
        logger.error(f'{datetime.now().strftime("%H:%M:%S")}: csv check failed')
        logger.error(csv.errors)
        return None
