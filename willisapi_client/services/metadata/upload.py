import pandas as pd
from tqdm import tqdm
from datetime import datetime
import requests

from willisapi_client.timer import measure
from willisapi_client.willisapi_client import WillisapiClient
from willisapi_client.logging_setup import logger as logger
from willisapi_client.services.metadata.utils import MetadataValidation, UploadUtils


@measure
def metadata_upload(api_key: str, csv_path: str, **kwargs):

    csv = MetadataValidation(csv_path=csv_path)
    if csv.load_and_validate():
        logger.info(f'{datetime.now().strftime("%H:%M:%S")}: csv check passed')
        csv.create_final_csv()

        wc = WillisapiClient(env=kwargs.get("env"))
        url = wc.get_metadata_upload_url()
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
                            with open(row.file_path, "rb") as f:
                                files = {"file": f}
                                response = requests.put(
                                    presigned,
                                    data=files["file"],
                                    headers={"Content-Type": "audio/ogg"},
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
        results_df.to_csv(
            f"metadata_upload_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            index=False,
        )
        return results_df
    else:
        logger.error(f'{datetime.now().strftime("%H:%M:%S")}: csv check failed')
        return None
