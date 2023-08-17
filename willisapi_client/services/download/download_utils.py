import requests
import json
import time
import random
import pandas as pd
from typing import Tuple

class DownloadUtils:
    def request(url, headers, try_number):
        """
        This is an internal download function which makes a GET API call to brooklyn.health API server

        Returns:
            json
        """
        try:
            response = requests.get(url, headers=headers)
            res_json = response.json()
        except (requests.exceptions.ConnectionError, json.decoder.JSONDecodeError) as ex:
            if try_number == 3:
                raise 
            time.sleep(random.random()*2)
            return DownloadUtils.request(url, headers, try_number=try_number+1)
        else:
            return res_json
    
    def generate_response_df(response) -> Tuple[pd.DataFrame, str]:
        """
        This function converts the json data to dataframe
        """
        response_df = pd.DataFrame()
        try:
            project_name = response["items"]["project"]["project_name"]
            num_pts = len(response["items"]["project"]["participant"])
            for pt in range(0, num_pts):
                pt_id_ext = response["items"]["project"]["participant"][pt]["participant_Id"]
                num_records = len(response["items"]["project"]["participant"][pt]["results"])
                for rec in range(0, num_records):
                    filename = response["items"]["project"]["participant"][pt]["results"][rec]["file_name"]
                    time_collected = response["items"]["project"]["participant"][pt]["results"][rec]["timestamp"]
                    main_df = pd.DataFrame([[pt_id_ext, filename, project_name, time_collected]] * num_records, columns=['pt_id_external', 'filename', 'project_name', 'time_collected'])
                    emotional_expressivity_summary_df = pd.read_json(json.dumps(response["items"]["project"]["participant"][pt]["results"][rec]["measures"]["emotional_expressivity_summary"][0]))
                    facial_expressivity_summary_df = pd.read_json(json.dumps(response["items"]["project"]["participant"][pt]["results"][rec]["measures"]["facial_expressivity_summary"][0]))
                    vocal_acoustics_summary_df = pd.read_json(json.dumps(response["items"]["project"]["participant"][pt]["results"][rec]["measures"]["vocal_acoustic_summary"][0]))
                    speech_characteristics_summary_df = pd.read_json(json.dumps(response["items"]["project"]["participant"][pt]["results"][rec]["measures"]["speech_characteristics_summary"][0]))
                    df = pd.concat([main_df, emotional_expressivity_summary_df, facial_expressivity_summary_df, vocal_acoustics_summary_df, speech_characteristics_summary_df], axis=1)
                    response_df = response_df._append(df, ignore_index=True)
        except Exception as ex:
            return None, f"{ex}"
        else:
            return response_df, None
