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
        
    def _get_project_name_and_pts_count(response):
        """
        Get project and participant count from json data
        """
        return (response["items"]["project"]["project_name"], len(response["items"]["project"]["participant"]))
    
    def _get_pt_id_ext_and_num_records(response, pt):
        """
        Get participant id external and records count from json data
        """
        return (response["items"]["project"]["participant"][pt]["participant_Id"], len(response["items"]["project"]["participant"][pt]["results"]))
    
    def _get_filename_and_timestamp(response, pt, rec):
        """
        Get filename and time_collected from json data
        """
        return (response["items"]["project"]["participant"][pt]["results"][rec]["file_name"], response["items"]["project"]["participant"][pt]["results"][rec]["timestamp"])
    
    def _get_defined_columns():
        """
        Get defined columns name
        """
        return ['pt_id_external', 'filename', 'project_name', 'time_collected']
    
    def _get_summary_df_from_json(response, pt, rec, workflow_tag):
        """
        Get summary dataframe of each workflow tag json data
        """
        measures_dict = response["items"]["project"]["participant"][pt]["results"][rec]["measures"]
        if workflow_tag in measures_dict:
            if measures_dict[workflow_tag]:
                return pd.read_json(json.dumps(measures_dict[workflow_tag][0]))
        return pd.DataFrame()

    def generate_response_df(response) -> Tuple[pd.DataFrame, str]:
        """
        This function converts the json data to dataframe
        """
        response_df = pd.DataFrame()
        try:
            if not response["items"]:
                return response_df
            (project_name, num_pts) = DownloadUtils._get_project_name_and_pts_count(response)
            for pt in range(0, num_pts):
                (pt_id_ext, num_records) = DownloadUtils._get_pt_id_ext_and_num_records(response, pt)
                for rec in range(0, num_records):
                    (filename, time_collected) = DownloadUtils._get_filename_and_timestamp(response, pt, rec)
                    main_df = pd.DataFrame([[pt_id_ext, filename, project_name, time_collected]] * num_records, columns=DownloadUtils._get_defined_columns())
                    emotional_expressivity_summary_df = DownloadUtils._get_summary_df_from_json(response, pt, rec, "emotional_expressivity_summary")
                    facial_expressivity_summary_df = DownloadUtils._get_summary_df_from_json(response, pt, rec, "facial_expressivity_summary")
                    vocal_acoustics_summary_df = DownloadUtils._get_summary_df_from_json(response, pt, rec, "vocal_acoustic_summary")
                    speech_characteristics_summary_df = DownloadUtils._get_summary_df_from_json(response, pt, rec, "speech_characteristics_summary")
                    df = pd.concat([main_df, emotional_expressivity_summary_df, facial_expressivity_summary_df, vocal_acoustics_summary_df, speech_characteristics_summary_df], axis=1)
                    response_df = response_df._append(df, ignore_index=True)
        except Exception as ex:
            return None, f"{ex}"
        else:
            return response_df, None
