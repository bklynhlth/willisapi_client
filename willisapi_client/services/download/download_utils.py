import requests
import json
import time
import random
import pandas as pd
from typing import Tuple

emotional_expressivity_summary = "emotional_expressivity_summary"
facial_expressivity_summary = "facial_expressivity_summary"
vocal_acoustic_summary = "vocal_acoustic_summary"
speech_characteristics_summary = "speech_characteristics_summary"

class DownloadUtils:

    def request(url, headers, try_number):
        """
        ------------------------------------------------------------------------------------------------------
        Class: DownloadUtils

        Function: request

        Description: This is an internal download function which makes a GET API call to brooklyn.health API server

        Parameters:
        ----------
        url: The URL of the API endpoint.
        headers: The headers to be sent in the request.
        try_number: The number of times the function has been tried.

        Returns:
        ----------
        json: The JSON response from the API server.
        ------------------------------------------------------------------------------------------------------
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
        ------------------------------------------------------------------------------------------------------
        Class: DownloadUtils

        Function: _get_project_name_and_pts_count

        Description: Get project and participant count from json data

        Parameters:
        ----------
        response: The JSON response from the API server.

        Returns:
        ----------
        (project_name, pt_count): Name of the project and number of participants of the project (str, int)
        ------------------------------------------------------------------------------------------------------
        """
        return (response["items"]["project"]["project_name"], len(response["items"]["project"]["participant"]))
    
    def _get_pt_id_ext_and_num_records(response, pt):
        """
        ------------------------------------------------------------------------------------------------------
        Class: DownloadUtils

        Function: _get_pt_id_ext_and_num_records

        Description: Get participant id external and records count from json data

        Parameters:
        ----------
        response: The JSON response from the API server.
        pt: Index of participant

        Returns:
        ----------
        (pt_id, record_count): Id of the participant and number of records of the participant (str, int)
        ------------------------------------------------------------------------------------------------------
        """
        return (response["items"]["project"]["participant"][pt]["participant_Id"], len(response["items"]["project"]["participant"][pt]["results"]))
    
    def _get_filename_and_timestamp(response, pt, rec):
        """
        ------------------------------------------------------------------------------------------------------
        Class: DownloadUtils

        Function: _get_filename_and_timestamp
        
        Description: Get filename and time_collected from json data

        Parameters:
        ----------
        response: The JSON response from the API server.
        pt: Index of participant
        rec: Record data from API server

        Returns:
        ----------
        (filename, timestamp): filename and timestamp of the record (str, str)
        ------------------------------------------------------------------------------------------------------
        """
        return (response["items"]["project"]["participant"][pt]["results"][rec]["file_name"], response["items"]["project"]["participant"][pt]["results"][rec]["timestamp"])
    
    def _get_defined_columns():
        """
        ------------------------------------------------------------------------------------------------------
        Class: DownloadUtils

        Function: _get_defined_columns

        Description: Get defined columns name

        Returns:
        ----------
        column_names: A list of static column names of the dataframe
        ------------------------------------------------------------------------------------------------------
        """
        return ['project_name', 'pt_id_external', 'filename', 'time_collected']
    
    def _get_summary_df_from_json(response, pt, rec, workflow_tag):
        """
        ------------------------------------------------------------------------------------------------------
        Class: DownloadUtils

        Function: _get_summary_df_from_json

        Description: Get summary dataframe of each workflow tag json data
        
        Parameters:
        ----------
        response: The JSON response from the API server.
        pt: Index of participant
        rec: Record data from API server
        workflow_tag: Workflow Tag

        Returns:
        ----------
        df: A pandas dataframe of specific workflow tag
        ------------------------------------------------------------------------------------------------------
        """
        measures_dict = response["items"]["project"]["participant"][pt]["results"][rec]["measures"]
        if workflow_tag in measures_dict:
            if measures_dict[workflow_tag]:
                if workflow_tag in [emotional_expressivity_summary, facial_expressivity_summary]:
                    return pd.read_json(json.dumps(measures_dict[workflow_tag][0])).head(1)
                if workflow_tag in [vocal_acoustic_summary, speech_characteristics_summary]:
                    return pd.read_json(json.dumps(measures_dict[workflow_tag][0]))                      
        return pd.DataFrame()

    def generate_response_df(response) -> Tuple[pd.DataFrame, str]:
        """
        ------------------------------------------------------------------------------------------------------
        Class: DownloadUtils

        Function: generate_response_df

        Description: This function converts the json data to dataframe
        
        Parameters:
        ----------
        response: The JSON response from the API server.

        Returns:
        ----------
        (dataframe, error): Generates response dataframe and error message
        ------------------------------------------------------------------------------------------------------
        """
        response_df = pd.DataFrame()
        try:
            if not response["items"]:
                return response_df, "No Data Found"
            (project_name, num_pts) = DownloadUtils._get_project_name_and_pts_count(response)
            for pt in range(0, num_pts):
                (pt_id_ext, num_records) = DownloadUtils._get_pt_id_ext_and_num_records(response, pt)
                for rec in range(0, num_records):
                    (filename, time_collected) = DownloadUtils._get_filename_and_timestamp(response, pt, rec)
                    main_df = pd.DataFrame([[project_name, pt_id_ext, filename, time_collected]], columns=DownloadUtils._get_defined_columns())
                    emotional_expressivity_summary_df = DownloadUtils._get_summary_df_from_json(response, pt, rec, emotional_expressivity_summary)
                    facial_expressivity_summary_df = DownloadUtils._get_summary_df_from_json(response, pt, rec, facial_expressivity_summary)
                    vocal_acoustics_summary_df = DownloadUtils._get_summary_df_from_json(response, pt, rec, vocal_acoustic_summary)
                    speech_characteristics_summary_df = DownloadUtils._get_summary_df_from_json(response, pt, rec, speech_characteristics_summary)
                    df = pd.concat([main_df, emotional_expressivity_summary_df, facial_expressivity_summary_df, vocal_acoustics_summary_df, speech_characteristics_summary_df], axis=1)
                    response_df = response_df._append(df, ignore_index=True)
            response_df = response_df.drop(['stats'], axis=1)
        except Exception as ex:
            return None, f"{ex}"
        else:
            return response_df, None
