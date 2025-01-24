import requests
import json
import time
import random
import pandas as pd
from typing import Tuple
from http import HTTPStatus


vocal_acoustics_simple_summary = "vocal_acoustics_simple_summary"
speech_characteristics_summary = "speech_characteristics_summary"
rater_qa_summary = "rater_qa_summary"


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
        except (
            requests.exceptions.ConnectionError,
            json.decoder.JSONDecodeError,
        ) as ex:
            if try_number == 3:
                raise
            time.sleep(random.random() * 2)
            return DownloadUtils.request(url, headers, try_number=try_number + 1)
        else:
            return res_json

    def _get_study_id_ext_and_pts_count(response):
        """
        ------------------------------------------------------------------------------------------------------
        Class: DownloadUtils

        Function: _get_study_id_ext_and_pts_count

        Description: Get study and participant count from json data

        Parameters:
        ----------
        response: The JSON response from the API server.

        Returns:
        ----------
        (study_id_ext, pt_count): Name of the study and number of participants of the study (str, int)
        ------------------------------------------------------------------------------------------------------
        """
        return (
            response["study"]["study_id"],
            len(response["study"]["participant"]),
        )

    def _get_pt_id_ext_and_num_coas(response, pt):
        """
        ------------------------------------------------------------------------------------------------------
        Class: DownloadUtils

        Function: _get_pt_id_ext_and_num_coas

        Description: Get participant id external and coas count from json data

        Parameters:
        ----------
        response: The JSON response from the API server.
        pt: Index of participant

        Returns:
        ----------
        (pt_id, coa_count): Id of the participant and number of coas of the participant (str, int)
        ------------------------------------------------------------------------------------------------------
        """
        return (
            response["study"]["participant"][pt]["participant_Id"],
            response["study"]["participant"][pt]["age"],
            response["study"]["participant"][pt]["sex"],
            response["study"]["participant"][pt]["race"],
            response["study"]["participant"][pt]["arm"],
            len(response["study"]["participant"][pt]["results"]),
        )

    def _get_filename(response, pt, rec):
        """
        ------------------------------------------------------------------------------------------------------
        Class: DownloadUtils

        Function: _get_filename

        Description: Get filename from json data

        Parameters:
        ----------
        response: The JSON response from the API server.
        pt: Index of participant
        rec: coa data from API server

        Returns:
        ----------
        (filename): filename of the coa (str, str)
        ------------------------------------------------------------------------------------------------------
        """
        return response["study"]["participant"][pt]["results"][rec]["file_name"]

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
        return [
            "study_id_ext",
            "pt_id_external",
            "filename",
            "language",
            "age",
            "sex",
            "race",
            "arm",
        ]

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
        rec: coa data from API server
        workflow_tag: Workflow Tag

        Returns:
        ----------
        df: A pandas dataframe of specific workflow tag
        ------------------------------------------------------------------------------------------------------
        """
        measures_dict = response["study"]["participant"][pt]["results"][rec]["measures"]
        if (
            workflow_tag in measures_dict
            and measures_dict[workflow_tag]
            and workflow_tag
            in [
                vocal_acoustics_simple_summary,
                speech_characteristics_summary,
                rater_qa_summary,
            ]
        ):
            return pd.read_json(json.dumps(measures_dict[workflow_tag][0]))
        return pd.DataFrame()

    def get_data_from_presigned_url(url: str):
        """
        ------------------------------------------------------------------------------------------------------
        Class: DownloadUtils

        Function: get_data_from_presigned_url

        Description: This function downloads the json data using S3 preisgned URL

        Parameters:
        ----------
        response: S3 Presigned URL

        Returns:
        ----------
        (data, error): Generates response data and error message
        ------------------------------------------------------------------------------------------------------
        """
        response = {}
        try:
            data = requests.get(url)
            if data.status_code == HTTPStatus.OK:
                response = data.json()
        except Exception:
            pass

        return response

    def generate_response_df(data) -> Tuple[pd.DataFrame, str]:
        """
        ------------------------------------------------------------------------------------------------------
        Class: DownloadUtils

        Function: generate_response_df

        Description: This function converts the json data to dataframe

        Parameters:
        ----------
        data: The JSON data from the API server.

        Returns:
        ----------
        (dataframe, error): Generates response dataframe and error message
        ------------------------------------------------------------------------------------------------------
        """
        response_df = pd.DataFrame()
        try:
            if not data:
                return response_df, "No Data Found"
            (study_id_ext, num_pts) = DownloadUtils._get_study_id_ext_and_pts_count(
                data
            )
            for pt in range(0, num_pts):
                (
                    pt_id_ext,
                    age,
                    sex,
                    race,
                    arm,
                    num_coas,
                ) = DownloadUtils._get_pt_id_ext_and_num_coas(data, pt)
                for rec in range(0, num_coas):
                    (filename) = DownloadUtils._get_filename(data, pt, rec)
                    main_df = pd.DataFrame(
                        [
                            [
                                study_id_ext,
                                pt_id_ext,
                                filename,
                                age,
                                sex,
                                race,
                                arm,
                            ]
                        ],
                        columns=DownloadUtils._get_defined_columns(),
                    )

                    vocal_acoustics_simple_summary_df = (
                        DownloadUtils._get_summary_df_from_json(
                            data, pt, rec, vocal_acoustics_simple_summary
                        )
                    )
                    speech_characteristics_summary_df = (
                        DownloadUtils._get_summary_df_from_json(
                            data, pt, rec, speech_characteristics_summary
                        )
                    )
                    rater_qa_summary_df = DownloadUtils._get_summary_df_from_json(
                        data, pt, rec, rater_qa_summary
                    )
                    df = pd.concat(
                        [
                            main_df,
                            vocal_acoustics_simple_summary_df,
                            speech_characteristics_summary_df,
                            rater_qa_summary_df,
                        ],
                        axis=1,
                    )
                    response_df = response_df._append(df, ignore_index=True)
            if "stats" in response_df.columns:
                response_df = response_df.drop(["stats"], axis=1)
        except Exception as ex:
            return None, f"{ex}"
        else:
            return response_df, None
