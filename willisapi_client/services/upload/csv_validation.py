import pandas as pd
import numpy as np
import os
import pathlib
import re
from typing import Tuple

from willisapi_client.logging_setup import logger as logger
from willisapi_client.services.upload.language_choices import (
    LANGUAGE_CHOICES,
    English_us,
    SEX_CHOICES,
)
from dateutil.parser import parse, ParserError
from datetime import datetime


class CSVValidation:
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.expected_file_ext = "csv"
        self.study_id_ext = "study_id_ext"
        self.tags = "workflow"
        self.pt_id_external = "pt_id_external"
        self.upload_file_path = "file_path"
        self.language = "language"
        self.age = "age"
        self.sex = "sex"
        self.race = "race"
        self.arm = "arm"

        self.expected_headers = {
            self.study_id_ext,
            self.upload_file_path,
            self.tags,
            self.pt_id_external,
            self.language,
            self.age,
            self.sex,
            self.race,
            self.arm,
        }
        self.gender_field = ["M", "F"]
        self.workflow = [
            "vocal_acoustics_simple",
            "speech_characteristics",
            "speech_transcription_aws",
            "voice_and_speech",
            "facial_expressivity",
            "emotional_expressivity",
            "emotion_and_expressivity",
            "speaker_separation",
            "speech_characteristics_from_json",
            "eye_blink_rate",
            "rater_qa",
        ]
        self.dynamic_workflow = [
            "speech_transcription_aws_",
            "speaker_separation_",
            "scale_",
            "speech_characteristics_",
            "rater_qa_",
            "scale_wd_",
            "rater_qa_wd_",
        ]
        self.collect_time_format = r"^\d{4}-\d{2}-\d{2}$"
        self.df = None
        self.invalid_csv = "invalid csv input"

    def _is_valid(self) -> bool:
        """
        ------------------------------------------------------------------------------------------------------
        Class: CSVValidation

        Function: _is_valid

        Description: Checks the validity of input file

        Returns:
        ----------
        boolena: True/False based on input file validity
        ------------------------------------------------------------------------------------------------------
        """
        if not self._is_file():
            logger.error(self.invalid_csv)
            return False
        if not self._is_valid_file_ext():
            logger.error(self.invalid_csv)
            return False
        if not self._is_valid_headers():
            logger.error(self.invalid_csv)
            return False
        return True

    def _is_file(self) -> bool:
        """
        ------------------------------------------------------------------------------------------------------
        Class: CSVValidation

        Function: _is_file

        Description: Check if input is a file

        Returns:
        ----------
        boolena: True/False based on input file
        ------------------------------------------------------------------------------------------------------
        """
        return os.path.exists(self.file_path) and os.path.isfile(self.file_path)

    def _is_valid_file_ext(self) -> bool:
        """
        ------------------------------------------------------------------------------------------------------
        Class: CSVValidation

        Function: _is_valid_file_ext

        Description: Check if input is a valid CSV file

        Returns:
        ----------
        boolena: True/False based on valid input csv file
        ------------------------------------------------------------------------------------------------------
        """
        file_ext = self.file_path.split(".")[-1]
        if file_ext == self.expected_file_ext:
            return True
        return False

    def _is_valid_headers(self) -> bool:
        """
        ------------------------------------------------------------------------------------------------------
        Class: CSVValidation

        Function: _is_valid_headers

        Description: Check if input CSV has valid headers

        Returns:
        ----------
        boolena: True/False based on input CSV headers
        ------------------------------------------------------------------------------------------------------
        """
        df = pd.read_csv(self.file_path)
        df = df.replace({np.nan: None})
        headers = set(df.columns)
        if headers == self.expected_headers:
            self.df = df
            return True
        return False

    def _is_study_id_ext_valid(self, name: str) -> Tuple[bool, str]:
        """
        ------------------------------------------------------------------------------------------------------
        Class: CSVValidation

        Function: _is_study_id_ext_valid

        Description: Check if study_id_ext is empty

        Parameters:
        ----------
        name: name of the study

        Returns:
        ----------
        boolena: True/False based on valid study_id_ext
        error: A str error message if study is invalid
        ------------------------------------------------------------------------------------------------------
        """
        if name:
            return True, None
        return False, f"Invalid {self.study_id_ext} formatting"

    def _is_file_path_valid(self, file_path: str) -> Tuple[bool, str]:
        """
        ------------------------------------------------------------------------------------------------------
        Class: CSVValidation

        Function: _is_file_path_valid

        Description: Check if file path is valid

        Parameters:
        ----------
        file_path: A string of file path

        Returns:
        ----------
        boolena: True/False based on valid file_path
        error: A str error message if file_path is invalid
        ------------------------------------------------------------------------------------------------------
        """
        if file_path and os.path.exists(file_path) and os.path.isfile(file_path):
            return True, None
        return False, f"Invalid {file_path} formatting"

    def _is_workflow_valid(self, workflow: str) -> Tuple[bool, str]:
        """
        ------------------------------------------------------------------------------------------------------
        Class: CSVValidation

        Function: _is_workflow_valid

        Description: Check if workflow tags are valid

        Parameters:
        ----------
        workflow: A comma separated string of workflow tags

        Returns:
        ----------
        boolena: True/False based on valid workflow
        error: A str error message if workflow is invalid
        ------------------------------------------------------------------------------------------------------
        """
        tags = workflow.split(",")
        for tag in tags:
            if not (
                tag in self.workflow or tag.startswith(tuple(self.dynamic_workflow))
            ):
                return False, f"Invalid {self.tags} formatting"
        return True, None

    def _is_pt_id_external_valid(self, pt_id_ext: str) -> Tuple[bool, str]:
        """
        ------------------------------------------------------------------------------------------------------
        Class: CSVValidation

        Function: _is_pt_id_external_valid

        Description: Check if pt_id_external is not empty

        Parameters:
        ----------
        pt_id_ext: A string of pt_id_external

        Returns:
        ----------
        boolena: True/False based on valid pt_id_ext
        error: A str error message if pt_id_ext is invalid
        ------------------------------------------------------------------------------------------------------
        """
        if pt_id_ext:
            return True, None
        return False, f"Invalid {self.pt_id_external} formatting"

    def _is_language_valid(self, language: str) -> Tuple[bool, str]:
        """
        ------------------------------------------------------------------------------------------------------
        Class: CSVValidation

        Function: _is_language_valid

        Description: Check if language is valid

        Parameters:
        ----------
        language: A language string

        Returns:
        ----------
        boolean: True/False based on valid language code
        error: A str error message if langauge is invalid
        ------------------------------------------------------------------------------------------------------
        """
        # if language:
        return (True, None)
        # return (False, f"Invalid {self.language} formatting")

    def _is_age_valid(self, age: int):
        """
        ------------------------------------------------------------------------------------------------------
        Class: CSVValidation

        Function: _is_age_valid

        Description: Check if age is valid

        Parameters:
        ----------
        language: A Age Integer

        Returns:
        ----------
        boolean: True/False based on valid age
        error: A str error message if age is invalid
        ------------------------------------------------------------------------------------------------------
        """
        if age is None or age:
            return (True, None)
        return (False, f"Invalid {self.age} formatting")

    def _is_sex_valid(self, sex: str) -> Tuple[bool, str]:
        """
        ------------------------------------------------------------------------------------------------------
        Class: CSVValidation

        Function: _is_sex_valid

        Description: Check if sex is valid

        Parameters:
        ----------
        language: A Sex string

        Returns:
        ----------
        boolean: True/False based on valid sex
        error: A str error message if sex is invalid
        ------------------------------------------------------------------------------------------------------
        """
        if sex is None or sex:
            return (True, None)
        return (False, f"Invalid {self.sex} formatting")

    def _is_race_valid(self, race: str) -> Tuple[bool, str]:
        """
        ------------------------------------------------------------------------------------------------------
        Class: CSVValidation

        Function: _is_race_valid

        Description: Check if race is valid

        Parameters:
        ----------
        language: A Race String

        Returns:
        ----------
        boolean: True/False based on valid race
        error: A str error message if race is invalid
        ------------------------------------------------------------------------------------------------------
        """
        if race is None or race:
            return True, None
        return False, f"Invalid {self.race} formatting"

    def _is_arm_valid(self, arm: str) -> Tuple[bool, str]:
        """
        ------------------------------------------------------------------------------------------------------
        Class: CSVValidation

        Function: _is_arm_valid

        Description: Check if arm is valid

        Parameters:
        ----------
        language: A arm String

        Returns:
        ----------
        boolean: True/False based on valid arm
        error: A str error message if arm is invalid
        ------------------------------------------------------------------------------------------------------
        """
        if arm is None or arm:
            return True, None
        return False, f"Invalid {self.arm} formatting"

    def validate_row(self, row) -> Tuple[bool, str]:
        """
        ------------------------------------------------------------------------------------------------------
        Class: CSVValidation

        Function: validate_row

        Description: This function validates a row of a dataframe

        Parameters:
        ----------
        row: A row of a dataframe

        Returns:
        ----------
        boolena: True/False based on valid row
        error: A str error message if row is invalid
        ------------------------------------------------------------------------------------------------------
        """
        is_valid_study, error = self._is_study_id_ext_valid(row[self.study_id_ext])
        if error:
            return (is_valid_study, error)

        is_valid_file, error = self._is_file_path_valid(row[self.upload_file_path])
        if error:
            return (is_valid_file, error)

        is_valid_wft, error = self._is_workflow_valid(row[self.tags])
        if error:
            return (is_valid_wft, error)

        is_valid_pt_id, error = self._is_pt_id_external_valid(row[self.pt_id_external])
        if error:
            return (is_valid_pt_id, error)

        is_valid_language, error = self._is_language_valid(row[self.language])
        if error:
            return (is_valid_language, error)

        is_valid_age, error = self._is_age_valid(row[self.age])
        if error:
            return (is_valid_age, error)

        is_valid_sex, error = self._is_sex_valid(row[self.sex])
        if error:
            return (is_valid_sex, error)

        is_valid_race, error = self._is_race_valid(row[self.race])
        if error:
            return (is_valid_race, error)

        is_arm, error = self._is_arm_valid(row[self.arm])
        if error:
            return (is_arm, error)

        return True, None

    def get_filename(self) -> str:
        """
        ------------------------------------------------------------------------------------------------------
        Class: CSVValidation

        Function: get_filename

        Description: This function returns the name of the file

        Returns:
        ----------
        filename: filename of class object instance (str)
        ------------------------------------------------------------------------------------------------------
        """
        return pathlib.Path(self.file_path).name

    def get_dataframe(self):
        """
        ------------------------------------------------------------------------------------------------------
        Class: CSVValidation

        Function: get_dataframe

        Description: This function returns the dataframe

        Returns:
        ----------
        df: df of class object instance (pd.DataFrame)
        ------------------------------------------------------------------------------------------------------
        """
        return self.df
