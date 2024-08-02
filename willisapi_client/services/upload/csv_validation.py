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
        self.project_name = "project_name"
        self.tags = "workflow_tags"
        self.pt_id_external = "pt_id_external"
        self.time_collected = "time_collected"
        self.upload_file_path = "file_path"
        self.language = "language"
        self.age = "age"
        self.sex = "sex"
        self.race = "race"
        self.study_arm = "study_arm"
        self.clinical_score_a = "clinical_score_a"
        self.clinical_score_b = "clinical_score_b"
        self.clinical_score_c = "clinical_score_c"
        self.clinical_score_d = "clinical_score_d"
        self.clinical_score_e = "clinical_score_e"
        self.expected_headers = {
            self.project_name,
            self.upload_file_path,
            self.tags,
            self.pt_id_external,
            self.time_collected,
            self.language,
            self.age,
            self.sex,
            self.race,
            self.study_arm,
            self.clinical_score_a,
            self.clinical_score_b,
            self.clinical_score_c,
            self.clinical_score_d,
            self.clinical_score_e,
        }
        self.gender_field = ["M", "F"]
        self.workflow_tags = [
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
        self.dynamic_workflow_tags = [
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

    def _is_project_name_valid(self, name: str) -> Tuple[bool, str]:
        """
        ------------------------------------------------------------------------------------------------------
        Class: CSVValidation

        Function: _is_project_name_valid

        Description: Check if project_name is empty

        Parameters:
        ----------
        name: name of the project

        Returns:
        ----------
        boolena: True/False based on valid project_name
        error: A str error message if project is invalid
        ------------------------------------------------------------------------------------------------------
        """
        if name:
            return True, None
        return False, f"Invalid {self.project_name} formatting"

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

    def _is_workflow_tags_valid(self, workflow_tags: str) -> Tuple[bool, str]:
        """
        ------------------------------------------------------------------------------------------------------
        Class: CSVValidation

        Function: _is_workflow_tags_valid

        Description: Check if workflow tags are valid

        Parameters:
        ----------
        workflow_tags: A comma separated string of workflow tags

        Returns:
        ----------
        boolena: True/False based on valid workflow_tags
        error: A str error message if workflow_tags is invalid
        ------------------------------------------------------------------------------------------------------
        """
        tags = workflow_tags.split(",")
        for tag in tags:
            if not (
                tag in self.workflow_tags
                or tag.startswith(tuple(self.dynamic_workflow_tags))
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

    def _is_time_collected_valid(self, collect_time: str) -> Tuple[bool, str]:
        """
        ------------------------------------------------------------------------------------------------------
        Class: CSVValidation

        Function: _is_time_collected_valid

        Description: Check if collect_time is valid

        Parameters:
        ----------
        collect_time: A string to collect_time (YYYY-MM-DD)

        Returns:
        ----------
        boolena: True/False based on valid collect_time
        error: A str error message if collect_time is invalid
        ------------------------------------------------------------------------------------------------------
        """
        date_pattern = re.compile(self.collect_time_format)
        if collect_time and date_pattern.match(collect_time):
            try:
                date = parse(collect_time)
            except ParserError:
                return False, f"Invalid {self.time_collected} formatting"
            else:
                if date > datetime.now():
                    return False, f"Invalid {self.time_collected} formatting"
        return True, None

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

    def _is_study_arm_valid(self, study_arm: str) -> Tuple[bool, str]:
        """
        ------------------------------------------------------------------------------------------------------
        Class: CSVValidation

        Function: _is_study_arm_valid

        Description: Check if study_arm is valid

        Parameters:
        ----------
        language: A study_arm String

        Returns:
        ----------
        boolean: True/False based on valid study_arm
        error: A str error message if study_arm is invalid
        ------------------------------------------------------------------------------------------------------
        """
        if study_arm is None or study_arm:
            return True, None
        return False, f"Invalid {self.study_arm} formatting"

    def _is_clinical_score_valid_a(self, clinical_score_a: str) -> Tuple[bool, str]:
        """
        ------------------------------------------------------------------------------------------------------
        Class: CSVValidation

        Function: _is_clinical_score_valid_a

        Description: Check if clinical_score_a is valid

        Parameters:
        ----------
        language: A clinical_score_a String

        Returns:
        ----------
        boolean: True/False based on valid clinical_score_a
        error: A str error message if clinical_score_a is invalid
        ------------------------------------------------------------------------------------------------------
        """
        if clinical_score_a is None or clinical_score_a:
            return True, None
        return False, f"Invalid {self.clinical_score_a} formatting"

    def _is_clinical_score_valid_b(self, clinical_score_b: str) -> Tuple[bool, str]:
        """
        ------------------------------------------------------------------------------------------------------
        Class: CSVValidation

        Function: _is_clinical_score_valid_b

        Description: Check if clinical_score_b is valid

        Parameters:
        ----------
        language: A clinical_score_b String

        Returns:
        ----------
        boolean: True/False based on valid clinical_score_b
        error: A str error message if clinical_score_b is invalid
        ------------------------------------------------------------------------------------------------------
        """
        if clinical_score_b is None or clinical_score_b:
            return True, None
        return False, f"Invalid {self.clinical_score_b} formatting"

    def _is_clinical_score_valid_c(self, clinical_score_c: str) -> Tuple[bool, str]:
        """
        ------------------------------------------------------------------------------------------------------
        Class: CSVValidation

        Function: _is_clinical_score_valid_c

        Description: Check if clinical_score_c is valid

        Parameters:
        ----------
        language: A clinical_score_c String

        Returns:
        ----------
        boolean: True/False based on valid clinical_score_c
        error: A str error message if clinical_score_c is invalid
        ------------------------------------------------------------------------------------------------------
        """
        if clinical_score_c is None or clinical_score_c:
            return True, None
        return False, f"Invalid {self.clinical_score_c} formatting"

    def _is_clinical_score_valid_d(self, clinical_score_d: str) -> Tuple[bool, str]:
        """
        ------------------------------------------------------------------------------------------------------
        Class: CSVValidation

        Function: _is_clinical_score_valid_d

        Description: Check if clinical_score_d is valid

        Parameters:
        ----------
        language: A clinical_score_d String

        Returns:
        ----------
        boolean: True/False based on valid clinical_score_d
        error: A str error message if clinical_score_d is invalid
        ------------------------------------------------------------------------------------------------------
        """
        if clinical_score_d is None or clinical_score_d:
            return True, None
        return False, f"Invalid {self.clinical_score_d} formatting"

    def _is_clinical_score_valid_e(self, clinical_score_e: str) -> Tuple[bool, str]:
        """
        ------------------------------------------------------------------------------------------------------
        Class: CSVValidation

        Function: _is_clinical_score_valid_e

        Description: Check if clinical_score_e is valid

        Parameters:
        ----------
        language: A clinical_score_e String

        Returns:
        ----------
        boolean: True/False based on valid clinical_score_e
        error: A str error message if clinical_score_e is invalid
        ------------------------------------------------------------------------------------------------------
        """
        if clinical_score_e is None or clinical_score_e:
            return True, None
        return False, f"Invalid {self.clinical_score_e} formatting"

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
        is_valid_project, error = self._is_project_name_valid(row[self.project_name])
        if error:
            return (is_valid_project, error)

        is_valid_file, error = self._is_file_path_valid(row[self.upload_file_path])
        if error:
            return (is_valid_file, error)

        is_valid_wft, error = self._is_workflow_tags_valid(row[self.tags])
        if error:
            return (is_valid_wft, error)

        is_valid_pt_id, error = self._is_pt_id_external_valid(row[self.pt_id_external])
        if error:
            return (is_valid_pt_id, error)

        is_valid_collect_time, error = self._is_time_collected_valid(
            row[self.time_collected]
        )
        if error:
            return (is_valid_collect_time, error)

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

        is_study_arm, error = self._is_study_arm_valid(row[self.study_arm])
        if error:
            return (is_study_arm, error)

        is_clinical_score_a, error = self._is_clinical_score_valid_a(
            row[self.clinical_score_a]
        )
        if error:
            return (is_clinical_score_a, error)

        is_clinical_score_b, error = self._is_clinical_score_valid_b(
            row[self.clinical_score_b]
        )
        if error:
            return (is_clinical_score_b, error)

        is_clinical_score_c, error = self._is_clinical_score_valid_c(
            row[self.clinical_score_c]
        )
        if error:
            return (is_clinical_score_c, error)

        is_clinical_score_d, error = self._is_clinical_score_valid_d(
            row[self.clinical_score_d]
        )
        if error:
            return (is_clinical_score_d, error)

        is_clinical_score_e, error = self._is_clinical_score_valid_e(
            row[self.clinical_score_e]
        )
        if error:
            return (is_clinical_score_e, error)

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
