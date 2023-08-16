import pandas as pd
import numpy as np
import os
import pathlib
import re
from typing import Tuple

from willisapi_client.services.exceptions import InvalidFileType, InvalidFilePath, InvalidCSVColumns
from willisapi_client.logging_setup import logger as logger

class CSVValidation():
    def __init__(self, file_path: str):
        self.expected_file_ext = 'csv'
        self.expected_headers = {'project_name', 'file_path', 'workflow_tags', 'pt_id_external', 'time_collected', 'data_type'}
        self.workflow_tags  = [
                                'vocal_acoustics',
                                'speech_characteristics',
                                'speech_transcription',
                                'voice_and_speech',
                                'facial_expressivity',
                                'emotional_expressivity',
                                'emotion_and_expressivity',
                                'speech_transcription_panss',
                                'scale_panss',
                                'speaker_separation',
                                'speech_characteristics_from_json'
                                ]
        self.collect_time_format = r'^\d{4}-\d{2}-\d{2}$'
        self.file_path = file_path
        self.df = None
    
    def _is_valid(self) -> bool:
        """
        Checks the validity of input file

        Returns:
            boolean
        """
        if not self._is_file(self.file_path):
            logger.error("Invalid CSV input")
            return False
        if not self._is_valid_file_ext(self.file_path):
            logger.error("Invalid CSV input")
            return False
        if not self._is_valid_headers(self.file_path):
            logger.error("Invalid CSV input")
            return False
        return True
    
    def _is_file(self, file_path: str) -> bool:
        """
        Check if input is a file

        Parameters:
            file_path: A string of file path
        Returns:
            boolean
        """
        return os.path.exists(file_path) and os.path.isfile(file_path)

    def _is_valid_file_ext(self, file_path: str) -> bool:
        """
        Check if input is a valid CSV file

        Parameters:
            file_path: A string of file path
        Returns:
            boolean
        """
        file_ext = file_path.split(".")[-1]
        if file_ext == self.expected_file_ext:
            return True
        return False
            
    def _is_valid_headers(self, file_path: str) -> bool:
        """
        Check if input CSV has valid headers

        Parameters:
            file_path: A string of file path
        Returns:
            boolean
        """
        df = pd.read_csv(file_path)
        df = df.replace({np.nan: None})
        headers = set(df.columns)
        if headers == self.expected_headers:
            self.df = df
            return True
        return False

    def _is_project_name_valid(self, name: str) -> Tuple[bool, str]:
        """
        Check if project_name is not empty

        Parameters:
            name: project name
        Returns:
            boolean, str
        """
        if name:
            return True, None
        return False, "Invalid Project Name"
    
    def _is_file_path_valid(self, file_path: str) -> Tuple[bool, str]:
        """
        Check if file path is not valid

        Parameters:
            file_path: A string of file path
        Returns:
            boolean, str
        """
        if file_path and os.path.exists(file_path) and os.path.isfile(file_path):
            return True, None
        return False, "Invalid File Path"

    def _is_workflow_tags_valid(self, workflow_tags: str) -> Tuple[bool, str]:
        """
        Check if workflow tags are valid

        Parameters:
            workflow_tags: A comma separated string of workflow tags
        Returns:
            boolean, str
        """
        tags = workflow_tags.split(",")
        if set(tags).issubset(set(self.workflow_tags)):
            return True, None
        return False, "Invalid Workflow Tags"
    
    def _is_pt_id_external_valid(self, pt_id_ext: str) -> Tuple[bool, str]:
        """
        Check if pt_id_external is not empty

        Parameters:
            pt_id_ext: A string to pt_id_external
        Returns:
            boolean, str
        """
        if pt_id_ext:
            return True, None
        return False, "Invalid pt_id_external"
    
    def _is_time_collected_valid(self, collect_time: str) -> Tuple[bool, str]:
        """
        Check if collect_time is valid

        Parameters:
            collect_time: A string to collect_time (YYYY-MM-DD)
        Returns:
            boolean, str
        """
        if collect_time == None:
            return True, None
        if collect_time and bool(re.match(self.collect_time_format, collect_time)):
            return True, None
        return False, "Invalid time_collected"
    
    def _is_data_type_valid(self, data_type: str) -> Tuple[bool, str]:
        """
        Check if data type is valid

        Parameters:
            data_type: A string representation of data types
        Returns:
            boolean, str
        """
        if data_type == None or data_type:
            return True, None
        return False, "Invalid data type"

    def validate_row(self, row) -> Tuple[bool, str]:
        """
        This function validates a row of a dataframe

        Parameters:
            row: this is row of a dataframe
        Returns:
            boolean, str
        """
        is_valid_project, error = self._is_project_name_valid(row.project_name)
        if error: return (is_valid_project, error) 

        is_valid_file, error = self._is_file_path_valid(row.file_path) 
        if error: return (is_valid_file, error)
        
        is_valid_wft, error = self._is_workflow_tags_valid(row.workflow_tags)
        if error: return (is_valid_wft, error)
        
        is_valid_pt_id, error = self._is_pt_id_external_valid(row.pt_id_external)
        if error: return (is_valid_pt_id, error)
        
        is_valid_collect_time, error = self._is_time_collected_valid(row.time_collected)
        if error: return (is_valid_collect_time, error)
        
        is_valid_datatype, error = self._is_data_type_valid(row.data_type)
        if error: return (is_valid_datatype, error)
        
        return True, None

    def get_filename(self, file_path: str) -> str:
        """
        This function returns the name of the file

        Parameters:
            file_path: A string representation of file path
        Returns:
            str
        """
        return pathlib.Path(file_path).name
