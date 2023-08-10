import pandas as pd
import numpy as np
import os
import pathlib
import re

from willisapi_client.services.exceptions import InvalidFileType, InvalidFilePath, InvalidCSVColumns

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
    
    def _is_valid(self):
        if not self._is_file(self.file_path):
            print("Input is not a file")
            return False
        if not self._is_valid_file_ext(self.file_path):
            print("Invalid CSV input")
            return False
        if not self._is_valid_headers(self.file_path):
            print("Invalid File Headers")
            return False
        return True
    
    def _is_file(self, file_path):
        return os.path.exists(file_path) and os.path.isfile(file_path)

    def _is_valid_file_ext(self, file_path):
        file_ext = file_path.split(".")[-1]
        if file_ext == self.expected_file_ext:
            return True
        return False
            
    def _is_valid_headers(self, file_path):
        df = pd.read_csv(file_path)
        df = df.replace({np.nan: None})
        headers = set(df.columns)
        if headers == self.expected_headers:
            self.df = df
            return True
        return False

    def _is_project_name_valid(self, name: str) -> bool:
        if name:
            return True
        return False
    
    def _is_file_path_valid(self, file_path: str) -> bool:
        if file_path and os.path.exists(file_path) and os.path.isfile(file_path):
            return True
        return False

    def _is_workflow_tags_valid(self, workflow_tags: str) -> bool:
        tags = workflow_tags.split(",")
        if set(tags).issubset(set(self.workflow_tags)):
            return True
        return False
    
    def _is_pt_id_external_valid(self, pt_id_ext: str) -> bool:
        if pt_id_ext:
            return True
        return False
    
    def _is_time_collected_valid(self, collect_time: str) -> bool:
        if collect_time == None:
            return True
        if collect_time and bool(re.match(self.collect_time_format, collect_time)):
            return True
        return False
    
    def _is_data_type_valid(self, data_type: str) -> bool:
        if data_type == None or data_type:
            return True
        return False

    def validate_row(self, row) -> bool:
        return (self._is_project_name_valid(row.project_name) and
        self._is_file_path_valid(row.file_path) and
        self._is_workflow_tags_valid(row.workflow_tags) and 
        self._is_pt_id_external_valid(row.pt_id_external) and 
        self._is_time_collected_valid(row.time_collected) and
        self._is_data_type_valid(row.data_type))

    def get_filename(self, file_path):
        return pathlib.Path(file_path).name
