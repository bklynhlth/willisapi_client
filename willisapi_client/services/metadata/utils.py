import pandas as pd
import numpy as np
from typing import List, Dict, Any, Tuple
import json
import os
import hashlib
import base64
import requests
from urllib.parse import urlsplit
from .language_choices import (
    LANGUAGE_CHOICES,
)


ALLOWED_COA_NAMES = ["MADRS", "YMRS", "PHQ-9", "GAD-7", "HAM-D"]

COA_ITEM_COUNTS = {"MADRS": 10, "YMRS": 10, "PHQ-9": 9, "GAD-7": 7, "HAM-D": 17}

SUPPORTED_AUDIO_EXTENSIONS = [".mp3", ".wav", ".ogg"]

class MetadataValidation:
    REQUIRED_COLUMNS = [
        "study_id",
        "site_id",
        "participant_id",
        "visit_name",
        "visit_order",
        "coa_name",
        "coa_item_number",
        "coa_item_value",
        "file_path",
        "time_collected",
        "recording_order",
    ]

    OPTIONAL_COLUMNS = ["rater_id", "age", "sex", "race", "language"]

    def __init__(
        self,
        csv_path: str,
        force_upload: bool = False,
    ):
        """
        Initialize validator with CSV file path.

        Args:
            csv_path: Path to the CSV file
        """
        self.csv_path = csv_path
        self.df = None
        self.errors = []
        self.transformed_df = None
        self.force_upload = force_upload

    def validate_columns(self) -> bool:
        """
        Validate that all required columns are present.

        Returns:
            bool: True if validation passes, False otherwise
        """
        missing_cols = [
            col for col in self.REQUIRED_COLUMNS if col not in self.df.columns
        ]

        if missing_cols:
            self.errors.append(f"Missing required columns: {', '.join(missing_cols)}")
            return False
        return True

    def validate_data_types(self) -> bool:
        """
        Validate data types for key columns.

        Returns:
            bool: True if validation passes, False otherwise
        """
        valid = True

        # Check visit_order is numeric
        if not pd.api.types.is_numeric_dtype(self.df["visit_order"]):
            try:
                self.df["visit_order"] = pd.to_numeric(self.df["visit_order"])
            except:
                self.errors.append("visit_order must be numeric")
                valid = False

        # Check age is numeric (if present)
        if "age" in self.df.columns and not self.df["age"].isna().all():
            if not pd.api.types.is_numeric_dtype(self.df["age"]):
                try:
                    self.df["age"] = pd.to_numeric(self.df["age"], errors="coerce")
                except:
                    self.errors.append("age must be numeric")
                    valid = False

        # Check coa_item_number is numeric
        if not pd.api.types.is_numeric_dtype(self.df["coa_item_number"]):
            try:
                self.df["coa_item_number"] = pd.to_numeric(self.df["coa_item_number"])
            except:
                self.errors.append("coa_item_number must be numeric")
                valid = False

        # Check coa_item_value is numeric
        if not pd.api.types.is_numeric_dtype(self.df["coa_item_value"]):
            try:
                self.df["coa_item_value"] = pd.to_numeric(
                    self.df["coa_item_value"], errors="coerce"
                )
            except:
                self.errors.append("coa_item_value must be numeric")
                valid = False

        return valid

    def validate_coa_names(self) -> bool:
        """
        Validate that coa_name values are in the allowed list.

        Returns:
            bool: True if validation passes, False otherwise
        """
        # Convert to lowercase for comparison
        self.df["coa_name"] = self.df["coa_name"].str.strip()

        invalid_coa = self.df[~self.df["coa_name"].isin(ALLOWED_COA_NAMES)]

        if not invalid_coa.empty:
            invalid_values = invalid_coa["coa_name"].unique().tolist()
            self.errors.append(
                f"Invalid coa_name values found: {invalid_values}. "
                f"Allowed values are: {', '.join(ALLOWED_COA_NAMES)}"
            )
            return False
        return True

    def validate_recording_consistency(self) -> bool:
        """
        Validate that for each unique recording (grouped by study_id, site_id,
        participant_id, visit_name, coa_name, recording_order), all metadata
        columns remain consistent across rows (only coa_item_number and
        coa_item_value should vary).

        Returns:
            bool: True if validation passes, False otherwise
        """
        valid = True

        # Define grouping columns that identify a unique recording
        grouping_cols = [
            "file_path",
        ]

        # Define columns that should be consistent within a recording
        # (all columns except coa_item_number and coa_item_value)
        metadata_cols = [
            col
            for col in self.df.columns
            if col not in ["coa_item_number", "coa_item_value", "recording_order"]
        ]

        # Group by recording identifier
        for group_key, group_df in self.df.groupby(grouping_cols, dropna=False):
            # For each metadata column, check if all values are the same
            for col in metadata_cols:
                if col in grouping_cols:
                    continue  # Skip the grouping columns themselves

                # Get unique non-null values for this column in the group
                unique_values = group_df[col].dropna().unique()

                # Check if we have more than one unique value
                if len(unique_values) > 1:
                    valid = False
                    # Create a readable identifier for the recording
                    file_path = group_key[grouping_cols.index("file_path")]

                    self.errors.append(
                        f"Inconsistent values in column '{col}' for recording: {file_path}. "
                        f"Found values: {list(unique_values)}. All metadata columns must be "
                        f"identical within a single recording."
                    )

        return valid

    def load_and_validate(self) -> bool:
        """
        Load CSV and run all validations.

        Returns:
            bool: True if all validations pass, False otherwise
        """
        try:
            self.df = pd.read_csv(self.csv_path)
            self.df = self.df.replace({np.nan: None})
        except Exception as e:
            self.errors.append(f"Failed to load CSV: {str(e)}")
            return False

        # Strip whitespace from column names
        self.df.columns = self.df.columns.str.strip()

        validations = [
            self.validate_columns(),
            self.validate_data_types(),
            self.validate_coa_names(),
            self.validate_recording_consistency(),
        ]

        return all(validations)

    def transform_to_serializer_format(self) -> List[Dict[str, Any]]:
        """
        Transform CSV data to match BulkUploadSerializer format.
        Groups rows by unique assessment (study, site, participant, visit, coa)
        and creates actual_scores JSON structure.
        Missing items are filled with score of 0.

        Returns:
            List of dictionaries matching the serializer format
        """
        if self.df is None:
            raise ValueError("CSV not loaded. Call load_and_validate() first.")

        # Group by unique assessment identifiers
        grouping_cols = [
            "study_id",
            "site_id",
            "participant_id",
            "visit_name",
            "visit_order",
            "coa_name",
            "file_path",
            "recording_order",
        ]

        # Add optional columns that are present
        optional_present = [
            col for col in self.OPTIONAL_COLUMNS if col in self.df.columns
        ]

        results = []

        # First, identify the assessment-level groups (without recording_order)
        assessment_grouping_cols = [
            "study_id",
            "site_id",
            "participant_id",
            "visit_name",
            "coa_name",
        ]

        # Build a lookup of minimum recording_order per assessment
        min_recording_lookup = {}
        for assessment_key, assessment_df in self.df.groupby(
            assessment_grouping_cols, dropna=False
        ):
            min_recording_order = assessment_df["recording_order"].min()
            min_recording_lookup[assessment_key] = min_recording_order

        # Build a lookup of maximum recording_order per VISIT (not per COA)
        visit_grouping_cols = [
            "study_id",
            "site_id",
            "participant_id",
            "visit_name",
        ]
        max_recording_lookup = {}
        for visit_key, visit_df in self.df.groupby(visit_grouping_cols, dropna=False):
            max_recording_order = visit_df["recording_order"].max()
            max_recording_lookup[visit_key] = max_recording_order

        actual_scores_cache = {}
        results = []

        # Process ALL recordings (not just the first one)
        for group_key, group_df in self.df.groupby(grouping_cols, dropna=False):
            # Create base record
            record = {}
            for i, col in enumerate(grouping_cols):
                record[col] = group_key[i]

            # Add optional fields from first row of group
            first_row = group_df.iloc[0]
            for col in optional_present:
                if col not in ["time_collected"]:  # Exclude metadata
                    record[col] = first_row[col]

            # Get expected item count for this COA
            coa_name = record["coa_name"]
            expected_items = COA_ITEM_COUNTS.get(coa_name, 10)

            # Determine if this is the first and last recording for this assessment/visit
            assessment_key = (
                record["study_id"],
                record["site_id"],
                record["participant_id"],
                record["visit_name"],
                record["coa_name"],
            )

            visit_key = (
                record["study_id"],
                record["site_id"],
                record["participant_id"],
                record["visit_name"],
            )

            min_recording_order = min_recording_lookup[assessment_key]
            max_recording_order = max_recording_lookup[visit_key]
            is_first_recording = record["recording_order"] == min_recording_order
            is_last_recording = record["recording_order"] == max_recording_order

            # Add is_last_recording to the record
            record["is_last_recording"] = is_last_recording

            if assessment_key in actual_scores_cache:
                # Reuse cached actual_scores
                actual_scores = actual_scores_cache[assessment_key]
            else:
                # Create a dictionary to store item scores (only for first recording)
                item_scores = {}

                if is_first_recording:
                    # Extract item scores from CSV rows
                    for _, row in group_df.iterrows():
                        item_num = (
                            int(row["coa_item_number"])
                            if pd.notna(row["coa_item_number"])
                            else None
                        )
                        item_score = row["coa_item_value"]

                        if item_num is not None:
                            # Convert to int if not null, otherwise use None
                            if pd.notna(item_score):
                                item_scores[item_num] = int(item_score)
                            else:
                                item_scores[item_num] = None

                # Build sections with all expected items
                sections = []
                total_score = 0

                for item_num in range(1, expected_items + 1):
                    if is_first_recording:
                        # Use extracted scores (or None if missing)
                        item_score = item_scores.get(item_num, None)
                    else:
                        # Subsequent recordings have None for all items
                        item_score = None

                    total_score = total_score + (
                        item_score if item_score is not None else 0
                    )

                    section = {
                        "section_id": f"s{item_num:02d}",
                        "section_notes": None,
                        "items": [
                            {"item_id": f"i{item_num:02d}", "item_score": item_score}
                        ],
                    }
                    sections.append(section)

                actual_scores = {
                    "sections": sections,
                    "total_score": total_score if is_first_recording else 0,
                    "total_severity": None,
                }

                # Cache the actual_scores for this assessment
                actual_scores_cache[assessment_key] = actual_scores

            record["actual_scores"] = actual_scores
            results.append(record)

        return results

    def create_final_csv(self) -> pd.DataFrame:
        """
        Create a final grouped CSV with actual_scores as JSON string.

        Args:
            output_path: Path where the final CSV should be saved

        Returns:
            DataFrame containing the grouped data
        """
        transformed_data = self.transform_to_serializer_format()

        # Convert actual_scores dict to JSON string for CSV storage
        for record in transformed_data:
            record["actual_scores"] = json.dumps(record["actual_scores"])
            record["force_upload"] = self.force_upload

        self.transformed_df = pd.DataFrame(transformed_data)
        return self.transformed_df

    def get_errors(self) -> List[str]:
        """
        Get list of validation errors.

        Returns:
            List of error messages
        """
        return self.errors


class UploadUtils:
    def __init__(self, row):
        self.row = row

    def validate_row(self):
        if not os.path.exists(self.row.file_path):
            return (False, "File path does not exist")
        if self.row.language not in LANGUAGE_CHOICES:
            return (False, f"Invalid language: {self.row.language}")
        return (True, None)

    def validate_processed_data_row(self):
        if self.row.language not in LANGUAGE_CHOICES:
            return (False, f"Invalid language: {self.row.language}")
        return (True, None)

    def calculate_file_checksum(self, file_path: str) -> str:
        try:
            with open(file_path, "rb") as f:
                file_data = f.read()

            hash_bytes = hashlib.sha256(file_data).digest()
            base64_hash = base64.b64encode(hash_bytes).decode("utf-8")
            return base64_hash
        except Exception as e:
            raise RuntimeError(f"Failed to calculate checksum: {e}")

    def generate_payload(self) -> Dict[str, Any]:
        payload = {
            "study_id": self.row.study_id,
            "site_id": self.row.site_id,
            "rater_id": self.row.rater_id,
            "participant_id": self.row.participant_id,
            "age": self.row.age,
            "sex": self.row.sex,
            "race": self.row.race,
            "language": self.row.language,
            "visit_name": self.row.visit_name,
            "visit_order": int(self.row.visit_order),
            "coa_name": self.row.coa_name,
            "filename": os.path.basename(self.row.file_path),
            "force_upload": self.row.force_upload,
            "actual_scores": json.loads(self.row.actual_scores),
            "checksum": self.calculate_file_checksum(self.row.file_path),
            "recording_order": int(self.row.recording_order),
            "is_last_recording": self.row.is_last_recording,
        }
        return payload

    def generate_processed_payload(self, files: List[Dict[str, str]]) -> Dict[str, Any]:

        payload = {
            "study_id": self.row.study_id,
            "site_id": self.row.site_id,
            "rater_id": self.row.rater_id,
            "pt_id": self.row.pt_id,
            "language": self.row.language,
            "visit_id": self.row.visit_id,
            "visit_order": int(self.row.visit_order),
            "coa_id": self.row.coa_id,
            "filename": os.path.basename(self.row.recording),
            "actual_scores": json.loads(self.row.scores_actual),
            "files": files,
            "force_upload": self.row.force_upload,
        }
        return payload

    def post(
        self, api_key: str, url: str, headers: Dict[str, str], payload: Dict[str, Any]
    ) -> Dict[str, Any]:
        try:
            response = requests.post(url, headers=headers, json=payload)
            res_json = response.json()

        except Exception as ex:
            return {"upload_status": "Failed", "error": str(ex), "response": None}
        else:
            if response.status_code not in [200, 201]:
                return {"upload_status": "Failed", "error": res_json, "response": None}
            else:
                return {"upload_status": "Success", "response": res_json, "error": None}


class ProcessedMetadataValidation:
    REQUIRED_COLUMNS = [
        "study_id",
        "site_id",
        "pt_id",
        "visit_id",
        "visit_order",
        "coa_id",
        "timestamp",
        "recording",
        "recording_order",
        "workflow",
    ]

    OPTIONAL_COLUMNS = ["rater_id", "language"]

    def __init__(
        self,
        csv_path: str,
        force_upload: bool = False,
    ):
        """
        Initialize validator with CSV file path.

        Args:
            csv_path: Path to the CSV file
        """
        self.csv_path = csv_path
        self.df = None
        self.errors = []
        self.transformed_df = None
        self.force_upload = force_upload

    def validate_columns(self) -> bool:
        """
        Validate that all required columns are present.

        Returns:
            bool: True if validation passes, False otherwise
        """
        missing_cols = [
            col for col in self.REQUIRED_COLUMNS if col not in self.df.columns
        ]

        if missing_cols:
            self.errors.append(f"Missing required columns: {', '.join(missing_cols)}")
            return False
        return True

    def validate_data_types(self) -> bool:
        """
        Validate data types for key columns.

        Returns:
            bool: True if validation passes, False otherwise
        """
        valid = True

        # Check visit_order is numeric
        if not pd.api.types.is_numeric_dtype(self.df["visit_order"]):
            try:
                self.df["visit_order"] = pd.to_numeric(self.df["visit_order"])
            except:
                self.errors.append("visit_order must be numeric")
                valid = False

        # Check recording_order is numeric
        if not pd.api.types.is_numeric_dtype(self.df["recording_order"]):
            try:
                self.df["recording_order"] = pd.to_numeric(self.df["recording_order"])
            except:
                self.errors.append("recording_order must be numeric")
                valid = False

        return valid

    def validate_coa_names(self) -> bool:
        """
        Validate that coa_id values are in the allowed list.

        Returns:
            bool: True if validation passes, False otherwise
        """
        # Convert to lowercase for comparison
        self.df["coa_id"] = self.df["coa_id"].str.strip()

        invalid_coa = self.df[~self.df["coa_id"].isin(ALLOWED_COA_NAMES)]

        if not invalid_coa.empty:
            invalid_values = invalid_coa["coa_id"].unique().tolist()
            self.errors.append(
                f"Invalid coa_id values found: {invalid_values}. "
                f"Allowed values are: {', '.join(ALLOWED_COA_NAMES)}"
            )
            return False
        return True

    def load_and_validate(self) -> bool:
        """
        Load CSV and run all validations.

        Returns:
            bool: True if all validations pass, False otherwise
        """
        try:
            self.df = pd.read_csv(self.csv_path)
            self.df = self.df.replace({np.nan: None})
        except Exception as e:
            self.errors.append(f"Failed to load CSV: {str(e)}")
            return False

        # Strip whitespace from column names
        self.df.columns = self.df.columns.str.strip()

        validations = [
            self.validate_columns(),
            self.validate_data_types(),
            self.validate_coa_names(),
        ]

        return all(validations)

    def create_scores_json(self, row: pd.Series, coa_id: str) -> str:
        """Create scores JSON format based on COA type (MADRS=10 items, HAMD=17 items)."""
        # Determine number of items based on coa_id
        num_items = COA_ITEM_COUNTS.get(coa_id, 10)

        sections = []
        total_score = 0

        for i in range(1, num_items + 1):
            item_col = f"item_score_{i:02d}"
            item_score = None

            # Get item score if column exists and has a value
            if item_col in row.index and pd.notna(row[item_col]):
                item_score = (
                    int(row[item_col])
                    if isinstance(row[item_col], (int, float))
                    else None
                )
                if item_score is not None:
                    total_score += item_score

            section = {
                "section_id": f"s{i:02d}",
                "section_notes": None,
                "items": [{"item_id": f"i{i:02d}", "item_score": item_score}],
            }
            sections.append(section)

        scores_dict = {
            "sections": sections,
            "total_score": total_score,
            "total_severity": None,
        }

        return json.dumps(scores_dict)

    def create_final_csv(self) -> pd.DataFrame:
        """Concatenate multipart recordings grouped by pt_id, visit_id, visit_order, coa_id."""

        merged = []

        # Group by pt_id, visit_id, visit_order, coa_id
        grouping_cols = ["pt_id", "visit_id", "visit_order", "coa_id"]

        for rec_id, group in self.df.groupby(grouping_cols):
            grp = group.sort_values("recording_order")

            if len(grp) == 1:
                row0 = grp.iloc[0].copy()
                recording = row0["recording"]
                ext = os.path.splitext(recording)[1].lower() if recording else ""

                # Skip single recordings with unsupported file types
                if ext not in SUPPORTED_AUDIO_EXTENSIONS:
                    row0["recording_count"] = 1
                    row0["original_recordings"] = recording
                    row0["skip_upload"] = True
                    row0["skip_reason"] = (
                        f"Unsupported file type '{ext}'. "
                        f"Only {', '.join(SUPPORTED_AUDIO_EXTENSIONS)} are supported."
                    )
                    merged.append(row0)
                    continue

                row0["recording_count"] = 1
                row0["original_recordings"] = row0["recording"]
                row0["skip_upload"] = False
                row0["skip_reason"] = None
                merged.append(row0)
                continue

            parts = []
            bases = []
            unsupported_files = []

            # Download all parts and validate extensions
            for idx, (_, row) in enumerate(grp.iterrows()):
                uri = row["recording"]

                bucket, key = _get_bucket_n_key_path_from_s3url(uri)
                fname = os.path.basename(key)
                ext = os.path.splitext(fname)[1].lower()

                # Check if file extension is supported
                if ext not in SUPPORTED_AUDIO_EXTENSIONS:
                    unsupported_files.append(f"{fname} ({ext})")

                bases.append(os.path.splitext(fname)[0])

            # Skip if any files have unsupported extensions
            if unsupported_files:
                row0 = grp.iloc[0].copy()
                row0["recording_count"] = len(grp)
                row0["original_recordings"] = ", ".join(
                    [r["recording"] for _, r in grp.iterrows()]
                )
                row0["skip_upload"] = True
                row0["skip_reason"] = (
                    f"Unsupported file type(s): {', '.join(unsupported_files)}. "
                    f"Only {', '.join(SUPPORTED_AUDIO_EXTENSIONS)} are supported."
                )
                merged.append(row0)
                continue

            merged_name = "_".join(bases) + ".wav"

            row0 = grp.iloc[0].copy()
            orig_bucket, orig_key = _get_bucket_n_key_path_from_s3url(
                grp.iloc[0]["recording"]
            )
            merged_key = os.path.join(os.path.dirname(orig_key), merged_name)

            # Create new URI (no upload, logical merge only)
            new_uri = f"s3://{orig_bucket}/{merged_key}"

            # Use the first record (min recording_order) as base
            row0 = grp.iloc[0].copy()
            row0["recording"] = new_uri
            row0["recording_count"] = len(grp)
            row0["original_recordings"] = ", ".join(
                [r["recording"] for _, r in grp.iterrows()]
            )
            row0["skip_upload"] = False
            row0["skip_reason"] = None
            merged.append(row0)

        self.transformed_df = pd.DataFrame(merged)
        self.transformed_df["scores_actual"] = self.transformed_df.apply(
            lambda row: self.create_scores_json(row, row["coa_id"]), axis=1
        )

        item_score_cols = [
            col for col in self.transformed_df.columns if col.startswith("item_score_")
        ]

        self.transformed_df = self.transformed_df.drop(columns=item_score_cols)
        # add force_upload = force_upload for all rows of self.transformed_df
        self.transformed_df["force_upload"] = self.force_upload

        return self.transformed_df


def find_files_with_pattern(directory, search_pattern):
    matches = []
    if not os.path.exists(directory):
        return matches
    # Walk through directory
    for root, dirs, files in os.walk(directory):
        for file in files:
            # Check if pattern is in filename
            if search_pattern in file:
                filepath = os.path.join(root, file)
                matches.append(filepath)
    return matches


def _get_bucket_n_key_path_from_s3url(path):
    url_split = urlsplit(path)
    return url_split.netloc, url_split.path[1:]


def get_last_n_directories(filepath, n=3):
    result = None
    error = None
    try:
        parts = filepath.split(os.sep)
        last_parts = parts[-(n + 1) :]
        result = os.sep.join(last_parts)
    except Exception as e:
        error = "Could not extract container name and container version from file path"
    return result, error
