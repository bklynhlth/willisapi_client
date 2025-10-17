import pandas as pd
from typing import List, Dict, Any, Tuple
import json
import os
import requests
from .language_choices import (
    LANGUAGE_CHOICES,
)


class MetadataValidation:
    REQUIRED_COLUMNS = [
        "study_id",
        "site_id",
        "rater_email",
        "participant_id",
        "visit_name",
        "visit_order",
        "coa_name",
        "coa_item_number",
        "coa_item_value",
        "file_path",
        "time_collected",
    ]

    OPTIONAL_COLUMNS = ["age", "sex", "race", "language"]

    ALLOWED_COA_NAMES = ["MADRS", "YMRS", "PHQ-9", "GAD-7"]

    COA_ITEM_COUNTS = {"MADRS": 10, "YMRS": 10, "PHQ-9": 9, "GAD-7": 7}

    def __init__(self, csv_path: str, force_upload: bool = False):
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

    def validate_email(self) -> bool:
        """
        Basic email validation for rater_email column.

        Returns:
            bool: True if validation passes, False otherwise
        """
        invalid_emails = self.df[~self.df["rater_email"].str.contains("@", na=False)]

        if not invalid_emails.empty:
            self.errors.append(
                f"Invalid email addresses found in rows: {invalid_emails.index.tolist()}"
            )
            return False
        return True

    def validate_coa_names(self) -> bool:
        """
        Validate that coa_name values are in the allowed list.

        Returns:
            bool: True if validation passes, False otherwise
        """
        # Convert to lowercase for comparison
        self.df["coa_name"] = self.df["coa_name"].str.strip()

        invalid_coa = self.df[~self.df["coa_name"].isin(self.ALLOWED_COA_NAMES)]

        if not invalid_coa.empty:
            invalid_values = invalid_coa["coa_name"].unique().tolist()
            self.errors.append(
                f"Invalid coa_name values found: {invalid_values}. "
                f"Allowed values are: {', '.join(self.ALLOWED_COA_NAMES)}"
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
        except Exception as e:
            self.errors.append(f"Failed to load CSV: {str(e)}")
            return False

        # Strip whitespace from column names
        self.df.columns = self.df.columns.str.strip()

        validations = [
            self.validate_columns(),
            self.validate_data_types(),
            self.validate_email(),
            self.validate_coa_names(),
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
            "rater_email",
            "participant_id",
            "visit_name",
            "visit_order",
            "coa_name",
            "file_path",
        ]

        # Add optional columns that are present
        optional_present = [
            col for col in self.OPTIONAL_COLUMNS if col in self.df.columns
        ]

        results = []

        for group_key, group_df in self.df.groupby(grouping_cols, dropna=False):
            # Create base record
            record = {}
            for i, col in enumerate(grouping_cols):
                record[col] = group_key[i]

            # Add optional fields from first row of group
            first_row = group_df.iloc[0]
            for col in optional_present:
                if col not in ["time_collected"]:  # Exclude metadata
                    value = first_row[col]
                    if pd.notna(value):
                        record[col] = value

            # Get expected item count for this COA
            coa_name = record["coa_name"]
            expected_items = self.COA_ITEM_COUNTS.get(coa_name, 10)

            # Create a dictionary to store item scores
            item_scores = {}

            for _, row in group_df.iterrows():
                item_num = int(row["coa_item_number"])
                item_score = row["coa_item_value"]

                # Convert to int if not null, otherwise use 0 as default
                if pd.notna(item_score):
                    item_scores[item_num] = int(item_score)
                else:
                    item_scores[item_num] = None

            # Build sections with all expected items (fill missing with 0)
            sections = []
            total_score = 0

            for item_num in range(1, expected_items + 1):
                item_score = item_scores.get(item_num, None)  # Default to 0 if missing
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
                "total_score": total_score,
                "total_severity": None,
            }

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

    def validate_row(self) -> Tuple[bool, str | None]:
        if not os.path.exists(self.row.file_path):
            return (False, "File path does not exist")
        if self.row.language not in LANGUAGE_CHOICES:
            return (False, f"Invalid language: {self.row.language}")
        return (True, None)

    def generate_payload(self) -> Dict[str, Any]:
        payload = {
            "study_id": self.row.study_id,
            "site_id": self.row.site_id,
            "rater_email": self.row.rater_email,
            "participant_id": self.row.participant_id,
            "age": self.row.age,
            "sex": self.row.sex,
            "race": self.row.race,
            "language": self.row.language,
            "visit_name": self.row.visit_name,
            "visit_order": int(self.row.visit_order),
            "coa_name": self.row.coa_name,
            "file_path": self.row.file_path,
            "filename": os.path.basename(self.row.file_path),
            "force_upload": self.row.force_upload,
            "actual_scores": json.loads(self.row.actual_scores),
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
