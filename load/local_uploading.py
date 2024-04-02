import json
import pandas as pd
from pathlib import Path
from datetime import datetime


class LocalUploader:
    def upload_as_json(self, data, filename: str, time_stamp: bool = False):
        """
        Saves the given data as a JSON file at the specified filename.
        If time_stamp is True, appends a timestamp to the filename before saving.

        Args:
            data: The data to be saved as JSON.
            filename (str): The filename (including path) where the data will be saved.
                            If time_stamp is True, a timestamp is appended to the filename.
            time_stamp (bool): If True, appends a timestamp to the filename.
                               Default is False.

        This method is part of the **LocalUploader**.
        """
        # Convert string filename to Path object for easier manipulation
        filename = Path(filename)

        # Append timestamp if required
        if time_stamp:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M")
            filename = filename.with_name(
                f"{filename.stem}_{timestamp}{filename.suffix}")

        # Ensure the directory exists or create it
        filename.parent.mkdir(parents=True, exist_ok=True)

        # Write the JSON data to the specified file
        with open(filename, 'w', encoding='utf-8') as file:
            json.dump(data, file, ensure_ascii=False, indent=4)

    def upload_as_parquet(self, df: pd.DataFrame, filename: str):
        # Implement saving DataFrame as Parquet
        pass
