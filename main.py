# Libs
import pandas as pd
from pathlib import Path

# Classes
from extract.warcraft_logs import WarcraftLogsAPI
from load.local_uploading import LocalUploader


def main():

    # Initialize the API extractor
    extractor = WarcraftLogsAPI()

    # Initialize the local uploader
    # ### TODO: Change this to be configurable by env variable to use local or gcs
    loader = LocalUploader()

    # List of report IDs to iterate over
    report_ids = ["hZVQcWTtDvFdwM9Y", "D3NZCKFv6jzRdrBy", "TtX46dKDP3WVAyzg"]

    # Directory to save the files
    save_dir = Path("./saved_reports")
    save_dir.mkdir(exist_ok=True)  # Create the directory if it doesn't exist

    for report_id in report_ids:
        # Fetch the report data using the extractor
        report_data = extractor.get_report_data_by_report_id(report_id)

        # JSON filename including report ID for identification
        json_filename = save_dir / f"report_{report_id}_data.json"
        loader.upload_as_json(report_data, json_filename, time_stamp=True)

        # Data contains nested objects
        df = pd.json_normalize(report_data)
        print(df.head(3))

        # Parquet filename
        # parquet_filename = save_dir / f"report_{report_id}_data.parquet"
        # Save the report data as Parquet
        # loader.upload_as_parquet(df, parquet_filename)


if __name__ == "__main__":
    main()
