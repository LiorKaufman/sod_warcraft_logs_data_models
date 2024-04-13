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
    report_ids = ['ar8BmXDxn3PFZMjz', 'vAtgBFNRQTD8dcrP', 'h2APtzJg6mZVWBvN', 'GWMHT1j9yXDba4rn', 'VWHZhd1YXAF4z6P8', '7npMHmzDKdQNk6BT', 'XrbgGMhqjDzQApnT', '6VAmHkbyw8Lv3aYN', '7kK2vcLnNAdGQRpa', 'gGmRVzJYfZLh1dKH', 'agjwF4Hph8MnfAJk', 'GKXHC3N1VWMfqJZd', 'B18qkvLw2T6GCdQP', '9AwV6vYKdzD1PFBc', 'VwRzpM9aBK1cfyHv', 'ga82zbjYHVCNTD7m', '1x93G4BvwDTJ7Mzn', 'TfbnKQyHgZtWGc2M', 'tf3NvCMXdkWgKRpG', 'kazQjRGJVF78Kwr3', 'AqVNjrfmCJGdWy6F', 'Q4F3yPBXVxH8RtjN', 'NBct4QDrafXCYLAj', 'hmDgGw618CrAW3RF', 'BqT9QPgkc8HAKWNy', 'AghjcQ1xpzqBZnm2', 'NC3LZHjWw2nRV1fJ', 'a26wHjd79pvVYF4X', 'bwvyzDrdg47qNaGt', 'kH9VCZ8gMBfJKpNd', 'L2cQKgqYyxWVM718', '8MWzNJjDHGhc1BnP', 'ZN6kX7AcWfFmBRQD', 'DqLdybRM194fpGnC', 'qK4yMCXTa1bwrNB7', '2LAZ6VDmkG8JCN47', 'qRkbXdYCmPH9QyAN',
                  'DFcjkQ8mKLzapMNH', 'VCHbQWYMA4xZLjKT', 'z9ahXfmQPyrwnLdH', 'A1bfy9zgHGMF6Cw8', 'JzmVkp46HYqR2L9A', 'Kr6MhZg1avmdfVJC', 'dV8gf6cG1v9Kbpay', 'DG48hHv3R72MbjzA', 'FPTXcqmkd9Kv7Wg4', 'f34vXP7NB9jxwKJ8', 'hWPTpYJGaCNmQnqd', 'k6MJWpcjyqY2FmHQ', 'TXtybc26mkqMVBNA', '2jD6QLA1XVf9vptq', 'yHaQj7fLqgDtX8Rd', 'z6X7cBV2wYGC9NZJ', 'c2ZfK7pdnwX1yLgB', 'hpJMBxRgTa9Pzk7t', 'XvkatTLr2gbQqGVm', 'LQdBgxptnYP3Aq9a', '2QmNGXwgKrtd3Ffk', 'Vgrd8yXDNpP7B2Wz', '3vKLRz1X74GDjnhT', 'wb4xZ173GVQpFNjX', '6RChLnMa4zADvPrN', 'AFm9dNJ4G3LK8P2f', 'VNzLm3xhPZGpX2Wq', '4Xr2gcKVJdn86QD3', 'ja6fDr9MbLR2d4TQ', 'TtX46dKDP3WVAyzg', 'Qf7MxBJkc2TmWYw3', 'Btjd9YF46A8gKp3T', 'D3NZCKFv6jzRdrBy', '6GvjfAqhQ8tykgwm', 'hZVQcWTtDvFdwM9Y', 'qtkTdK7M961ANh3r', 'TqFM1kjtb68DgLHv']
    print(f"Extracting data for {len(report_ids)}")

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
