import json
import os
import sys
from decimal import Decimal
from typing import Any

import boto3
from pydantic import BaseModel, Field


class ResultItem(BaseModel):
    """
    Represents a result item with core metadata and scores for DynamoDB storage.

    Attributes:
        run_id (str): The unique identifier for the run, must be a non-empty string.
        test_id (str): The unique identifier for the test, must be a non-empty string.
        start_time (str): The start time of the test, must be a non-empty string.
        end_time (str): The end time of the test, must be a non-empty string.
        duration (Decimal): The duration of the test in seconds, must be greater than 0.
        metric (str): The name of the metric used, must be a non-empty string.
        model (str): The name of the model used, must be a non-empty string.
        scores (dict[str, Any]): A dictionary containing flattened metric values.
        raw_data_file (str): The file path to the raw data, must be a non-empty string.
    """

    # Core keys from run_metadata
    run_id: str = Field(min_length=1)  # Ensures run_id is not empty
    test_id: str = Field(min_length=1)  # Ensures test_id is not empty
    start_time: str = Field(min_length=1)  # Ensures start_time is not empty
    end_time: str = Field(min_length=1)  # Ensures end_time is not empty
    duration: Decimal = Field(gt=0)  # Ensures duration is positive

    # Core keys from metadata
    metric: str = Field(min_length=1)  # Ensures metric name is not empty
    model: str = Field(min_length=1)  # Ensures model name is not empty

    # Flattened metric values (dynamic in original JSON, so use dict)
    scores: dict[str, Any] = Field(
        default_factory=dict
    )  # Initializes scores as an empty dictionary

    # Keep raw data if needed
    raw_data_file: str = Field(min_length=1)  # Ensures raw_data_file path is not empty


class Result:
    ignore_keys = [
        "individual_scores",
        "grading_criteria",
    ]  # Keys to ignore during processing

    def __init__(self, result_path: str):
        """
        Initialize the Result object with the path to the result file.

        Args:
            result_path (str): The file path to the JSON result file.
        """
        self.result_path = result_path  # Store the path for later use

    def _flatten_metric_result(
        self, metric_result_dict: dict, parent_key: str = "", sep: str = "_"
    ) -> dict:
        """
        Flatten a nested dictionary into a single level with compound keys.

        Args:
            metric_result_dict (dict): The dictionary to flatten.
            parent_key (str, optional): The prefix for the current dictionary level.
            sep (str, optional): Separator between keys.

        Returns:
            dict: Flattened dictionary.
        """
        flattened_dict = {}
        for key, value in metric_result_dict.items():
            new_key = (
                f"{parent_key}{sep}{key}" if parent_key else key
            )  # Create compound key
            if isinstance(value, dict):
                # Recursively flatten nested dictionaries
                flattened_dict.update(
                    self._flatten_metric_result(value, parent_key=new_key, sep=sep)
                )
            else:
                flattened_dict[new_key] = value  # Add non-dict values directly
        return flattened_dict

    def read_result_from_file(self) -> dict:
        """
        Read and parse the JSON result file.

        This method attempts to open the specified result file and load its
        contents as JSON. If the file is not found, contains invalid JSON,
        or any other error occurs, an appropriate error message is printed
        and the program exits.

        Returns:
            dict: The parsed JSON data from the result file.
        """
        try:
            with open(self.result_path, "r") as f:
                return json.load(f, parse_float=Decimal)  # Load JSON data from file
        except FileNotFoundError:
            print(f"Error: The file {self.result_path} was not found.")
            sys.exit(1)  # Exit if file is not found
        except json.JSONDecodeError:
            print(f"Error: The file {self.result_path} contains invalid JSON.")
            sys.exit(1)  # Exit if JSON is invalid
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            sys.exit(1)  # Exit for any other unexpected errors

    def format_result_for_dynamodb(self) -> list[ResultItem]:
        """
        Format the result data for storage in DynamoDB.

        This method reads the JSON result file, extracts relevant metadata and results,
        and formats them into a list of ResultItem objects. Each ResultItem encapsulates
        the necessary information for storage in DynamoDB, including run metadata,
        metric scores, and model details.

        Returns:
            list[ResultItem]: A list of ResultItem objects containing formatted data
            ready for DynamoDB storage.
        """
        data = self.read_result_from_file()  # Read data from file

        # Extract run_metadata and run_results for processing
        run_metadata = data.get("run_metadata", {})
        run_results = data.get("run_results", [])

        items: list[ResultItem] = []
        for result in run_results:
            metadata = result.get("metadata", {})
            results = result.get("results", {})

            # Extract metric name and model name to uniquely identify the test and the model used
            metric_name = metadata.get("metric", {}).get("name", "")
            model_name = metadata.get("connector", {}).get("model", "")

            scores_flat = {}
            eval_summary = results.get("evaluation_summary", {})
            for metric_key, metric_val in eval_summary.items():
                # Check if the metric_key is relevant to the current metric and ensure it's a dictionary
                if metric_key in metric_name and isinstance(metric_val, dict):
                    for metric_result_key, metric_result_val in metric_val.items():
                        # Skip keys that are meant to be ignored
                        if metric_result_key in Result.ignore_keys:
                            continue

                        # Flatten the metric result values for easier storage and retrieval
                        if isinstance(metric_result_val, dict):
                            flattened_scores = self._flatten_metric_result(
                                metric_result_val
                            )
                        else:
                            flattened_scores = metric_result_val

                        # Update the scores_flat dictionary with the flattened scores
                        scores_flat[metric_result_key] = flattened_scores

            # Create a ResultItem to encapsulate all relevant data for storage in DynamoDB
            item = ResultItem(
                run_id=run_metadata.get("run_id", ""),
                test_id=run_metadata.get("test_id", ""),
                start_time=run_metadata.get("start_time", ""),
                end_time=run_metadata.get("end_time", ""),
                duration=run_metadata.get("duration", Decimal(0)),
                metric=metric_name,
                model=model_name,
                scores=scores_flat,
                raw_data_file=self.result_path,
            )
            items.append(item)

        # Return the list of ResultItem objects for further processing or storage
        return items

    def write_result_to_dynamodb(
        self, dynamodb_result: list[ResultItem], table_name: str
    ):
        """
        Write formatted result items to a DynamoDB table.

        This method takes a list of ResultItem objects and writes them to the specified
        DynamoDB table using batch writing for efficiency. Each item is transformed into
        a dictionary suitable for DynamoDB storage, including partition and sort keys
        for indexing and retrieval.

        Args:
            dynamodb_result (list): A list of ResultItem objects to be stored in DynamoDB.
            table_name (str): The name of the DynamoDB table where the results will be stored.
        """
        dynamodb = boto3.resource("dynamodb")  # Initialize DynamoDB resource
        table = dynamodb.Table(table_name)  # Access the specified table

        with (
            table.batch_writer() as batch
        ):  # Use batch writer for efficient bulk writing
            for result_item in dynamodb_result:
                item_dict = result_item.model_dump()  # Convert ResultItem to dictionary

                # Construct partition key using run_id for unique identification
                pk = f"RUN#{item_dict['run_id']}"
                # Construct sort key using start_time and metric for time ordering
                sk = f"{item_dict['start_time']}#{item_dict['metric']}"

                # Construct GSI partition key using model for model-based queries
                gsi1pk = f"MODEL#{item_dict['model']}"
                # Construct GSI sort key using start_time for time-based queries
                gsi1sk = item_dict["start_time"]

                # Prepare the item for DynamoDB with necessary keys and attributes
                dynamo_item = {
                    "PK": pk,
                    "SK": sk,
                    "GSI1PK": gsi1pk,
                    "GSI1SK": gsi1sk,
                    **item_dict,  # Include all other attributes from the ResultItem
                }

                batch.put_item(Item=dynamo_item)  # Add item to batch for writing


def main():
    """
    Main function to execute the script.

    This function checks if the correct number of command-line arguments
    are provided. It expects a file path to a JSON result file as an argument.
    If the argument is missing, it prints the usage instructions and exits.
    It then creates a Result object with the provided file path and formats
    the result for DynamoDB.
    """
    if len(sys.argv) < 2:
        print("Usage: python write_result_ddb.py <result_path>")
        sys.exit(1)  # Exit if no file path is provided

    # Read the result from the file
    result_path = sys.argv[1]
    result = Result(result_path)

    # Format the result for DynamoDB
    dynamodb_result = result.format_result_for_dynamodb()

    # Write the result to DynamoDB
    dynamodb_table_name = os.getenv("DYNAMODB_TABLE_NAME")
    if not dynamodb_table_name:
        raise ValueError("DYNAMODB_TABLE_NAME environment variable is not set")
    result.write_result_to_dynamodb(dynamodb_result, dynamodb_table_name)


if __name__ == "__main__":
    main()
