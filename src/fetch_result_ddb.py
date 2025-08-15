import json
import os

import boto3
from boto3.dynamodb.conditions import Key


def fetch_result_from_ddb(table_name: str):
    """
    Fetch results from a DynamoDB table.

    This function scans the specified DynamoDB table to collect unique model keys
    and queries for the latest result for each model using the GSI1 index.

    Args:
        table_name (str): The name of the DynamoDB table to fetch results from.

    Returns:
        dict: A dictionary containing the latest results for each unique model.
    """
    dynamodb = boto3.resource(
        "dynamodb"
    )  # Initialize DynamoDB resource to interact with AWS DynamoDB service
    table = dynamodb.Table(
        table_name
    )  # Access the specified table using the table name

    # Step 1: Scan to get all model keys (only GSI1PK) to identify unique models
    scan_resp = table.scan(ProjectionExpression="GSI1PK")

    # Collect unique models from GSI1PK values like "MODEL#gpt-4o"
    # This helps in identifying distinct models stored in the table
    unique_models = set()
    for item in scan_resp.get("Items", []):
        model_key = item.get("GSI1PK", "")
        if model_key.startswith("MODEL#"):
            unique_models.add(model_key.split("#")[1])

    # Step 2: Query for the latest result for each model to ensure we have the most recent data
    latest_results = {}
    for model in unique_models:
        # Query the table for the latest result for the current model using GSI1 index
        resp = table.query(
            IndexName="GSI1",
            KeyConditionExpression=Key("GSI1PK").eq(f"MODEL#{model}"),
            ScanIndexForward=False,  # Descending order to get the latest result
            Limit=1,
        )
        items = resp.get("Items", [])
        if items:
            # Remove "MODEL#" prefix before saving to keep model names clean
            model_name = items[0].get("GSI1PK", "").split("#")[1]
            latest_results[model_name] = items[0]

    return latest_results


def main():
    """
    Main function to fetch results from DynamoDB.

    This function retrieves the DynamoDB table name and result file path from environment
    variables. If these variables are not set, it raises a ValueError to ensure the user
    is aware of the missing configuration. It then calls the function to fetch results
    from the specified DynamoDB table and writes them to a JSON file.

    Raises:
        ValueError: If the 'DYNAMODB_TABLE_NAME' or 'RESULT_FILE_PATH' environment variable is not set.
    """
    # Retrieve the DynamoDB table name from the environment variable 'DYNAMODB_TABLE_NAME'.
    dynamodb_table_name = os.getenv("DYNAMODB_TABLE_NAME")
    result_file_path = os.getenv("RESULT_FILE_PATH")

    # Check if the table name is not set; if so, raise a ValueError to alert the user.
    if not dynamodb_table_name:
        raise ValueError("DYNAMODB_TABLE_NAME environment variable is not set")

    # Check if the result file path is not set; if so, raise a ValueError to alert the user.
    if not result_file_path:
        raise ValueError("RESULT_FILE_PATH environment variable is not set")

    # Call the function to fetch results from the specified DynamoDB table using the retrieved table name.
    models_results = fetch_result_from_ddb(dynamodb_table_name)

    # Write the unique models results to a JSON file for persistent storage and future reference
    with open(result_file_path, "w", encoding="utf-8") as f:
        json.dump(models_results, f, indent=2)


if __name__ == "__main__":
    main()
