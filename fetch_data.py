import boto3
from decimal import Decimal
import json
import os

# Get environment variables from GitHub Actions secrets
REGION = os.getenv("AWS_DEFAULT_REGION", "ap-southeast-1")
TABLE_NAME = os.getenv("DYNAMODB_TABLE", "FoodReviews")

dynamodb = boto3.resource("dynamodb", region_name=REGION)
table = dynamodb.Table(TABLE_NAME)

# Fetch all items (for large tables use pagination or queries)
response = table.scan()
items = response.get("Items", [])

# Sort by rating (descending) and pick top 10
items = sorted(items, key=lambda x: x["rating"], reverse=True)[:10]

# Convert Decimal to float for JSON
def decimal_default(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError

# Ensure data folder exists
os.makedirs("data", exist_ok=True)

with open("data/top_foods.json", "w") as f:
    json.dump(items, f, default=decimal_default, indent=2)

print("✅ top_foods.json updated successfully")
