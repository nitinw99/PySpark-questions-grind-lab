from pyspark.sql.functions import col


# Assume there is a JSON file in a S3 bucket. 
# Read it and get the outer and nested fields, then output the data in a parquet file in another S3 Bucket
'''

json_string = """
[
    {
        "id": 1,
        "name": "Alice",
        "department": "HR",
        "address": {
            "city": "New York",
            "state": "NY"
        }
    },
    {
        "id": 2,
        "name": "Bob",
        "department": "IT",
        "address": {
            "city": "San Francisco",
            "state": "CA"
        }
    },
    {
        "id": 3,
        "name": "Charlie",
        "department": "Finance",
        "address": {
            "city": "Chicago",
            "state": "IL"
        }
    }
]
"""

'''

input_path  = "s3://your-bucket/sample_data.json"

df = spark.read.json(input_path)

df_flattened = df.select(
            "id",
            "name",
            "department",
            "address.city",
            "address.state"
).withColumnRenamed( "city", "address_city").withColumnRenamed( "state", "address_state")

output_path  = "s3://your-bucket/output/"

df_flattened.write.mode("overwrite").parquet(output_path)

print("Successfully written.")