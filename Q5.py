from pyspark.sql.functions import monotonically_increasing_id

# Include another index column while preserving the dataset's integrity.

data = [ (1001, 5001, 200, "2025-01-01"), 
(1002, 5002, 450, "2025-01-02"), 
(1003, 5003, 300, "2025-01-03"), 
(1004, 5004, 150, "2025-01-04"), 
(1005, 5005, 500, "2025-01-05"), ]

columns = ["Customer_ID", "Product_ID", "Amount", "Date"] 

df = spark.createDataFrame(data=data, schema=columns)
# df.show()

df = df.coalesce(1).withColumn("index_col", monotonically_increasing_id())

df.show()