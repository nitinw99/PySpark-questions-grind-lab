from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Remove the duplicate rows based on the composite key (customer_id, order_id) and 
# retain only the row with the latest order_date for each combination of customer_id and order_id.

data = [ (101, 1001, "2025-01-15", 500.00), (102, 1002, "2025-01-14", 300.00), (101, 1001, "2025-01-17", 550.00), (103, 1003, "2025-01-16", 450.00), 
(102, 1002, "2025-01-18", 320.00), (103, 1003, "2025-01-19", 460.00) ] 

schema = ["customer_id", "order_id", "order_date", "total_amount"] 

df = spark.createDataFrame(data, schema)
# df.show()

df = df.withColumn("order_date", F.to_date(df["order_date"], "yyyy-MM-dd"))
window_spec = Window.partitionBy("customer_id", "order_id").orderBy(F.col("order_date").desc())

df_new = df.withColumn("rn", F.row_number().over(window_spec))

result_df = df_new.filter(F.col("rn")==1).drop(F.col("rn"))

result_df.show()