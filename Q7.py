from pyspark.sql.functions import col, countDistinct, sum
import time

# Perform aggregation on product_id column and there is skewness on this column. Try to optimize that as well and compare the time before and after aggregations.

data = [ (1, 1001, 'A', 10), (2, 1002, 'A', 15), (3, 1003, 'B', 30), (4, 1004, 'C', 25), (5, 1005, 'A', 20), (6, 1006, 'B', 40), (7, 1007, 'D', 50), (8, 1008, 'C', 15), (9, 1009, 'A', 10), (10, 1010, 'A', 30) ]

schema = ["transaction_id", "user_id", "product_id", "purchase_amount"]

df = spark.createDataFrame(data=data, schema=schema)

df.show()

print(f"Initial number of partitions: {df.rdd.getNumPartitions()}")
start_time = time.time()

# df.select("product_id").distinct().count()
# 4 Distinct values
df_repartitioned = df.repartition(4, "product_id")
# df_repartitioned.show()

result = df_repartitioned.groupBy("product_id").agg(sum(col("purchase_amount")), countDistinct(col("user_id"))).withColumnRenamed("sum(purchase_amount)", "total_purchase_amt").withColumnRenamed("count(user_id)", "total_users_bought")

result.show()

end_time = time.time()

total_time = end_time - start_time
total_time_inSecs = int(total_time)
print(total_time_inSecs)