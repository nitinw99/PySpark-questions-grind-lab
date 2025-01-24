from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Find the difference between the price on each day with it's previous day.

data = [ (1, "KitKat", 1000.0, "2021-01-01"), 
(1, "KitKat", 2000.0, "2021-01-02"), 
(1, "KitKat", 1000.0, "2021-01-03"),
(1, "KitKat", 2000.0, "2021-01-04"),
(1, "KitKat", 3000.0, "2021-01-05"), 
(1, "KitKat", 1000.0, "2021-01-06") ]

schema = ["IT_ID", "IT_Name", "Price", "PriceDate"]

df = spark.createDataFrame(data, schema)
# df.show()

windowSpec = Window.partitionBy(col("IT_ID")).orderBy("PriceDate")

df = df.withColumn("Prev_Price", lag("Price").over(windowSpec))

df = df.withColumn("Price_Diff", col("Price") - coalesce(col("Prev_Price"), lit(0)))
# df.show()
result_df = df.select("IT_ID", "IT_Name", "Price", "PriceDate", "Price_Diff")

result_df.show()