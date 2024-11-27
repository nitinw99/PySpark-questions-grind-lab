from pyspark.sql.functions import *

data = [
    (10,20,11,20),
    (20, 11, 10,99),
    (10, 11, 20,  1),
    (30, 12, 20,99),
    (10, 11, 20, 20),
    (40, 13, 15,  3),
    (30, 8, 11, 99)
]
schema = "A int , B int , C int , D int"
df = spark.createDataFrame(data = data , schema = schema)

# display(df)


df_list = []

for col in df.columns:
  print("Processing column ", col)
  df_col = df.select(col).groupBy(col).count()
  df_col = df_col.withColumnRenamed("count", f"count_{col}")
  # df_col.show()
  df_list.append(df_col)

# df_list


# Method 1

from pyspark.sql.window import Window

windowSpec = Window.partitionBy().orderBy(lit(1))
df_list = [df.withColumn("rn", row_number().over(windowSpec)) for df in df_list]

# df_list[0].show()


# Method 2
df_list = [df.withColumn("rn", monotonically_increasing_id()) for df in df_list]

# df_list[0].show()

# Going with Method 1

df_final = df_list[0]
for df in df_list[1:]:
  df_final = df_final.join(df, "rn", "outer")

df_final = df_final.drop("rn")

df_final.show()