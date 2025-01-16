from pyspark.sql.functions import col

# Create DataFrame for products 

products = spark.createDataFrame([ (1, "Laptop"), (2, "Tablet"), 
(3, "Smartphone"), (4, "Monitor"), 
(5, "Keyboard") ], ["product_id", "product_name"]) 

# Create DataFrame for sales 

sales = spark.createDataFrame([ (101, 1, "2025-01-01"), (102, 3, "2025-01-02"), (103, 5, "2025-01-03") ], ["sale_id", "product_id", "sale_date"])


# products.show()
# sales.show()

# Finding products that have never been sold
df_joined = products.join(sales, on=products.product_id==sales.product_id, how="left").filter(sales.product_id.isNull())\
        .select(products.product_id, products.product_name)

df_joined.show()