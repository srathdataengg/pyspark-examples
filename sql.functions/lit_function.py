"""PySpark SQL functions lit() and typedLit() are used to add a new column to DataFrame by assigning a literal or
constant value. Both these functions return Column type as return type. typedLit() provides a way to be explicit
about the data type of the constant value being added to a DataFrame, helping to ensure data consistency and type
correctness of PySpark workflows.

lit function to add constant column:
PySpark lit function to used to add consatnt or literal value as new column to the DataFrame
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when

spark = SparkSession.builder.master("local[*]").appName("lit function").getOrCreate()

_data = [("111", 50000), ("222", 34000), ("333", 60000)]

_schema = ["EmpId", "Salary"]

df = spark.createDataFrame(data=_data, schema=_schema)

df.printSchema()
df.show()

#  lit() usage

df = df.select(col("EmpID"), col("Salary"), lit("1").alias("lit_value"))
df.show()

df = df.select(col("EmpID"), col("Salary"), lit("NULL").alias("dummy_col"))
df.show()

"""
Example 2 : lit() function with with Column - to derive a new column based on certain condition
"""

df3 = df.withColumn("lit_value2",
                    when((col("Salary") >= 50000) & (col("Salary") <= 60000), lit(100)).otherwise(lit(200)))
df3.show(truncate=False)

df4 = df.withColumn("random_value",
                    when((col("EmpID") == 111), lit(10)).otherwise(2))
df4.show()


