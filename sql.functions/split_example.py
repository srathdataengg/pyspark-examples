"""
split() can split a string into array, this can be done based on delimiter like spaces, comma
PySpark SQL split() is grouped under Array Functions in PySpark SQL Functions class with the below syntax.
pyspark.sql.functions.split(str,pattern,limit=-1)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split

spark = SparkSession.builder.appName("split example").getOrCreate()

_data = [("James, A, Smith", "2018", "M", 3000),
         ("Michael, Rose, Jones", "2010", "M", 4000),
         ("Robert,K,Williams", "2010", "M", 4000),
         ("Maria,Anne,Jones", "2005", "F", 4000),
         ("Jen,Mary,Brown", "2010", "", -1)
         ]
_cols = ["name", "dob", "gender", "salary"]

split_df = spark.createDataFrame(_data, _cols)
split_df.show()
split_df.printSchema()

split_df2 = split_df.select(split(col("name"), ",").alias("split_name")).drop("name")

split_df2.show()

split_df.createOrReplaceTempView("person")
spark.sql("select SPLIT(name,',') as split_name from person").show()

