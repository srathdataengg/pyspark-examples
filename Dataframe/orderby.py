"""
You can use both sort() and orderBy() function of PySpark Dataframe to sort columns in ascending and descending order
based on single or multiple columns. You can also do sorting based on Spark SQL sorting functions
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col,lit
from pyspark.sql.types import StructField,StructType,StringType

simpleData = [("James","Sales","NY",90000,34,10000),
              ("Michael","Sales","NY",86000,56,20000), \
            ("Robert","Sales","CA",81000,30,23000), \
    ("Maria","Finance","CA",90000,24,23000), \
    ("Raman","Finance","CA",99000,40,24000), \
    ("Scott","Finance","NY",83000,36,19000), \
    ("Jen","Finance","NY",79000,53,15000), \
    ("Jeff","Marketing","CA",80000,25,18000), \
    ("Kumar","Marketing","NY",91000,50,21000) ]

columns = ["employeename","department","state","salary","age","bonus"]

spark = SparkSession.builder.appName("sort-groupby-examples").master("local[*]").getOrCreate()

sample_df = spark.createDataFrame(data=simpleData,schema=columns)

sample_df.show(truncate=False)

sample_df.sort(col("salary").desc(),col("bonus").desc()).show()

sample_df.sort("department","state").show()

sample_df.orderBy(col("department").desc(),col("state").desc()).show()

# Using Raw SQL

sample_df.createOrReplaceTempView("EMP")
spark.sql("select employeename,department,state,salary,age,bonus from EMP order by department desc").show(truncate=False)
