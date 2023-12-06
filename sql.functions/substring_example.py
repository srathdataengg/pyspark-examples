"""
substring function to extract the substring from a string column
by providing position and length of the string.

substring(str,pos,len) pyspark.sql.functions.substring
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import substr, substring, col

spark = SparkSession.builder.appName("substring").getOrCreate()

data = [(1, "20200828"), (2, "20180525")]
columns = ["id", "date"]
df = spark.createDataFrame(data, columns)
df.show()
df.withColumn("year", substring("date", 1, 4)) \
    .withColumn("month", substring("date", 5, 6)) \
    .withColumn("date", substring("date", 7, 8)).show(truncate=False)

# using select

df.select('date', substring('date', 1, 4).alias("year") \
          , substring('date', 5, 6).alias('month') \
          , substring('date', 7, 8).alias('date')).show()
