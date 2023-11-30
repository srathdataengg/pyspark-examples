"""
Pyspark sql functions- aggregate.
Aggregate functions on a group of rows and give singular value
- approx_count_distinct - function returns count of distinct
-avg - it returns the average value of columns
-collect_list - returns all values from i/p column with duplicates
- collect_set - returns all values from i/p column after duplicates eliminated
countDistinct - returns the number of distinct columns
count
grouping
first
last
kurtosis
max
min
mean
skewness
stddev
stddev_samp
stddev_pop
sum
sumDistinct
variance, var_samp, var_pop
"""

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import approx_count_distinct,avg,collect_list,countDistinct
from pyspark.sql.functions import count,grouping,first,last,kurtosis,min,max
from pyspark.sql.functions import mean,skewness,stddev,stddev_pop,sum
from pyspark.sql.functions import sumDistinct,variance,var_samp,var_pop

spark = SparkSession.builder.appName("aggregate functions").getOrCreate()

simpleData = [("James", "Sales", 3000),
    ("Michael", "Sales", 4600),
    ("Robert", "Sales", 4100),
    ("Maria", "Finance", 3000),
    ("James", "Sales", 3000),
    ("Scott", "Finance", 3300),
    ("Jen", "Finance", 3900),
    ("Jeff", "Marketing", 3000),
    ("Kumar", "Marketing", 2000),
    ("Saif", "Sales", 4100)
  ]
schema = ["employee_name", "department", "salary"]

df = spark.createDataFrame(simpleData,schema)

print("approx count distinct:"+str(df.select(approx_count_distinct('salary')).collect()[0][0]))
