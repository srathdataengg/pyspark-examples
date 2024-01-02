"""_summary_
Simillar to SQL groupby clause Pyspark has groupBy() function is used to collect the identical data into groups on 
Dataframe and perform count,sum,avg,min,max functions on the grouped data.

# Syntax:-
DataFrame.groupBy(*cols)
or
DataFrame.groupby(*cols)

When we perform groupBy() on PySpark dataframe, it returns GroupedData object which contains below aggregate functions.
count() - Use groupBy() count() to return the number of rows for each group.
mean() - returns the mean of values for each group.
max() - returns the maximum of values for each group.
min() - returns the minimum of values for each group.
sum() - returns the total for values for each group.
avg() - returns the average of each group values
"""
# Imports library

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
simpleData = [("James","Sales","NY",90000,34,10000),
    ("Michael","Sales","NY",86000,56,20000),
    ("Robert","Sales","CA",81000,30,23000),
    ("Maria","Finance","CA",90000,24,23000),
    ("Raman","Finance","CA",99000,40,24000),
    ("Scott","Finance","NY",83000,36,19000),
    ("Jen","Finance","NY",79000,53,15000),
    ("Jeff","Marketing","CA",80000,25,18000),
    ("Kumar","Marketing","NY",91000,50,21000)
  ]
schema = ["employee_name","department","state","salary","age","bonus"]

spark = SparkSession.builder.appName("groupby examples").master("local[*]").getOrCreate()

simpleDF = spark.createDataFrame(data = simpleData,schema=schema)

simpleDF.show(truncate=False)
simpleDF.printSchema()

# groupBy() on department and find the sum of salary department wise using sum()

simpleDF.groupBy("department").sum("salary").show(truncate=False)

# calculate the number of employees department wise

simpleDF.groupBy("department").count().show()

# calculate the min salary in each department

simpleDF.groupby("department").min("salary").show()

# calculate the max salary in each department

simpleDF.groupby("department").max("salary").show()

# calculare the avg salary in each department

simpleDF.groupby("department").avg("salary").withColumnRenamed("avg(salary)","avg_salary").show()

# calculate the mean salary in each department

simpleDF.groupBy("department").mean("salary").withColumnRenamed("mean(salary)","mean_salary").show()

# Using multiple columns
# group by on department,state and find sum of salary and bonus and rename o/p columns

simpleDF.groupBy("department","state")\
.sum("salary","bonus")\
.withColumnRenamed("sum(salary)","salary_sum")\
.withColumnRenamed("sum(bonus)","bonus_sum")\
.show(truncate=False)

# Running more aggregates at a time.
# using agg() aggregare function we can calculate many aggregations at a time on a simgle statement using SQL functions sum(),avg(),min(),max() mean() etc .

simpleDF.groupBy("department")\
    .agg(sum("salary").alias("salary_sum")\
         ,max("salary").alias("salary_max")\
         ,min("salary").alias("salary_min")
         ).show(truncate=False)

# agg on multiple columns

simpleDF.groupBy("department","state")\
    .agg(sum("salary").alias("salary_sum")\
         ,sum("bonus").alias("bonus_sum")\
         ,max("salary").alias("salary_max")
         ).show()

# Using filter on aggregate data
# Simillar to SQL having clause. On PySpark you can use where() or filter()

simpleDF.groupBy("department")\
    .agg(sum("salary").alias("Salary_sum")\
         ,sum("bonus").alias("bonus_sum"))\
            .where(col("bonus_sum")>40000).show()

# PySpark SQL Group by count 

simpleDF.createOrReplaceTempView("EMP")

spark.sql("Select * from EMP").show()

sql_str ="select department,state,count(*) as count from EMP group by department,state"

spark.sql(sql_str).show(truncate=False)