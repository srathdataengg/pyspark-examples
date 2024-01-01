"""_summary_: - sample select() to slect single and multiple columns
    and collect() 
    """

from pyspark.sql import SparkSession
from pyspark.sql.types import StructField,StructType,StringType
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("select-collect example").master("local[*]").getOrCreate()

data = [("James","Smith","USA","CA"),
        ("Michael","Rose","USA","NY"),
        ("Robert","Williams","USA","CA"),
        ("Maria","Jones","USA","FL")]

schema = ["firstname","lastname","country","state"]

sample_df = spark.createDataFrame(data=data,schema=schema)

sample_df.show(truncate=False)

sample_df.select(col("firstname"),col("lastname")).show()

# select all columns

sample_df.select("*").show()
sample_df.select(col("*")).show()

# select columns by index
sample_df.select(sample_df.columns[:3]).show()

sample_df.select(sample_df.columns[2:4]).show()

data1 = [
        (("James",None,"Smith"),"OH","M"),
        (("Anna","Rose",""),"NY","F"),
        (("Julia","","Williams"),"OH","F"),
        (("Maria","Anne","Jones"),"NY","M"),
        (("Jen","Mary","Brown"),"NY","M"),
        (("Mike","Mary","Williams"),"OH","M")
        ]
schema1 = StructType([
    StructField('name',StructType([
        StructField("firstname",StringType(),True),
        StructField("middlename",StringType(),True),
        StructField("lastname",StringType(),True)
    ])),
    StructField("state",StringType(),True),
    StructField("gender",StringType(),True)
])

sample_df2 = spark.createDataFrame(data=data1,schema=schema1)

sample_df2.show(truncate=False)

sample_df2.select(col("name.firstname"),col("name.lastname")).show()

sample_df2.withColumn("firstname",col("name.firstname"))\
        .withColumn("middle_name",col("name.middlename")).show()

"""_summary_ :-PySpark RDD/DataFrame collect() is an action operation that is used to retrieve all the elements of the dataset 
(from all nodes) to the driver node. We should use 
the collect() on smaller dataset usually after filter(), group() e.t.c. Retrieving larger datasets results in OutOfMemory error.
    
 """

dept = [("Finance",10),
        ("Marketing",20),
        ("Sales",30),
        ("IT",40)]

dept_columns  =["department_name","department_id"]

dept_df = spark.createDataFrame(data=dept,schema=dept_columns)

dept_df.printSchema()
dept_df.show()

dept_collect_df = dept_df.collect()

print(dept_collect_df) # it retrieves the dataframe into an array of rowtype to the driver node.

# Note that now it is in array of rowtype - we can use for loop in python to retrieve the data.
for row in dept_collect_df:
    print(row['department_name'] + "," +str(row['department_id']))

# Returns the first row value
    
print("Return the 1st array")
print(dept_df.collect()[0][0])

print("Returns the 1st element of the array")
print(dept_df.collect()[0]) # returns the 1st element in an array(1st row)