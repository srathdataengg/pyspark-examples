# Create dataframe with struct using Row class
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col

data = [Row(name="James", prop=Row(hair="black", eye="blue")),
        Row(name="Ann", prop=Row(hair="grey", eye="black"))]

spark = SparkSession.builder.appName("Row class").master("local[*]").getOrCreate()
df = spark.createDataFrame(data)
df.show()
df.printSchema()

# Access Struct columns
df.select(df.prop.hair).show()
df.select(df["prop.hair"]).show()
df.select(col("prop.hair")).show()

# Access all columns from struct
df.select(col("prop.*")).show()

# PySpark Column Operators
# PySpark column also provides a wsy to do arithmetic operations on columns using operators

data = [(100, 2, 1), (200, 3, 4), (300, 4, 4)]
df = spark.createDataFrame(data).toDF("num1", "num2", "num3")

# Arithmetic Operations
df.select(df.num1 + df.num2).show()

data = [("James", "Bond", "100", None),
        ("Ann", "Varsa", "200", 'F'),
        ("Tom Cruise", "XXX", "400", ''),
        ("Tom Brand", None, "400", 'M')]

columns = ["fname", "lname", "id", "gender"]

df1 = spark.createDataFrame(data, columns)

df1.show()
df1.printSchema()

from pyspark.sql.functions import expr, when

# df1.select(df1.name.alias("firstname"),
# df1.lname.alias("lastname")).show()

df1.select(expr("fname||','|| lname").alias("fullname")).show()

# asc, desc to sort ascending and descending order respectively.
df1.sort(df1.fname.asc(), df1.lname.asc()).show()
df1.sort(df1.fname.desc()).show()

# cast() and astype() - used to convert the data type
df1.select(df1.fname, df1.id.cast("int")).printSchema()

# substr() - Returns a column after getting sub string from the column
df1.select(df1.fname.substr(1, 2)).alias("substr").show()

"""
when() and otherwise() - it is simillar to SQL case when 
executes sequence of expressions until it matches the condition and returns 
a value when match
"""
df1.select(df1.fname, df1.lname, \
           when(df1.gender == "M", "Males").when(df1.gender == "F", "Female").\
           when(df1.gender == None, "").otherwise(df1.gender).alias("new_gender")).show()
