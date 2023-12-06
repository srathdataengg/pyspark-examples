from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("interview").getOrCreate()

data = [('Genece', 2, 75000),
        ('Jaimin', 2, 80000),
        ('soumya', 2, 80000),
        ('Gene', 4, 85000),
        ('Kimberli', 4, 55000),
        ('Gabrielia', 4, 60000),
        ('Romanai', 2, 75000)
        ]
schema = "emp_name string,dept_id int,salary int"

df = spark.createDataFrame(data, schema)
df.show()

