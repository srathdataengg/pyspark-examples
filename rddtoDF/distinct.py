from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("distinct").getOrCreate()

# Prepare Data
data = [("James", "Sales", 3000),
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

# Create DataFrame
columns = ["employee_name", "department", "salary"]

df = spark.createDataFrame(data, columns)
df.printSchema()
df.show()

# Get distinct rows by comparing all columns
distinctDF = df.distinct()

distinctDF.show()
print("Distinct count :"+str(distinctDF.count()))

df2 = df.drop_duplicates()
print("Distinct ount :"+str(df2.count()))