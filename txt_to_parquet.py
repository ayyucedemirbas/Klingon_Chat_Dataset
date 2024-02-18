from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col

spark = SparkSession.builder.appName('txt_to_parquet').getOrCreate()

df = spark.read.text("klingon.txt")

df = df.withColumn("text", split(col("value"), ",")[0])\
       .drop("value")
df.write.parquet("klingon.parquet")
