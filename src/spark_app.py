from pyspark.sql import SparkSession

SparkSession.builder.getOrCreate().sparkContext.setLogLevel("ERROR")

#Sem zip
from enade_to_parquet import Enade2019ToParquet
from enade_question import EnadeQuestion
#Com zip
#from src.enade_to_parquet import Enade2019ToParquet
#from src.enade_to_parquet import Enade2019ToParquet
import sys

if __name__ == "__main__":
    spark = (SparkSession.builder.config(
        "spark.serializer",
        "org.apache.spark.serializer.KryoSerializer").getOrCreate())

    loadEnadeToParquet = Enade2019ToParquet(spark)
    loadEnadeQuestion = EnadeQuestion(spark)
    loadEnadeToParquet.run()
    loadEnadeQuestion.run()
    spark.stop()