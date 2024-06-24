from pyspark import SparkContext
from pyspark.sql import SparkSession


if __name__ == "__main__":
    sc = SparkContext.getOrCreate()
    spark = SparkSession(sc)


# this is an app placeholder
# modify this file to do the real stuff