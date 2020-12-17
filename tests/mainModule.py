import logging
from pyspark.sql import SparkSession
from pyspark import HiveContext
from pyspark import SparkConf
from pyspark import SparkContext
import pyspark
from pyspark.sql import SparkSession
import pytest
import shutil

def spark_session():
    return SparkSession.builder \
        .master('local[1]') \
        .appName('SparkByExamples.com') \
        .getOrCreate()


def test_create_table(spark_session):
    df = spark_session.createDataFrame([['one', 'two']]).toDF(*['first', 'second'])
    print(df.show())

    df2 = spark_session.createDataFrame([['one', 'two']]).toDF(*['first', 'second'])

    df.createOrReplaceTempView('sample')
    spark_session.sql("select * from sample").show()

    assert df.subtract(df2).count() == 0

test_create_table(spark_session())