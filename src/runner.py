from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql import HiveContext
from pyspark.sql import SparkSession
import findspark
findspark.init()
from pyspark.sql import Row
from pyspark.sql.types import StringType, ArrayType
from pyspark.sql.functions import udf, col, max as max, to_date, date_add, \
    add_months
from datetime import datetime, timedelta
import os
from os.path import join, abspath
from typing import Optional
import logging
import random
import string
import math
import mathOperations as mo
import UsedFunctions as uf
import conf.variables as v
import conf.configs as c

class someClass:

 def main():
  rec = {}

  spark = SparkSession.builder \
          .appName("app1") \
          .enableHiveSupport() \
          .getOrCreate()
  # Hive settings
  settings = [
      ("hive.exec.dynamic.partition", "true"),
      ("hive.exec.dynamic.partition.mode", "nonstrict"),
      ("spark.sql.orc.filterPushdown", "true"),
      ("hive.msck.path.validation", "ignore"),
      ("spark.sql.caseSensitive", "true"),
      ("spark.speculation", "false"),
      ("hive.metastore.authorization.storage.checks", "false"),
      ("hive.metastore.client.connect.retry.delay", "5s"),
      ("hive.metastore.client.socket.timeout", "1800s"),
      ("hive.metastore.connect.retries", "12"),
      ("hive.metastore.execute.setugi", "false"),
      ("hive.metastore.failure.retries", "12"),
      ("hive.metastore.schema.verification", "false"),
      ("hive.metastore.schema.verification.record.version", "false"),
      ("hive.metastore.server.max.threads", "100000"),
      ("hive.metastore.authorization.storage.checks", "/apps/hive/warehouse")
  ]
  spark.sparkContext._conf.setAll(settings)

  sc = SparkContext.getOrCreate()
  print(sc.getConf().getAll())
  sqlContext = SQLContext(sc)
  ##HiveContext = HiveContext(sc)
  lst = (spark.sql("SELECT FROM_unixtime(unix_timestamp(), 'dd/MM/yyyy HH:mm:ss.ss') ")).collect()
  print("\nStarted at");uf.println(lst)

  ##global numRows
  numRows = 10   ## do in increment of 50K rows otherwise you blow up driver memory!
  #
  ## Check if table exist otherwise create it

  rows = 0
  sqltext  = ""
  if (spark.sql(f"""SHOW TABLES IN {v.DB} like '{v.tableName}'""").count() == 1):
    rows = spark.sql(f"""SELECT COUNT(1) FROM {v.fullyQualifiedTableName}""").collect()[0][0]
    print ("number of rows is ",rows)
  else:
    print(f"\nTable {v.fullyQualifiedTableName} does not exist, creating table ")
    sqltext = f"""
    CREATE TABLE {v.DB}.{v.tableName}(
    ID INT
    , CLUSTERED INT
    , SCATTERED INT
    , RANDOMISED INT
    , RANDOM_STRING VARCHAR(50)
    , SMALL_VC VARCHAR(50)
    , PADDING  VARCHAR(4000)
    )
    STORED AS PARQUET
    """
    spark.sql(sqltext)

  start = 0
  if (rows == 0):
    start = 1
    maxID= 0
  else:
    maxID = spark.sql(f"SELECT MAX(id) FROM {v.fullyQualifiedTableName}").collect()[0][0]
  start = maxID + 1
  end = start + numRows - 1
  print ("starting at ID = ",start, ",ending on = ",end)
  Range = range(start, end+1)
  ## This traverses through the Range and increment "x" by one unit each time, and that x value is used in the code to generate random data through Python functions in a class

  rdd = sc.parallelize(Range). \
           map(lambda x: (x, uf.clustered(x,numRows), \
                             uf.scattered(x,numRows), \
                             uf.randomised(x, numRows), \
                             uf.randomString(50), \
                             uf.padString(x," ",50), \
                             uf.padSingleChar("x",4000)))
  df = rdd.toDF(). \
       withColumnRenamed("_1","ID"). \
       withColumnRenamed("_2", "CLUSTERED"). \
       withColumnRenamed("_3", "SCATTERED"). \
       withColumnRenamed("_4", "RANDOMISED"). \
       withColumnRenamed("_5", "RANDOM_STRING"). \
       withColumnRenamed("_6", "SMALL_VC"). \
       withColumnRenamed("_7", "PADDING")
  df.write.mode("overwrite").saveAsTable("pycharm.ABCD")
  df.printSchema()
  df.explain()
  df.createOrReplaceTempView("tmp")
  sqltext = f"""
    INSERT INTO TABLE {v.fullyQualifiedTableName}
    SELECT
            ID
          , CLUSTERED
          , SCATTERED
          , RANDOMISED
          , RANDOM_STRING
          , SMALL_VC
          , PADDING
    FROM tmp
    """
  spark.sql(sqltext)
  spark.sql(f"SELECT MIN(id) AS minID, MAX(id) AS maxID FROM {v.fullyQualifiedTableName}").show(n=20,truncate=False,vertical=False)
  ##sqlContext.sql("""SELECT * FROM pycharm.randomDataPy ORDER BY id""").show(n=20,truncate=False,vertical=False)
  lst = (spark.sql("SELECT FROM_unixtime(unix_timestamp(), 'dd/MM/yyyy HH:mm:ss.ss') ")).collect()
  print("\nFinished at");uf.println(lst)


  ##print(os.listdir(warehouseLocation))
  spark.sql("show databases").show()


if __name__ == "__main__":
  print("\n working on maths")
  mathoperations = mo.MathOperations()
  jsonstuff = mo.Jsonstuff()
  print(mathoperations.testAddition(2,3))
  print(mathoperations.testMultiplication(10, 25))
  mathoperations.dispName()
  print("\nExpected gross salary per year at daily rate of GBP" + str(mathoperations.returnDailyRate()) + " is " + mathoperations.expectedYearlyIncome())
  print("\n" + jsonstuff.loadJson())
  print("\n working on Hive table")
  a = someClass
  a.main()
  print("\nworking on Oracle table")
  import run_oracle as to
  c = to.runOracle
  c.run_oracle_module()
  print("\ndoing Google cloud stuff")
  import etl_python_oracle_to_GCP as etl
  b = etl.AllInOne
  b.delete_blob_if_exists_and_upload_to_GCP()
  b.drop_if_bqTable_exists()
  b.bq_create_table()
  b.bq_load_csv_in_gcs()

