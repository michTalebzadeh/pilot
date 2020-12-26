from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import HiveContext
import src.usedFunctions as uf
import conf.variables as v

def test_runner():
   spark = SparkSession.builder \
        .appName("test") \
        .enableHiveSupport() \
        .getOrCreate()
   sc = SparkContext.getOrCreate()
   rows = 0
   numRows = 10
   if (spark.sql(f"""SHOW TABLES IN {v.DB} like '{v.tableName}'""").count() == 1):
       tablestat = spark.sql(f"""ANALYZE TABLE {v.fullyQualifiedTableName} compute statistics""")
       assert tablestat
       rows = spark.sql(f"""SELECT COUNT(1) FROM {v.fullyQualifiedTableName}""").collect()[0][0]
       assert rows >= 0  # table exists whether empty or not
   else:
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
       rows = spark.sql(f"""SELECT COUNT(1) FROM {v.fullyQualifiedTableName}""").collect()[0][0]
       assert rows == 0  # an empty table was created ok

   start = 0
   if (rows == 0):
       start = 1
       maxID = 0
   else:
       maxID = spark.sql(f"SELECT MAX(id) FROM {v.fullyQualifiedTableName}").collect()[0][0]
   start = maxID + 1
   end = start + numRows - 1
   assert end - start > 0
   Range = range(start, end+1)
   rdd = sc.parallelize(Range). \
         map(lambda x: (x, uf.clustered(x, numRows), \
                        uf.scattered(x, numRows), \
                        uf.randomised(x, numRows), \
                        uf.randomString(50), \
                        uf.padString(x, " ", 50), \
                        uf.padSingleChar("x", 4000)))
   assert rdd
   df = rdd.toDF(). \
         withColumnRenamed("_1", "ID"). \
         withColumnRenamed("_2", "CLUSTERED"). \
         withColumnRenamed("_3", "SCATTERED"). \
         withColumnRenamed("_4", "RANDOMISED"). \
         withColumnRenamed("_5", "RANDOM_STRING"). \
         withColumnRenamed("_6", "SMALL_VC"). \
         withColumnRenamed("_7", "PADDING")
   assert df
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
   diff = spark.sql(f"SELECT MAX(id) - MIN(id) FROM {v.fullyQualifiedTableName}").collect()[0][0]
   assert diff > 0  # there should be rows in table