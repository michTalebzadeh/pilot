from pyspark.sql.functions import pandas_udf
from pyspark.sql.functions import PandasUDFType
from sparkutils import sparkstuff as s
from pyspark.sql.types import *
import pandas as pd

class S1:
  appName = "app1"
  spark = s.spark_session(appName)
  sc = s.sparkcontext()
  df = spark.createDataFrame([("a", 0), ("a", 1), ("b", 30), ("b", -50)], ["group", "power"])


  def below_threshold(threshold, group="group", power="power"):
    @pandas_udf("struct<group: string, below_threshold: boolean>", PandasUDFType.GROUPED_MAP)
    def below_threshold_(df):
        df = pd.DataFrame(
           df.groupby(group).apply(lambda x: (x[power] < threshold).any()))
        df.reset_index(inplace=True, drop=False)
        return df

    return below_threshold_

  df.groupBy("group").apply(below_threshold(-40)).show()
