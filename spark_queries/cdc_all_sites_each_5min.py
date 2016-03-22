from pyspark.sql import HiveContext
from pyspark import SparkConf
from pyspark import SparkContext
from operator import add

conf = SparkConf().setAppName("EDF").setMaster("local")
sc = SparkContext(conf=conf)
hiveContext = HiveContext(sc)
hiveContext.sql("use db_edf")

df = hiveContext.sql("select timestp as tp, consumption as cp from consumption").rdd.reduceByKey(add).sortBy(lambda x:x[0]).toDF()
df2 = df.select(df["_1"].alias("tp"), df["_2"].alias("sum_cp"))

df2.registerAsTable("tmp_table")
df3 = hiveContext.sql("select from_unixtime(tp) as date_time, sum_cp from tmp_table order by tp")
df3.show()

hiveContext.sql("create table final_cdc_all_sites_each_5min_with_date_v2 as select from_unixtime(tp) as date_time, tp, sum_cp from tmp_table")
