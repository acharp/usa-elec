from pyspark.sql import HiveContext
from pyspark import SparkConf
from pyspark import SparkContext
from operator import add

conf = SparkConf().setAppName("EDF").setMaster("local")
sc = SparkContext(conf=conf)
hiveContext = HiveContext(sc)
hiveContext.sql("use db_edf")

df1 = hiveContext.sql("select site_id as id, timestp as tp, consumption as cp from consumption")
df2 = hiveContext.sql("select site_id as id, industry as indus from sites")

df3 = df1.join(df2, df1.id == df2.id).groupBy(["tp","indus"]).mean("cp")

df3.show()

df4 = df3.select(df3.tp, df3.indus, df3["AVG(cp)"].alias("mean_cp")).orderBy("tp")

df4.show()

df4.registerAsTable("tmp_table")
hiveContext.sql("create table final_cdc_mean_by_inustry_each_5min_with_date as select from_unixtime(tp) as date_time, tp, indus, mean_cp from tmp_table")
