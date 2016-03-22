
from pyspark.sql import HiveContext
from pyspark import SparkConf
from pyspark import SparkContext
from operator import add

conf = SparkConf().setAppName("EDF").setMaster("local")
sc = SparkContext(conf=conf)
hiveContext = HiveContext(sc)
hiveContext.sql("use db_edf")

df1 =  hiveContext.sql("select site_id as id, timestp as tp, consumption as cp from consumption")
df2 = hiveContext.sql("select site_id as id, industry as indus, square_feet as sq from sites")

df = df1.join(df2, df1.id == df2.id)

df_res = df.rdd.map(lambda(a,b,c,d,e,f):(e, c/f)).reduceByKey(add).sortBy(lambda x: x[1]).toDF()
df_res2 = df_res.select(df_res["_1"].alias("indus"), df_res["_2"].alias("intensity"))
df_res2.show()

df_res2.registerAsTable("tmp_table")
hiveContext.sql("create table final_total_rank_indus_intensity as select * from tmp_table")
