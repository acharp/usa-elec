
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

df_winter = df.filter(df.tp > 1325376000).filter(df.tp < 1332220465)
df_res_winter = df_winter.rdd.map(lambda(a,b,c,d,e,f):(e, c/f)).reduceByKey(add).sortBy(lambda x: x[1]).toDF()
df_res_winter2 = df_res_winter.select(df_res_winter["_1"].alias("indus"), df_res_winter["_2"].alias("intensity"))
df_res_winter2.registerAsTable("tmp_table")
hiveContext.sql("create table final_winter_rank_indus_intensity_v2 as select * from tmp_table")

df_spring = df.filter(df.tp > 1332220465).filter(df.tp < 1340233729)
df_res_spring = df_spring.rdd.map(lambda(a,b,c,d,e,f):(e, c/f)).reduceByKey(add).sortBy(lambda x: x[1]).toDF()
df_res_spring2 = df_res_spring.select(df_res_spring["_1"].alias("indus"), df_res_spring["_2"].alias("intensity"))
df_res_spring2.registerAsTable("tmp_table2")
hiveContext.sql("create table final_spring_rank_indus_intensity_v2 as select * from tmp_table2")


df_summer = df.filter(df.tp > 1340233729).filter(df.tp < 1348325339)
df_res_summer = df_summer.rdd.map(lambda(a,b,c,d,e,f):(e, c/f)).reduceByKey(add).sortBy(lambda x: x[1]).toDF()
df_res_summer2 = df_res_summer.select(df_res_summer["_1"].alias("indus"), df_res_summer["_2"].alias("intensity"))
df_res_summer2.registerAsTable("tmp_table3")
hiveContext.sql("create table final_summer_rank_indus_intensity_v2 as select * from tmp_table3")

df_automn = df.filter(df.tp > 1348325339).filter(df.tp < 1356088297)
df_res_automn = df_automn.rdd.map(lambda(a,b,c,d,e,f):(e, c/f)).reduceByKey(add).sortBy(lambda x: x[1]).toDF()
df_res_automn2 = df_res_automn.select(df_res_automn["_1"].alias("indus"), df_res_automn["_2"].alias("intensity"))
df_res_automn2.registerAsTable("tmp_table4")
hiveContext.sql("create table final_automn_rank_indus_intensity_v2 as select * from tmp_table4")


#test = df.rdd.map(lambda(a,b,c,d,e,f):(e, c/f)).reduceByKey(add).sortBy(lambda x: x[1]).collect()
#print(test)
