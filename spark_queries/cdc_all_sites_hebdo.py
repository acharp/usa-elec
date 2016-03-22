from pyspark.sql import HiveContext
from pyspark import SparkConf
from pyspark import SparkContext
from operator import add

conf = SparkConf().setAppName("EDF").setMaster("local")
sc = SparkContext(conf=conf)
hiveContext = HiveContext(sc)
hiveContext.sql("use db_edf")

hiveContext.sql("create table final_cdc_all_sites_hebdo_with_date as select * from final_cdc_all_sites_each_5min_with_date_v2 where (tp % 604800) = 302400");
