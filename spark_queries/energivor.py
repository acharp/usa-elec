

from pyspark.sql import HiveContext
from pyspark import SparkConf
from pyspark import SparkContext
from operator import add
import datetime

conf = SparkConf().setAppName("EDF").setMaster("local")
sc = SparkContext(conf=conf)
hiveContext = HiveContext(sc)
hiveContext.sql("use db_edf")

firstday = 1325375940 #31/12/2011 23:59:00
ONE_DAY = 84600

df = hiveContext.sql("select timestp as tp, consumption as cp, site_id as id from consumption")

#while (firstday < 1356998400):
	
firstday += ONE_DAY
daydate = datetime.datetime.fromtimestamp(float(firstday)).strftime('%Y-%m-%d')
print(daydate)
test = df.filter(df.tp < firstday).groupBy("id").sum("cp")
test.show()

vect_date = [daydate] * len(test.collect())
print(vect_date)
df_date = hiveContext.createDataFrame(vect_date)
df_date.show() 
