from pyspark.sql import SQLContext
from pyspark import SparkConf, SparkContext
from RDD_Dfr import Data_dfr
from pyspark.sql import SparkSession,Row
from WriteToDB import WrToDB
from pyspark.sql.types import *

from pyspark.sql.functions import split

DFR = Data_dfr.DataFrame

########################################################
#行业人数需求
########################################################

session=  SparkSession.builder.getOrCreate()#获取或创建

ALL_dfr = DFR.withColumn("Jindustry", split(DFR['Jtype'], "_")[0])
ALL_dfr = ALL_dfr.filter(ALL_dfr['Jindustry']!='None')
# #重构
# mapRDD = RDD.map(lambda x:Row(
#         Jindustry =x.Jtype.split("_")[0],
#         JhireCount = x.JhireCount if (x.JhireCount!='') else 0
#         )
# )
#
#
#
# #构建Dataframe
# schema = StructType([
#     StructField("Jindustry", StringType(), True),
#     StructField("JhireCount", StringType(), True),
# ])
# ALL_dfr = session.createDataFrame(mapRDD,schema=schema)
ALL_dfr.registerTempTable("dataAll")
#构建临时表


df =  session.sql("select  Jindustry,sum(int(JhireCount)) Totalhirecount "
                   "from dataAll "
                  # "where JhireCount != ''"
                   "group by Jindustry")

print('='*100)
# df.show()
WrToDB(df,"Demand4_1","overwrite")

print('='*100)


