from pyspark.sql import SQLContext
from pyspark import SparkConf, SparkContext
from RDD_Dfr import Data_dfr
from pyspark.sql import SparkSession,Row
from WriteToDB import WrToDB
from pyspark.sql.types import *

from pyspark.sql.functions import split

DFR = Data_dfr.DataFrame

########################################################
#不同性质企业薪资分析
########################################################

session=  SparkSession.builder.getOrCreate()#获取或创建

#重构
# mapRDD = RDD.map(lambda x:Row(
#         JcomType =x.JcomType,
#         JminSalary = x.JminSalary,
#         JmaxSalary = x.JmaxSalary
#         )
#                  )
#
# #构建Dataframe
# schema = StructType([
#     StructField("JcomType", StringType(), True),
#     StructField("JminSalary", IntegerType(), True),
#     StructField("JmaxSalary", IntegerType(), True)
#
# ])
# ALL_dfr = session.createDataFrame(mapRDD,schema=schema)
DFR.registerTempTable("dataAll")
# ALL_dfr.registerTempTable("dataAll")


#构建临时表

df = session.sql("select  JcomFinanceStage,count(*) count, avg ((JmaxSalary+JminSalary)/2) JavSalary "
                   "from dataAll where JcomFinanceStage != '无' and JcomFinanceStage !='' "
                 "group by JcomFinanceStage")

print('='*100)
# df1.show()
df.show()

WrToDB(df,"Demand2_2","overwrite")

print('='*100)
