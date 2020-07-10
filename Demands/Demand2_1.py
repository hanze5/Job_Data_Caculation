from pyspark.sql import SQLContext
from pyspark import SparkConf, SparkContext
from RDD_Dfr import Data_dfr
from pyspark.sql import SparkSession,Row
from WriteToDB import WrToDB
from pyspark.sql.types import *

from pyspark.sql.functions import split

DFR = Data_dfr.DataFrame

########################################################
#不同规模企业薪资分析
########################################################

session=  SparkSession.builder.getOrCreate()#获取或创建

#重构
# mapRDD = RDD.map(lambda x:Row(
#         JcomSize =x.JcomSize,
#         JminSalary = x.JminSalary,
#         JmaxSalary = x.JmaxSalary
#         )
#                  )

# #构建Dataframe
# schema = StructType([
#     StructField("JcomSize", StringType(), True),
#     StructField("JminSalary", IntegerType(), True),
#     StructField("JmaxSalary", IntegerType(), True)
#
# ])

# ALL_dfr = session.createDataFrame(mapRDD,schema=schema)
DFR.registerTempTable("dataAll")


#构建临时表

df = session.sql("select JcomSize,count(*) count,count(*)/"+str(DFR.count())+" Radio, avg ((JmaxSalary+JminSalary)/2) JavSalary from dataAll where JminSalary is not null group by JcomSize")

print('='*100)
# df1.show()
df.show()

WrToDB(df,"Demand2_1",'overwrite')

print('='*100)
