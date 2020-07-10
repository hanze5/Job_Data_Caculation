from pyspark.sql import SQLContext
from pyspark import SparkConf, SparkContext
from RDD_Dfr import Data_dfr
from pyspark.sql import SparkSession,Row
from WriteToDB import WrToDB

from pyspark.sql.functions import split

DFR = Data_dfr.DataFrame

########################################################
#不同城市的平均薪资
########################################################

session=  SparkSession.builder.getOrCreate()#获取或创建

#重构
# mapRDD = RDD.map(lambda x:Row(
#         Jcity =x.Jarea.split("-")[-1],
#         JminSalary = x.JminSalary,
#         JmaxSalary = x.JmaxSalary
#         )
#                  )

DFR=DFR.withColumn("Jcity1", split(DFR['Jarea'], "-")[1])
DFR=DFR.withColumn("Jcity", split(DFR['Jcity1'], ",")[0])

#构建Dataframe
# ALL_dfr = session.createDataFrame(mapRDD)
DFR.registerTempTable("dataAll")

#构建临时表

df = session.sql("select  Jcity, avg ((JmaxSalary+JminSalary)/2) JavSalary "
                   "from dataAll "
                   "group by Jcity"
                 " order by Jcity")

print('='*100)

# df.show()

WrToDB(df,"Demand3_1","overwrite")

print('='*100)
