from pyspark.sql import SQLContext
from pyspark import SparkConf, SparkContext
from RDD_Dfr import Data_dfr
from pyspark.sql import SparkSession,Row
from WriteToDB import WrToDB
from pyspark.sql.functions import split

DFR = Data_dfr.DataFrame

########################################################
#不同行业的平均薪资
########################################################

session=  SparkSession.builder.getOrCreate()#获取或创建

#重构
# mapRDD = RDD.map(lambda x:Row(
#         Jindustry =x.Jtype.split("_")[0],
#         JminSalary = x.JminSalary,
#         JmaxSalary = x.JmaxSalary
#         )
#                  )

DFR=DFR.withColumn("Jindustry", split(DFR['Jtype'], "_")[0])
DFR = DFR.filter(DFR["Jindustry"]!='None')

#构建Dataframe
# ALL_dfr = session.createDataFrame(mapRDD)
DFR.registerTempTable("dataAll")

#构建临时表

df = session.sql("select  Jindustry,avg(JminSalary)  JavminSalary,avg(JmaxSalary) JavmaxSalary, avg ((JmaxSalary+JminSalary)/2) JavSalary "
                   "from dataAll "
                 "where JmaxSalary is not NULL "
                   "group by Jindustry")

print('='*100)

df.show()

WrToDB(df,"Demand1_2","overwrite")

print('='*100)
