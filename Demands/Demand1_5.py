
from RDD_Dfr import Data_dfr
from pyspark.sql import SparkSession,Row
from WriteToDB import WrToDB

from pyspark.sql.functions import split

DFR = Data_dfr.DataFrame


########################################################
#经验-学历-薪资
########################################################

session=  SparkSession.builder.getOrCreate()#获取或创建

# 重构
# mapRDD = RDD.map(lambda x: Row(
#     Jexperience=x.Jexperience,
#     Jeducation=x.Jeducation,
#     JminSalary=x.JminSalary,
#     JmaxSalary=x.JmaxSalary
#     )
# )


#构建Dataframe
# ALL_dfr = session.createDataFrame(mapRDD)
DFR.registerTempTable("dataAll")

#构建临时表

df = session.sql("select  Jexperience,Jeducation, avg ((JmaxSalary+JminSalary)/2) JavSalary "
                   "from dataAll "
                 "group by Jexperience, Jeducation "
                 "order by Jexperience")

print('='*100)

df.show()

WrToDB(df,"Demand1_5","overwrite")

print('='*100)
