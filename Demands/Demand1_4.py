from RDD_Dfr import Data_dfr
from pyspark.sql import SparkSession,Row
from WriteToDB import WrToDB

from pyspark.sql.functions import split

DFR = Data_dfr.DataFrame


########################################################
#行业-学历-薪资
########################################################

session=  SparkSession.builder.getOrCreate()#获取或创建

# # 重构
# mapRDD = RDD.map(lambda x: Row(
#     Jindustry =x.Jtype.split("_")[0],
#     Jeducation=x.Jeducation,
#     JminSalary=x.JminSalary,
#     JmaxSalary=x.JmaxSalary
#     )
# )

DFR=DFR.withColumn("Jindustry", split(DFR['Jtype'], "_")[0])


#构建Dataframe
# ALL_dfr = session.createDataFrame(mapRDD)
DFR.registerTempTable("dataAll")

#构建临时表

df = session.sql("select  Jindustry,Jeducation, avg ((JmaxSalary+JminSalary)/2) JavSalary "
                   "from dataAll "
                 "where Jindustry is not null and JminSalary is not null "
                 "group by Jindustry, Jeducation "
                 "order by Jindustry")

print('='*100)

df.show()
#
WrToDB(df,"Demand1_4","overwrite")

print('='*100)
