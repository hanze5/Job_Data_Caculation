from pyspark.sql import SQLContext
from pyspark import SparkConf, SparkContext
from RDD_Dfr import Data_dfr
from pyspark.sql import SparkSession,Row
from WriteToDB import WrToDB

from pyspark.sql.functions import split

DFR = Data_dfr.DataFrame


########################################################
#不同省份的行业数量及占比
########################################################

session=  SparkSession.builder.getOrCreate()#获取或创建

#重构
# mapRDD = RDD.map(lambda x:Row(
#         Jprovince =x.Jarea.split("-")[0],
#         Jindustry =x.Jtype.split("_")[0],
#         )
#                  )
DFR=DFR.withColumn("Jindustry", split(DFR['Jtype'], "_")[0])
DFR=DFR.withColumn("Jprovince", split(DFR['Jarea'], "-")[0])

#构建Dataframe
# ALL_dfr = session.createDataFrame(mapRDD)
df1 = DFR.groupBy('Jprovince','Jindustry').count().orderBy('Jprovince')
df2 = DFR.groupBy('Jprovince').count().orderBy('Jprovince')
#构建临时表
df1.registerTempTable('jobdata1')
df2.registerTempTable('jobdata2')

df3 =  session.sql("select  jobdata1.Jprovince, jobdata1.Jindustry , jobdata1.count count , jobdata1.count/jobdata2.count Ratio "
                   "from jobdata1,jobdata2 "
                   "where jobdata1.Jprovince = jobdata2.Jprovince")

print('='*100)
df3.show()
WrToDB(df3,"Demand3_2","overwrite")

print('='*100)


