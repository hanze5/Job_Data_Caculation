from pyspark.sql import SQLContext
from pyspark import SparkConf, SparkContext
from RDD_Dfr import Data_dfr
from pyspark.sql import SparkSession,Row
from WriteToDB import WrToDB

from pyspark.sql.functions import split

DFR = Data_dfr.DataFrame


########################################################
#不同岗位的学历要求分析
########################################################

session=  SparkSession.builder.getOrCreate()#获取或创建

#重构
# mapRDD = RDD.map(lambda x:Row(
#         Jindustry =x.Jtype.split("_")[0],
#         Jeducation=x.Jeducation,
#         )
#                  )
DFR=DFR.withColumn("Jindustry", split(DFR['Jtype'], "_")[0])
DFR.registerTempTable('dataAll')
#构建Dataframe

df1 = DFR.groupBy('Jindustry','Jeducation').count().orderBy('Jindustry')
df2 = DFR.groupBy('Jindustry').count().orderBy('Jindustry')
#构建临时表
df1.registerTempTable('jobdata1')
df2.registerTempTable('jobdata2')

df3 =  session.sql("select  jobdata1.Jindustry, jobdata1.Jeducation , jobdata1.count count , jobdata1.count/jobdata2.count Ratio "
                   "from jobdata1,jobdata2 "
                   "where jobdata1.Jindustry = jobdata2.Jindustry")


print('='*100)
df3.show()
WrToDB(df3,"Demand1_3","overWrite")

print('='*100)


