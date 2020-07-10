from RDD_Dfr import Data_dfr
from pyspark.sql import SparkSession,Row
from WriteToDB import WrToDB

from pyspark.sql.functions import split

DFR = Data_dfr.DataFrame

########################################################
#互联网不同开发岗位的平均薪资
########################################################

session=  SparkSession.builder.getOrCreate()#获取或创建
DFR  = DFR .drop("Jname")
DFR = DFR.withColumn('Jindustry',split(DFR['Jtype'],'_')[0])
DFR = DFR.withColumn('Jname',split(DFR['Jtype'],'_')[3])
DFR = DFR.filter(DFR['Jname']!='')


DFR.registerTempTable("dataAll")


df = session.sql("select  Jname, avg ((JmaxSalary+JminSalary)/2) JavSalary "
                   "from dataAll "
                 "where Jindustry = 'IT·互联网'"
                 "and JminSalary is not null and JmaxSalary is not null "
                 "group by Jname")

print('='*100)

df.show()

WrToDB(df,"Demand1_6","overwrite")

print('='*100)