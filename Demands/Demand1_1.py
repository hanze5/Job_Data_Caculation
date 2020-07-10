from RDD_Dfr import Data_dfr
from pyspark.sql import SparkSession,Row
from WriteToDB import WrToDB
from pyspark.sql.functions import split



DFR = Data_dfr.DataFrame
RDD = Data_dfr.DataFrame.rdd
sc = Data_dfr.sc

########################################################
#行业 以及其下级对上级行业的占比
########################################################

session=  SparkSession.builder.getOrCreate()#获取或创建

#重构

# mapRDD = RDD.map(lambda x:Row(
#         Jindustry1 =x.Jtype.split("_")[0],
#         Jindustry2 =x.Jtype.split("_")[1] if len(x.Jtype.split("_"))>1 else " ",
#         Jindustry3 =x.Jtype.split("_")[2] if len(x.Jtype.split("_"))>2 else " ",
#         )
#                  )
#
# industry1RDD = mapRDD.map(lambda x:(x[0],1)).reduceByKey(lambda x,y:x+y)


# finalResults = []
#
# for item1 in industry1RDD.collect():
#     # print(type(item),item[0])
#     industry2RDD = mapRDD.filter(lambda r:r[0]==item1[0]).map(lambda x: (x[1], 1)).reduceByKey(lambda x, y: x + y)
#     for item2  in industry2RDD.collect():
#             industry3RDD = mapRDD.filter(lambda r: r[1] == item2[0] and r[0]==item1[0] ).map(lambda x: (x[2], 1)).reduceByKey(lambda x, y: x + y)
#             for item3 in industry3RDD.collect():
#                     result=[item1[0],item2[0],item3[0],item2[1]/item1[1],item3[1]/item2[1]]
#                     finalResults.append(result)
#
# resultRDD = sc.parallelize(finalResults)
# resultRow = resultRDD.map(lambda p:Row(
#     Jindustry1 = p[0],
#     Jindustry2 =p[1],
#     Jindustry3 =p[2],
#     Ratio2to1 =p[3],
#     Ratio3to2 =p[4],
# ))
# resultDRF = session.createDataFrame(resultRow)
# # resultDRF.show()
# WrToDB(resultDRF,"Demand1_1_2","overwrite")

###############################################################################################################
DFR = DFR.withColumn('Jindustry1',split(DFR['Jtype'],'_')[0])
DFR = DFR.withColumn('Jindustry2',split(DFR['Jtype'],'_')[1])
DFR = DFR.withColumn('Jindustry3',split(DFR['Jtype'],'_')[2])
# DFR = DFR.withColumn('Jindustry2',split(DFR['Jtype'],'_')[1] if len(split(DFR['Jtype'],'_'))>1 else " ")
# DFR = DFR.withColumn('Jindustry3',split(DFR['Jtype'],'_')[2] if len(split(DFR['Jtype'],'_'))>2 else " ")

industry1DFR = DFR.groupBy("Jindustry1").count()
# count = industry1DFR.count()
finalResults = []
# count1 = 0
# count2 = 0
# count3 = 0

for item1 in industry1DFR.collect():
    industry2DFR = DFR.filter(DFR['Jindustry1']==item1.Jindustry1).groupBy('Jindustry2').count()
    count1 = item1[1]
    if(item1[0]==''):
        Jindustry1 = "其它"
    else:
        Jindustry1 = item1.Jindustry1
    result = [Jindustry1,"所有",count1]
    finalResults.append(result)

    for item2  in industry2DFR.collect():
        industry3DFR = DFR.filter(DFR['Jindustry1'] == item1.Jindustry1 ).filter(DFR['Jindustry2'] == item2.Jindustry2).groupBy('Jindustry3').count()
        count2 = item2[1]
        if (item2[0] == ''):
            Jindustry2 = "其它"
        else:
            Jindustry2 = item2.Jindustry2
        result = [Jindustry2,Jindustry1 , count2]
        finalResults.append(result)

        for item3 in industry3DFR.collect():
            count3 = item3[1]
            if (item3[0] == ''):
                Jindustry3 = "其它"
            else:
                Jindustry3 = item3.Jindustry3
            result = [Jindustry3, Jindustry2, count3]
            finalResults.append(result)

print(finalResults)
resultRDD = sc.parallelize(finalResults)
resultRow = resultRDD.map(lambda p:Row(
    JtypeNow = p[0],
    JtypeFather =p[1],
    count =p[2],
))
resultDRF = session.createDataFrame(resultRow).orderBy('JtypeFather','JtypeNow')
resultDRF.show()
WrToDB(resultDRF,"Demand1_1_2","overwrite")