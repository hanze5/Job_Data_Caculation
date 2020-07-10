import pandas as pd

from pyspark.sql import SQLContext
from pyspark import SparkConf, SparkContext
from RDD_Dfr import Data_dfr
from pyspark.sql import SparkSession,Row
from WriteToDB import WrToDB
import jieba
from getDataFromDB import wuhu

from pyspark.sql.functions import split


DFR = Data_dfr.DataFrame
sc = Data_dfr.sc

List = wuhu.dataList

########################################################
#不同行业的技能需求分析展示
############################################


session=  SparkSession.builder.getOrCreate()#获取或创建

#重构
# mapRDD = RDD.map(lambda x:Row(
#         Jindustry =x.Jtype.split("_")[0],
#         Jrequirements = x.Jrequirements
#         )
#                  )

def customFunction(row):
    return row.Jrequirements

def _map_to_pandas(rdds):
    """ Needs to be here due to pickling issues """
    return [pd.DataFrame(list(rdds))]

def toPandas(df, n_partitions=None):
    """
    Returns the contents of `df` as a local `pandas.DataFrame` in a speedy fashion. The DataFrame is
    repartitioned if `n_partitions` is passed.
    :param df:              pyspark.sql.DataFrame
    :param n_partitions:    int or None
    :return:                pandas.DataFrame
    """
    if n_partitions is not None: df = df.repartition(n_partitions)
    df_pand = df.rdd.mapPartitions(_map_to_pandas).collect()
    df_pand = pd.concat(df_pand)
    df_pand.columns = df.columns
    return df_pand

for Jitem in List:
    Jitem['Jindustry'] = Jitem['Jtype'].split('_')[0]

DFR = DFR.withColumn('Jindustry',split(DFR['Jtype'],'_')[0]).select('Jindustry','Jname')
# aaa = DFR.filter(DFR['Jindustry']=="金融").count
def calcu_by_industry(industry):
    print("="*40+industry+"="*40)
    # industryDFR = DFR.filter(DFR['Jindustry'] == industry)
    # print(industryDFR.count())
    stringsum = ""
    for i in List:
        if industry == i['Jindustry']:
            stringsum += i['Jname']

    print(stringsum)
    strList1= jieba.lcut(stringsum,cut_all= False)
    strList2 =[]
    laji = ["人员","服务","帮助","行业","链家","维护","操作","北京","2","1","3","4","和","有","及","等","公司","客户","）","（","+","-","销售",
            "5","/","工作","商圈","使用","相关","岗位职责","店铺","产品","推广","要求","制定","优化","管理","以上","分析","管理","进行",
            "熟悉"," 任职","较强","培训","活动","完成","计划"," 互联网","优先","以上学历","晋升","买家","员工","内容","平台","任职",
            "具有","业务","提供","为","","","","","","","","","",""]
    for item in strList1:
        if len(item)<2:
            continue
        elif item in laji:
            continue
        else:
            strList2.append(item)
    print("完成"+industry+"词语统计")
    strRDD = sc.parallelize(strList2)
    RDDMAP = strRDD.map(lambda x:(x,1))
    reduceRDD = RDDMAP.reduceByKey(lambda x,y:x+y)
    # print(reduceRDD.collect())
    ALLRDDRow = reduceRDD.map(lambda p:
           Row(
               Jindustry = industry,
            Jname=p[0],
            count=p[1],
           )
    )
    print("完成词频统计")
    try:
        ALL_dfr = session.createDataFrame(ALLRDDRow)
        ALL_dfr.show()
        # ALL_dfr.orderBy("count",ascending =0).show()
        WrToDB(ALL_dfr, "Demand5_1", "append")
    except:
        print("写入数据库时出问题了")

# industryDFR = mapRDD.map(lambda x:(x[0],1)).reduceByKey(lambda x,y:x+y)



0
industryDFR = DFR.groupBy('Jindustry').count()
for item in industryDFR.collect():
    # print(item)
    # # print(type(item),item[0])
    # print(item.Jindustry)
    calcu_by_industry(item.Jindustry)
print("齐活")

# calcu_by_industry('IT·互联网')


