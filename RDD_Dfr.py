import os

from pyspark.sql import SQLContext
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession,Row

import re

masterurl = 'local[*]'

# masterurl = 'spark://172.20.3.82:7077'
conf = SparkConf().setAppName("job_Calculation").setMaster(masterurl).set("spark.sql.execution.arrow.enabled", "true")
conf.set("spark.debug.maxToStringFields", "100")
conf.set('spark.executor.memory', '2g')
conf.set("spark.executor.cores", '5')
sc = SparkContext.getOrCreate(conf)

# conf = SparkConf().setAppName("job_Calculation").setMaster(masterurl).set("spark.driver.host",'172.31.6.4 ')
# spark=SparkSession.builder.config(conf=conf).getOrCreate()
# sc=spark.sparkContext

sql_context = SQLContext(sc)

url = "jdbc:mysql://47.113.123.159/job_info"
driver = 'com.mysql.cj.jdbc.Driver'
user = 'pa'
password = '258258cqu'
properties = {"user": user, "password": password}


data_51job = sql_context.read.jdbc(url=url, table='51job', properties=properties)
data_58job = sql_context.read.jdbc(url=url, table='58job', properties=properties)
data_cnzpjob = sql_context.read.jdbc(url=url, table='cnzp', properties=properties)
data_lagou = sql_context.read.jdbc(url=url, table='lagoujob', properties=properties)
data_liepin = sql_context.read.jdbc(url=url, table='liepin', properties=properties)

# # 合并
dataAll = data_51job.union(data_58job).union(data_lagou).union(data_liepin).union(data_cnzpjob)

# dataAll = sql_context.read.jdbc(url=url, table='jobclean', properties=properties)

# dataAll = sql_context.read.jdbc(url=url, table='51job', properties=properties)
#
# #2_2所需
# dataAll = data_lagou.union(data_liepin)

class Data_dfr:
    DataFrame = dataAll
    sc =sc
