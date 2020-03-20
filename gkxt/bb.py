#!/opt/anaconda2/bin python2.7-anaconda
# -*- coding: utf-8 -*-
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark import SparkConf, SparkContext
from pyspark.sql import functions as func
from pyspark.sql import SparkSession
import MysqlUtils
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils,TopicAndPartition
from pyspark.sql import Row
from kafka import KafkaProducer
import json
import esUtils
import kafkaUtils
import DataProcess
import redisUtils
import yaml
import redis
from sqlalchemy import create_engine
import sqlalchemy
import pandas as pd
import time
import sys
import os
from pyspark.sql.types import StructType,StructField,StringType
reload(sys)
sys.setdefaultencoding('utf-8')

f = open('./conf/gkxt.yaml')
yaml_conf = yaml.load(f)
spark_conf = yaml_conf['spark']
mysql_conf = yaml_conf['mysql']
kafka_conf = yaml_conf['kafka']
es_conf = yaml_conf['es']
redis_conf = yaml_conf['redis']

sql_context = SparkSession \
        .builder \
        .appName("GkxtDataProcess") \
        .config("spark.ui.port", "4734") \
        .config('spark.yarn.executor.memoryOverhead','8192') \
        .getOrCreate()
l = []
def test():
	a = [['1','2'],['3','4']]
	b = sql_context.sparkContext.parallelize(a).map(aa)
	schema = StructType([StructField("id",StringType(),True),StructField("name",StringType(),True)])
	df = sql_context.createDataFrame(b,schema)
	df.show()

def aa(data):
	return data

if __name__ == '__main__':
	test()
    
