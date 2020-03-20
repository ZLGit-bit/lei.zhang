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
import time
import sys
import os
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
        .appName("xzbaDataProcess") \
	.config("spark.ui.port", "7734") \
	.config('spark.yarn.executor.memoryOverhead','8192') \
        .getOrCreate()
sc = sql_context.sparkContext

def start():
	df = MysqlUtils.get_data(sql_context,mysql_conf,'fact_person_position_new')
	df.re
	index = "%s_%s" % (es_conf['index'],dt)
	#index = es_conf['index']
        index_type = es_conf['type']	
	es_nodes = es_conf['nodes']
	#重点人轨迹数据插入到es
        es_df.write.format('org.elasticsearch.spark.sql').option('es.resource','%s/%s' % (index,index_type)).option('es.nodes',es_nodes).option('es.mapping.id','id').option('es.index.auto.create','false').mode('append').save()

if __name__ == '__main__':
	start() 
