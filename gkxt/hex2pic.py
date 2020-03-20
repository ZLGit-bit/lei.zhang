#!/opt/anaconda2/bin python2.7-anaconda
# -*- coding: utf-8 -*-
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark import SparkConf, SparkContext
from pyspark.sql import functions as func
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils,TopicAndPartition
from pyspark.sql import Row
import json
import pandas as pd
import time
import sys
import os
from pyspark.sql.types import StructType,StructField,StringType
reload(sys)
sys.setdefaultencoding('utf-8')

sql_context = SparkSession \
        .builder \
        .appName("GkxtDataProcess") \
        .config("spark.ui.port", "3234") \
	.enableHiveSupport() \
        .config('spark.yarn.executor.memoryOverhead','8192') \
        .getOrCreate()

if __name__ == '__main__':
	sql='select 1 from stg.STG_RKGL_YNRK_LDRKGL_LDRYLDXX limit 10'
	df = sql_context.sql(sql)
	df.show()
    
