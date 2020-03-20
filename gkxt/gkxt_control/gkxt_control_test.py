# -*- coding:utf-8 -*-
from kafka import KafkaConsumer
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark import SparkConf, SparkContext
from pyspark.sql import functions as func
from pyspark.sql import SparkSession
import MysqlUtils
from redisUtils import RedisUtils
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils,TopicAndPartition
from pyspark.sql import Row
import json
import yaml
import redis
import time
from sqlalchemy import create_engine
import sqlalchemy
import datetime
import MysqlUtils
from Process_test import ControlProcess
from kafka import KafkaProducer
import pandas as pd
import time
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

f = open('/opt/bfd/gkxt/gkxt_control/conf/control_test.yaml')
yaml_conf = yaml.load(f)
mysql_conf = yaml_conf['mysql']
spark_conf = yaml_conf['spark']
kafka_conf = yaml_conf['kafka']
redis_conf = yaml_conf['redis']
brokers = kafka_conf['broker_list']
topic = kafka_conf['from_topic']
group_id = kafka_conf['group_id']
redisUtil = RedisUtils(redis_conf)
sql_context = SparkSession \
        .builder \
        .appName("GkxtControl") \
        .enableHiveSupport() \
        .config("spark.ui.port", "6364") \
        .config('spark.yarn.executor.memoryOverhead','8192') \
        .getOrCreate()
sc = sql_context.sparkContext
ssc=StreamingContext(sc,spark_conf['schedule'])
controlProcess = ControlProcess(yaml_conf,sql_context) #具体的执行方法类

#执行方法
def start():
    brokers = kafka_conf['broker_list']
    topic = kafka_conf['from_topic']
    group_id = kafka_conf['group_id']
    producer = KafkaProducer(bootstrap_servers = brokers.split(','))
    pool = redis.ConnectionPool(host=redis_conf['host'],password=redis_conf['pass'],port=redis_conf['port'])
    #sparkStreaming 读取kafka数据
    kafkaParams = {"metadata.broker.list": brokers,"group.id":group_id,"auto.offset.reset":"largest"}
    kafkaStreams = createDataStream(ssc,topic,kafkaParams)
    result=kafkaStreams
    print 'start_%s' % topic
    #数据处理
    kafkaStreams.transform(storeOffsetRanges).map(lambda x:x[1]).foreachRDD(lambda data:process(data))
    #result.pprint()
    ssc.start()
    ssc.awaitTermination()


#sparkStreaming stream转换
def storeOffsetRanges(rdd):
    global offsetRanges
    offsetRanges = rdd.offsetRanges()
    return rdd


#根据不同的类型目标执行不同的判断逻辑
def execute(data,rule_df):
	print 'start->process->execute'
	if rule_df.shape[0]>0 :
		controlProcess.executeFunc(data,rule_df) #离开区域
	else:
		pass

#遍历每个布控的布控规则
def process(data):
	start_time = time.time()
	#读取所有人的布控规则
	rule_df = MysqlUtils.get_data( mysql_conf, mysql_conf['control_sql'])
	ori_data = data.mapPartitions(mapjson)
	#print ori_data.toDF().toPandas()
	if not ori_data.isEmpty():
	    ori_df = ori_data.toDF().toPandas()
	    if ori_df.shape[0]>0:
	        print 'start->process'
		print '数据条数:%d' % ori_df.shape[0]
		#执行具体的布控判断方法
		s_time = time.time()
	        execute(ori_df,rule_df)
		end_time = time.time()
		#修改kafka的offset,记录最新的位置
		print end_time-s_time
		print s_time-start_time
	else:
	    pass
	saveKafkaOffset(data)

#解析json数据
def mapjson(jdata):
	l = []
	for data in jdata:
	    l.append(loadjson(data))
	return l

#解析json,把None替换成''
def loadjson(data):
	try:
	    reload(sys)
	    sys.setdefaultencoding('utf-8')

	    data = json.loads(str(data))
            device_district = fieldValue(data["device_district"])
            col_time = str(data["col_time"])
            card_num = str(data["card_num"])
            name = fieldValue(data["name"])
            #device_location = str(data["device_location"])
            idcard = str(data["idcard"])
            pic1 = str(data["pic1"])
            pic2 = str(data["pic2"])
	    dev_id = str(data["dev_id"])
            device_name = fieldValue(data["device_name"])
	    person_type = fieldValue(data["person_type"])
            card_type = str(data["card_type"])
            etl_time = str(data["etl_time"])
            data_source = fieldValue(data["data_source"])
            device_province = fieldValue(data["device_province"])
            behavior = fieldValue(data["behavior"])
            device_street = fieldValue(data["device_street"])
            device_city = fieldValue(data["device_city"])
            pic3 = str(data["pic3"])
            device_town = fieldValue(data["device_town"])
            device_id = str(data["device_id"])
            province_code = str(data["province_code"])
            city_code = str(data["city_code"])
            district_code = str(data["district_code"])
            jd = str(data["jd"])
            wd = str(data["wd"]) 
	    return Row(device_district=device_district,col_time=col_time,card_num=card_num,name=name,idcard=idcard,pic1=pic1,pic2=pic2,person_type=person_type,dev_id=dev_id,device_name=device_name,card_type=card_type,etl_time=etl_time,data_source=data_source,device_province=device_province,behavior=behavior,device_street=device_street,device_city=device_city,pic3=pic3,device_town=device_town,device_id=device_id,province_code=province_code,city_code=city_code,district_code=district_code,jd=jd,wd=wd)
	except Exception, e:
	    return Row(device_district='',col_time='',card_num='',name='',idcard='',pic1='',pic2='',person_type='',dev_id='',device_name='',card_type='',etl_time='',data_source='',device_province='',behavior='',device_street='',device_city='',pic3='',device_town='',device_id='',province_code='',city_code='',district_code='',jd='',wd='')

#判断字段值是否为空
def fieldValue(field_data):
	if field_data is None:
	    return ''
	else:
	    return field_data

#把offset存储到redis
def saveKafkaOffset(rdd):
    for o in offsetRanges:
        redisUtil.setRedisData('%s_kafkaOffset' % o.topic,o.partition,o.untilOffset)
        print "%s %s %s %s %s" % ('topic:%s' % o.topic,'分区:%s' % o.partition, '开始位置:%d' % o.fromOffset, '结束位置:%d' % o.untilOffset,'消费条数:%d' % (o.untilOffset-o.fromOffset))


#创建kafka数据流
def createDataStream(ssc,topic,kafkaParams):
        fromOffsets = {}
        #从redis中得到中topic已消费的分区和offset
        offsets = redisUtil.getRedisData('%s_kafkaOffset' % topic)
        for part in offsets:
                topicAndPartition = TopicAndPartition(topic,part)
                fromOffsets[topicAndPartition] = offsets[part]
        print fromOffsets

        if len(fromOffsets) == 0:
                print '------首次读取kafka------'
                kafkaStreams = KafkaUtils.createDirectStream(ssc,[topic],kafkaParams=kafkaParams)
        else:
                print '------从redis存储的offset处开始消费kafka数据------'
                kafkaStreams = KafkaUtils.createDirectStream(ssc,[topic],kafkaParams=kafkaParams,fromOffsets=fromOffsets)

        return kafkaStreams

if __name__ == '__main__':
	start()
