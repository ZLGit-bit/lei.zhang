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
from esUtils import EsUtils
from redisUtils import RedisUtils
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

f = open('/opt/bfd/gkxt/conf/gkxt.yaml' )
yaml_conf = yaml.load(f)
spark_conf = yaml_conf['spark']
mysql_conf = yaml_conf['mysql'] 
kafka_conf = yaml_conf['kafka']
es_conf = yaml_conf['es']
redis_conf = yaml_conf['redis']
es = EsUtils(es_conf)
redisUtil = RedisUtils(redis_conf)
sql_context = SparkSession \
        .builder \
        .appName("GkxtDataProcess") \
	.config("spark.ui.port", "4734") \
	.config('spark.yarn.executor.memoryOverhead','8192') \
        .getOrCreate()
sc = sql_context.sparkContext
ssc=StreamingContext(sc,spark_conf['schedule'])

#执行方法
def start():
    brokers = kafka_conf['broker_list']
    topic = kafka_conf['from_topic']
    group_id = kafka_conf['group_id']
    producer = KafkaProducer(bootstrap_servers = brokers.split(','))
    pool = redis.ConnectionPool(host=redis_conf['host'],password=redis_conf['pass'],port=redis_conf['port'])
    #sparkStreaming 读取kafka数据
    #kafkaParams = {"metadata.broker.list": brokers,"group.id":group_id,"auto.offset.reset":"smallest"}
    kafkaParams = {"metadata.broker.list": brokers,"group.id":group_id,"auto.offset.reset":"largest"}
    kafkaStreams = createDataStream(ssc,topic,kafkaParams)
    result=kafkaStreams  
    #数据处理
    kafkaStreams.transform(storeOffsetRanges).map(lambda x:x[1]).foreachRDD(lambda data:process(data,producer))
    #result.pprint()
    ssc.start()             
    ssc.awaitTermination() 
 
offsetRanges = []

#sparkStreaming stream转换
def storeOffsetRanges(rdd):
    global offsetRanges
    offsetRanges = rdd.offsetRanges()
    return rdd

#取出kafka中的数据
def flatmapRdd(rdd):
    nrdd = rdd.map(lambda d:d[1])
    nrdd

#数据处理
def process(data,producer):
	import MysqlUtils
	start = time.time()
	dt = str(time.strftime('%Y%m',time.localtime(time.time())))

	#把从kafk中读到的json数据解析
	#rdd = data.map(loadjson)
	rdd = data.mapPartitions(mapJson)
	t0 = time.time()
	print 'jsonParse:%s' % str(t0-start)
	regist_tb = mysql_conf['regist_tb']

	#es相关配置
	index = "%s_%s" % (es_conf['index'],dt)
	#index = es_conf['index']
        index_type = es_conf['type']	
	es_nodes = es_conf['nodes']
	t1 = time.time()
	for r_tb in regist_tb.split(','):
	    t1 = time.time()
	    #把mysql中计算需要的表注册成DataFrame
            MysqlUtils.regist_tb(sql_context,mysql_conf,r_tb)
	    t2 = time.time()
	if not rdd.isEmpty():
	   tdf = sql_context.createDataFrame(rdd,DataProcess.sourceStruct()).coalesce(10)
	   #把计算结果注册为spark的一张表res_tb,这个在配置文件res_sql中使用到
	   tdf.registerTempTable('res_tb')
	   #kafka中读到的数据和人的信息设备信息进行关联
	   t1 = time.time()
	   #tdf.show()
	   rs_df = sql_cal(spark_conf['res_sql']).coalesce(10).cache()
	   t2 = time.time()
	   print 'sql_cal:%f' % (t2-t1)
	   #rs_df.show()
	   print 'res_sql_cal'
	   if not rs_df.rdd.isEmpty():
	       m_df = DataProcess.mysql_data(rs_df)
	       #重点人最后一次位置更新mysql
	       MysqlUtils.save_last_place(mysql_conf,m_df)
	       t3 = time.time()
	       print 'save_mysql:%f' % (t3-t2)
	       k_data = DataProcess.kafka_data(rs_df)
	       #重点人数据插入到kafka
	       kafkaUtils.sendData(k_data,producer,kafka_conf)
	       t4 = time.time()
	       print 'save_kafka:%f' % (t4-t3)
	       es_df = DataProcess.es_data(rs_df).coalesce(20)
	       t41 = time.time()
	       print 'es_cal:%f' % (t41-t4)
	       #新建es索引
	       es.createIndex()
	       #if es_df != 0:
	       #重点人轨迹数据插入到es
               es_df.write.format('org.elasticsearch.spark.sql').option('es.resource','%s/%s' % (index,index_type)).option('es.nodes',es_nodes).option('es.mapping.id','id').option('es.index.auto.create','false').mode('append').save()
	       t5 = time.time()
	       print 'save_es:%f' % (t5-t4)
               rs_df.unpersist()
               m_df.unpersist()
               es_df.unpersist()
	       #更新kafka的本次消费到的offset至redis
	   saveKafkaOffset(data)

	end = time.time()
	print end-start

#并发解析json
def mapJson(rdd):
	l = []
	for data in rdd:
		l.append(loadjson(data))
	return l
		
#解析json数据
def loadjson(jdata):
    try:
	row = []
   	json_obj = json.loads(jdata)
    	card_type = json_obj['card_type']
	car_type = json_obj['car_type']
    	card_num = json_obj['card_num']
	device_name = json_obj['device_name']
    	col_time = json_obj['col_time'][:19]
	import datetime
	try:
	    d_time = datetime.datetime.strptime(col_time,'%Y-%m-%d %H:%M:%S')
	except Exception, e:
	    col_time = ''
	    pass	
    	data_source = json_obj['data_source']
    	behavior = json_obj['behavior']
    	device_id = json_obj['device_id']
    	pic1 = json_obj['pic1']
    	pic2 = json_obj['pic2']
    	pic3 = json_obj['pic3']
    	etl_time = json_obj['etl_time']
	if len(col_time)!=19 or col_time.index('-')!=4:
	    return ('1','1','1','1','1','1','1','1','1','1','1','1')
	row = (card_type,car_type,card_num,col_time,data_source,behavior,device_id,device_name,pic1,pic2,pic3,etl_time)
	return row
    except:
	return ('1','1','1','1','1','1','1','1','1','1','1','1')

#sql计算
def sql_cal(sql):
	#print sql
	df = sql_context.sql(sql)
	return df

def sendData(data,producer,kafka_conf):
        brokers = kafka_conf['broker_list']
        topic = kafka_conf['to_topic']
        if producer is None:
           producer = KafkaProducer(bootstrap_servers = brokers.split(','))
        for js in json.loads(data):
           js = json.dumps(js)
           insert(js,producer,topic)


def insert(data,producer,topic):
        kstr = str(str(data).decode('unicode_escape'))
        producer.send(topic,kstr)
	
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
