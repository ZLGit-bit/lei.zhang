#!/opt/anaconda2/bin python2.7-anaconda
# -*- coding: utf-8 -*-
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark import SparkConf, SparkContext
from pyspark.sql import functions as func
from pyspark.sql import SparkSession
import MysqlUtils
from pyspark.streaming import StreamingContext
from pyspark.sql.types import StructType,StructField,StringType
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

f = open('/opt/bfd/gkxt/conf/clgj.yaml' )
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
        .appName("yn_clgj") \
	.config("spark.ui.port", "4833") \
	.config('spark.yarn.executor.memoryOverhead','8192') \
        .getOrCreate()
sc = sql_context.sparkContext
ssc=StreamingContext(sc,spark_conf['schedule'])

#执行方法
def start():
    brokers = kafka_conf['broker_list']
    topic = 'clgj'
    group_id = 'clgj_group'
    producer = KafkaProducer(bootstrap_servers = brokers.split(','))
    pool = redis.ConnectionPool(host=redis_conf['host'],password=redis_conf['pass'],port=redis_conf['port'])
    #sparkStreaming 读取kafka数据
    kafkaParams = {"metadata.broker.list": brokers,"group.id":group_id,"auto.offset.reset":"smallest"}
    #kafkaParams = {"metadata.broker.list": brokers,"group.id":group_id,"auto.offset.reset":"largest"}
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
	index = 'person_track_zdry_clgj'
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
	   tdf = sql_context.createDataFrame(rdd,sourceStruct()).coalesce(10)
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
	       es_df = DataProcess.es_data_clgj(rs_df).coalesce(20)
	       #新建es索引
	       #es.createIndex()
	       
	       #重点人轨迹数据插入到es
               es_df.write.format('org.elasticsearch.spark.sql').option('es.resource','%s/%s' % (index,index_type)).option('es.nodes',es_nodes).option('es.mapping.id','id').option('es.index.auto.create','false').mode('append').save()
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
	
def sourceStruct():
        struct = StructType([StructField("card_type",StringType(),True),StructField("car_type",StringType(),True),StructField("card_num",StringType(),True),StructField("col_time",StringType(),True),StructField("data_source",StringType(),True),StructField("behavior",StringType(),True),StructField("device_id",StringType(),True),StructField("device_name",StringType(),True),StructField("lon",StringType(),True),StructField("lat",StringType(),True),StructField("brand",StringType(),True),StructField("sub_brand",StringType(),True),StructField("pic1",StringType(),True),StructField("pic2",StringType(),True),StructField("pic3",StringType(),True),StructField("etl_time",StringType(),True)])
        return struct
	
#解析json数据
def loadjson(jdata):
    try:
	row = []
	import re
	regex = re.compile(r'\\(?![/u])')
	jdata = regex.sub(r'\\\\',jdata)
	jdata = jdata.replace('\n','').replace('\t','')
   	json_obj = json.loads(jdata,strict=False)
    	card_type = '7'
	car_type = json_obj['speciesName']
    	card_num = json_obj['vehiclePlate']
    	col_time = json_obj['time'][:19]
	import datetime
	try:
	    d_time = datetime.datetime.strptime(col_time,'%Y%m%d%H%M%S')
	    f_time = d_time.strftime('%Y-%m-%d %H:%M:%S')
	    col_time = f_time
	except Exception, e:
	    col_time = ''
	    pass	
    	data_source = '1'
    	behavior = '1'
    	device_id = json_obj['tollCode']
	device_name= json_obj['tollName']
	brand = json_obj['brandName']
	sub_brand = json_obj['childBrandName']
	jd = str(json_obj['longitude'])
	wd = str(json_obj['latitude'])
	if jd.find('E')>0 or wd.find('E')>0:
	    jd = '0'
	    wd = '0'
	if float(jd)>200 or float(wd)>80 or float(wd)<10 or float(jd)<70:
	    jd = '0'
	    wd = '0'
        if float(jd)<float(wd):
            jd = wd
            wd = jd

    	pic1 = json_obj['url']
    	pic2 = ''
    	pic3 = ''
	etl_time = str(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())))
	if len(col_time)!=19 or col_time.index('-')!=4:
	    return ('1','1','1','1','1','1','1','1','1','1','1','1','1','1','1','1')
	row = (card_type,car_type,card_num,col_time,data_source,behavior,device_id,device_name,jd,wd,brand,sub_brand,pic1,pic2,pic3,etl_time)
	return row
    except Exception, e:
        #pass
	import inspect
        print "error:   ", inspect.stack()[0][1], inspect.stack()[0][3], ":", e, "\n\n\n"
	#print jdata
        #exit(-1)
	return ('1','1','1','1','1','1','1','1','1','1','1','1','1','1','1','1')

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
