#-*-coding:utf-8 -*-
import pandas as pd
import yaml
import kafkaUtils
import redisUtils
import json
from kafka import KafkaProducer
import time
import os

f = open('../conf/gkxt.yaml')
yaml_conf = yaml.load(f)
kafka_conf = yaml_conf['kafka']
spark_conf = yaml_conf['spark']
jr = redisUtils.getRedisConn()
brokers = kafka_conf['broker_list']
topic = kafka_conf['from_topic']
group_id = kafka_conf['group_id']
producer = KafkaProducer(bootstrap_servers = brokers.split(','))

def get_data(hc_type):
	print hc_type
	redis_key = '%s_file_time' % hc_type
	last_file_time = jr.get(redis_key)
	path = '/opt/bfd/gkxt/getData/data/%s.csv' % hc_type
	f_time = os.path.getmtime(path)
	file_time = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(f_time))
	print '当前文件时间:%s' % file_time
	#根据文件的修改时间来判断文件是否已经读取过
	if str(last_file_time) == str(file_time):
	    return 0
	this_time = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
	with open(path,'r') as f:
	    count = 0
	    for data in f.readlines():
		count = count+1
	        row = data.split(',')
	        d = {}
                d['card_type'] = row[0]
                d['card_num'] = row[1]
                d['col_time'] = (row[2][:19]).replace('/','-')
                d['data_source'] = row[3]
                d['behavior'] = row[4]
                d['device_id'] = row[5]
                d['pic1'] = row[6]
                d['pic2'] = row[7]
                d['pic3'] = row[8]
                d['etl_time'] = this_time
                d_json = json.dumps(d)
                #print d_json
                kafkaUtils.insert(d_json,producer,'test_realtime_track_source')
	    print '数据条数:%d' % count
	jr.set(redis_key,file_time)

if __name__ == '__main__':
	
	hc_types = ['hcry','hccl']
	while True:
	    for hc_type in hc_types:
	        get_data(hc_type)
	    time.sleep(30)

