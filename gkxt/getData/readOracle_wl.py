#-*- coding:utf-8 -*-
import cx_Oracle
import sys
import os
import time
import pandas as pd
import redisUtils
import kafkaUtils
import json
import yaml
from kafka import KafkaProducer

jr = redisUtils.getRedisConn()
f = open('../conf/gkxt.yaml')
f2 = open('./conf/conf.yaml')
ora_conf = yaml.load(f2)['oracle']['jqlk']
yaml_conf = yaml.load(f)
kafka_conf = yaml_conf['kafka']
brokers = kafka_conf['broker_list']
topic = kafka_conf['from_topic']
group_id = kafka_conf['group_id']
producer = KafkaProducer(bootstrap_servers = brokers.split(','))
reload(sys)
sys.setdefaultencoding('utf-8')
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
os.environ['NLS_LANG'] = 'AMERICAN_AMERICA.AL32UTF8'
#conn = cx_Oracle.connect('ZZPT2019/ZZPT2019@10.166.7.130:1521/orcl')

import oraclePool

pool = oraclePool.oracle(ora_conf)

def get_maxTime(data_type):
	conn = pool.get_conn(ora_conf)
	cur = conn.cursor()
	max_sql = ora_conf['max_sql']
	print max_sql
	data = cur.execute(max_sql)
	rows = data.fetchall()
	max_time = rows[0][0]
	conn.close()
	cur.close()
	return max_time

#获取景区数据
def get_data(data_type):
	conn = pool.get_conn(ora_conf)
	cur = conn.cursor()
	last_time = jr.get('%s_last_time' % data_type)
	max_time = get_maxTime(data_type)
	#this_time = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))

	exe_sql = ora_conf['data_sql'] % (last_time,max_time)

	x = cur.execute(exe_sql)
	data = x.fetchall()
	print exe_sql
	print '数据条数:%d' % len(data)
	for row in data:
	    d = {}
            d['card_type'] = row[0]
            d['card_num'] = row[1]
            d['col_time'] = row[2]
            d['data_source'] = row[3]
            d['behavior'] = row[4]
            d['device_id'] = row[5]
            d['pic1'] = row[6]
            d['pic2'] = row[7]
            d['pic3'] = row[8]
            d['etl_time'] = row[9]
            d_json = json.dumps(d)
            #print d_json
            kafkaUtils.insert(d_json,producer,'test_realtime_track_source')

	if str(max_time) != str(last_time):
            jr.set('%s_last_time' % data_type,max_time)
	conn.close()
	cur.close()

if __name__ == '__main__':
	while True:
	    get_data('jqlk')
	    time.sleep(60)
