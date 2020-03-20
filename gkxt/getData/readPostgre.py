#-*- coding:utf-8 -*-
import psycopg2
import time
import pandas as pd
import redisUtils
import kafkaUtils
import json
import yaml
from kafka import KafkaProducer

conn = psycopg2.connect(database="postgres", user="postgres", password="zhongmeng", host="10.170.166.63", port="5432")
cur = conn.cursor()
jr = redisUtils.getRedisConn()
f = open('../conf/gkxt.yaml')
yaml_conf = yaml.load(f)
kafka_conf = yaml_conf['kafka']
spark_conf = yaml_conf['spark']

brokers = kafka_conf['broker_list']
topic = kafka_conf['from_topic']
group_id = kafka_conf['group_id']
scedule = spark_conf['schedule']
producer = KafkaProducer(bootstrap_servers = brokers.split(','))

def get_maxTime():
   max_sql = 'select MAX(logtime) maxtime from VISITOR_LIST'
   cur.execute(max_sql)
   rows = cur.fetchall()
   max_time = rows[0][0]
   return max_time

#获取小区数据，写入到kafka
def get_data(cur):
    last_time = jr.get('xq_last_time')
    #last_time = '2019-05-01 10:00:00'
    max_time = get_maxTime()
    this_time = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
    sql = '''
SELECT
	OPERATESTYLENAME card_type,
	CARDID card_num,
	to_char(LOGTIME,'yyyy-mm-dd hh24:mi:ss') col_time,
	'小区出入记录' data_source,
	'小区门禁' behavior,
	depid device_id,
	'' pic1,
	'' pic2,
	'' pic3,
	to_char(now(),'yyyy-mm-dd HH24:MM:SS') etl_time
FROM
	VISITOR_LIST
where logtime>=to_timestamp('%s','yyyy-mm-dd hh24:mi:ss') and logtime<to_timestamp('%s','yyyy-mm-dd hh24:mi:ss')
''' % (last_time,max_time)
    print sql
    cur.execute(sql)

    rows = cur.fetchall()
    print '数据条数:%d' % len(rows)
    for row in rows:
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
	#print type(d_json)
	kafkaUtils.insert(d_json,producer,'test_realtime_track_source')

    if str(max_time) != str(last_time):
        jr.set('xq_last_time',max_time)

if __name__ == '__main__':
    scedule = spark_conf['schedule']

    while True:
	if conn is None:
	    conn = psycopg2.connect(database="postgres", user="postgres", password="zhongmeng", host="10.170.166.63", port="5432")
    	    cur = conn.cursor() 
        get_data(cur)
        time.sleep(20)
