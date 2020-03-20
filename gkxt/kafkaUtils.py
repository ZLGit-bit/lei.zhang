# -*- coding: utf-8 -*-
from kafka import KafkaProducer
import json
import sys
reload(sys)
sys.setdefaultencoding('utf-8')


def sendData(data,producer,kafka_conf):
	brokers = kafka_conf['broker_list']
	topic = kafka_conf['to_topic']
	if producer is None:
	   producer = KafkaProducer(bootstrap_servers = brokers.split(','))
	for js in json.loads(data):
	   js = json.dumps(js)
	   insert(js,producer,topic)



def insert(data,producer,topic):
	#print type(data)
	#kstr = str(str(data).decode('unicode_escape'))
	data = data.encode('utf-8')
	producer.send(topic,data)
