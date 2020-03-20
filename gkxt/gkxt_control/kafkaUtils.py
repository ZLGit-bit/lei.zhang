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
	   insert(js,producer,topic)



def insert(data,producer,topic):
	kstr = str(str(data).decode('unicode_escape'))
	producer.send(topic,kstr)
