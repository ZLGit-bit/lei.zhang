#-*- coding:utf-8 -*-
import yaml
from kafka import KafkaProducer
import json
import kafkaUtils
import time
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
f = open('./conf/gkxt.yaml')
yaml_conf = yaml.load(f)
spark_conf = yaml_conf['spark']
mysql_conf = yaml_conf['mysql']
kafka_conf = yaml_conf['kafka']


def main():
    brokers = kafka_conf['broker_list']
    topic = kafka_conf['from_topic']
    group_id = kafka_conf['group_id']
    producer = KafkaProducer(bootstrap_servers = brokers.split(','))
    k_data = u'{"device_district": null, "col_time": "2019-05-05 18:30:21", "person_type":"重点人员","card_num": "鲁B66666", "name": "聚集预警测试", "device_location": null, "idcard": "7777777777", "pic1": "", "pic2": "", "device_name": null, "card_type": null, "etl_time": "2019-05-05 10:19:23", "data_source": "过车数据", "device_province": null, "behavior": "聚集预警", "device_street": null, "device_city": null, "pic3": "", "id": "c6a8e83a609c03c96377bba688f44b03", "device_town": null, "device_id": "j1","province_code":"","city_code":"","district_code":"","jd":"","wd":"","person_type":"重大刑事前科人员"}'
    for x in range(4):
	s_data = k_data
        kafkaUtils.insert(s_data,producer,'realtime_track4')
        time.sleep(1)

if __name__ == '__main__':
	import os
	print os.getcwd()
