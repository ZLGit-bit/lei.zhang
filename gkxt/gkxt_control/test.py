# -*- coding:utf-8 -*-
import MysqlUtils
from redisUtils import RedisUtils
import json
import yaml
import redis
import time
from sqlalchemy import create_engine
import sqlalchemy
import datetime
import MysqlUtils
import pandas as pd
import time
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

f = open('/opt/bfd/gkxt/gkxt_control/conf/control.yaml')
yaml_conf = yaml.load(f)
mysql_conf = yaml_conf['mysql']
spark_conf = yaml_conf['spark']
kafka_conf = yaml_conf['kafka']
redis_conf = yaml_conf['redis']
brokers = kafka_conf['broker_list']
topic = kafka_conf['from_topic']
group_id = kafka_conf['group_id']
redisUtil = RedisUtils(redis_conf)

if __name__ == '__main__':
    print MysqlUtils.judge_table_update(mysql_conf,redis_conf,'control_info')
    #redisUtil = RedisUtils(redis_conf)
    #conn = redisUtil.getRedisConn()
    #conn.delete('control_info_update_time')
