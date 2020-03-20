#-*- coding:utf-8 -*-
import pandas as pd
import MysqlUtils
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark import SparkConf, SparkContext
from pyspark.sql import functions as func
from pyspark.sql import SparkSession
import MysqlUtils
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils,TopicAndPartition
from pyspark.sql import Row
import yaml
import time
from pyspark.sql.types import StructType,StructField,StringType
import sys
import logging
logging.basicConfig(level=logging.WARNING,filename='/opt/bfd/gkxt/fileSaveEs/logs/readFileEs.log',filemode='a')
reload(sys)
sys.setdefaultencoding('utf-8')
f = open('/opt/bfd/gkxt/fileSaveEs/readFileEs.yaml')
yaml_conf = yaml.load(f)
spark_conf = yaml_conf['spark']
es_conf = yaml_conf['es']
es_nodes = es_conf['nodes']

sql_context = SparkSession \
        .builder \
        .appName("readFileEs") \
        .config("spark.ui.port", "5721") \
        .getOrCreate()
sc = sql_context.sparkContext
this_time = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
def registTable(tb):
	pdf = MysqlUtils.get_data(tb)
	df = sql_context.createDataFrame(pdf)
	df.registerTempTable(tb)

def res_sql(df,sql,tb):
	df.registerTempTable(tb)
        res = sql_context.sql(sql)
	return res

def getHccl():
	start = time.time()
	path='file:///opt/bfd/gkxt/fileSaveEs/data/hccl_y.csv'
	sql = spark_conf['hccl_sql']
	struct = StructType([StructField('systemid',StringType(),True),StructField('hck_systemid',StringType(),True),StructField('bzqkbh_ywxt',StringType(),True),StructField('bzqkbh_zw',StringType(),True),StructField('hccjbh_ywxt',StringType(),True),StructField('hccjbh_zw',StringType(),True),StructField('hcdw_ywxt',StringType(),True),StructField('hcdw_zw',StringType(),True),StructField('hcsbbh_ywxt',StringType(),True),StructField('hcsbbh_zw',StringType(),True),StructField('jdchpzl_ywxt',StringType(),True),StructField('jdchpzl_zw',StringType(),True),StructField('bzk_rksj',StringType(),True),StructField('bzk_gxsj',StringType(),True),StructField('bzk_zzjg',StringType(),True),StructField('bzk_scbz',StringType(),True),StructField('ywk_ssxtmc',StringType(),True),StructField('thid',StringType(),True),StructField('rksj2',StringType(),True),StructField('chllx',StringType(),True),StructField('hcdw',StringType(),True),StructField('hcsbbh',StringType(),True),StructField('bzqkbh',StringType(),True),StructField('hccjbh',StringType(),True),StructField('hcdz',StringType(),True),StructField('gkxx',StringType(),True),StructField('sc_xxrksj',StringType(),True),StructField('hcr_zjhm',StringType(),True),StructField('hcclzp',StringType(),True),StructField('jdchpzl',StringType(),True),StructField('hb_gd',StringType(),True),StructField('dqwd',StringType(),True),StructField('jdchphm',StringType(),True),StructField('sjh',StringType(),True),StructField('pg_rksj',StringType(),True),StructField('clcs',StringType(),True),StructField('rksj3',StringType(),True),StructField('bcxx',StringType(),True),StructField('dqjd',StringType(),True),StructField('hcyw',StringType(),True),StructField('hcsj',StringType(),True)])
	ori_df = (sql_context.read.csv(path=path,sep='\001',schema=struct)).coalesce(10).cache()
	res = res_sql(ori_df,sql,'hccl').coalesce(20)
	res.write.format('org.elasticsearch.spark.sql').option('es.resource','%s/%s' % ('wu_qwjs_xhc_hccl_stb','xhc_hccl_stb')).option('es.nodes',es_nodes).option('es.mapping.id','id').option('es.index.auto.create','false').mode('append').save()
	ori_df.unpersist()
	end = time.time()
	logging.warning('hccl:%s,%f' % (this_time,end-start))

def getHcry():
	start = time.time()
	path='file:///opt/bfd/gkxt/fileSaveEs/data/hcry_y.csv'
	sql = spark_conf['hcry_sql']
	struct =  StructType([StructField('bzqkbh',StringType(),True),StructField('sjh',StringType(),True),StructField('zslx_dg',StringType(),True),StructField('hcsbbh',StringType(),True),StructField('hcsj',StringType(),True),StructField('zslxbh',StringType(),True),StructField('dqwd',StringType(),True),StructField('bdfsbh_ywxt',StringType(),True),StructField('bdfsbh_zw',StringType(),True),StructField('bzqkbh_ywxt',StringType(),True),StructField('bzqkbh_zw',StringType(),True),StructField('hccjbh_ywxt',StringType(),True),StructField('hccjbh_zw',StringType(),True),StructField('hcdwdm_ywxt',StringType(),True),StructField('fddbr_qtzj_zjhm',StringType(),True),StructField('pg_rksj',StringType(),True),StructField('bdfsbh',StringType(),True),StructField('rksj2',StringType(),True),StructField('rksj3',StringType(),True),StructField('zjlx',StringType(),True),StructField('chllx',StringType(),True),StructField('pg_gxsj',StringType(),True),StructField('hcyw',StringType(),True),StructField('hcdz',StringType(),True),StructField('dqjd',StringType(),True),StructField('hb_gd',StringType(),True),StructField('hcdwdm_zw',StringType(),True),StructField('hcsbbh_ywxt',StringType(),True),StructField('hcsbbh_zw',StringType(),True),StructField('mzdm_ywxt',StringType(),True),StructField('mzdm_zw',StringType(),True),StructField('xbdm_ywxt',StringType(),True),StructField('xbdm_zw',StringType(),True),StructField('zjlx_ywxt',StringType(),True),StructField('zjlx_zw',StringType(),True),StructField('zslxbh_ywxt',StringType(),True),StructField('zslxbh_zw',StringType(),True),StructField('hccjbh',StringType(),True),StructField('clcs',StringType(),True),StructField('xm',StringType(),True),StructField('xbdm',StringType(),True),StructField('sdzs_dg',StringType(),True),StructField('hck_systemid',StringType(),True),StructField('hcdwdm',StringType(),True),StructField('csrq',StringType(),True),StructField('mzdm',StringType(),True),StructField('zz_bz',StringType(),True),StructField('yxqx',StringType(),True),StructField('gmsfhm',StringType(),True),StructField('gkxx',StringType(),True),StructField('thid',StringType(),True),StructField('sdzsbh',StringType(),True),StructField('bzk_zzjg',StringType(),True),StructField('bzk_scbz',StringType(),True),StructField('ywk_ssxtmc',StringType(),True),StructField('systemid',StringType(),True),StructField('bzk_rksj',StringType(),True),StructField('bzk_gxsj',StringType(),True),StructField('qfjg_gajgmc',StringType(),True),StructField('hcr_zjhm',StringType(),True),StructField('sc_xxrksj',StringType(),True),StructField('bcsmxx',StringType(),True)])
	ori_df = (sql_context.read.csv(path=path,sep='\001',schema=struct)).coalesce(10).cache()
	res = res_sql(ori_df,sql,'hcry').coalesce(10)
	res.write.format('org.elasticsearch.spark.sql').option('es.resource','%s/%s' % ('re_qwjs_xhc_hcry_stb','xhc_hcry_stb')).option('es.nodes',es_nodes).option('es.mapping.id','id').option('es.index.auto.create','false').mode('append').save()
	ori_df.unpersist()
	end = time.time()
	logging.warning('hcry:%s,%f' % (this_time,end-start))

def getHotel():
	dt = time.strftime('%Y',time.localtime(time.time()))
	start = time.time()
	path='file:///opt/bfd/gkxt/fileSaveEs/data/hotel_y.csv'
	sql = spark_conf['hotel_sql']
	struct = StructType([StructField('id',StringType(),True),StructField('hotelid',StringType(),True),StructField('name',StringType(),True),StructField('sex',StringType(),True),StructField('native',StringType(),True),StructField('birth',StringType(),True),StructField('certtype',StringType(),True),StructField('certvalue',StringType(),True),StructField('countycode',StringType(),True),StructField('countyname',StringType(),True),StructField('address',StringType(),True),StructField('nowcountycode',StringType(),True),StructField('nowcountyname',StringType(),True),StructField('nowaddress',StringType(),True),StructField('countyorgcode',StringType(),True),StructField('countyorgname',StringType(),True),StructField('organizecode',StringType(),True),StructField('organizename',StringType(),True),StructField('enterroom',StringType(),True),StructField('entertime',StringType(),True),StructField('canceltime',StringType(),True),StructField('creditcardtype',StringType(),True),StructField('creditcardvalue',StringType(),True),StructField('farebody',StringType(),True),StructField('faretype',StringType(),True),StructField('status',StringType(),True),StructField('groupcode',StringType(),True),StructField('groupname',StringType(),True),StructField('entercharacter',StringType(),True),StructField('fromplace',StringType(),True),StructField('fromplacename',StringType(),True),StructField('nodeid',StringType(),True),StructField('transferstime',StringType(),True),StructField('transfersname',StringType(),True),StructField('receivetime',StringType(),True),StructField('inputuserid',StringType(),True),StructField('inputtime',StringType(),True),StructField('modifyuserid',StringType(),True),StructField('modifytime',StringType(),True),StructField('datasource',StringType(),True),StructField('remark',StringType(),True),StructField('inserttime',StringType(),True),StructField('hotelname',StringType(),True),StructField('hoteladdress',StringType(),True),StructField('station',StringType(),True),StructField('county',StringType(),True),StructField('insection',StringType(),True),StructField('iddatefrom',StringType(),True),StructField('iddateto',StringType(),True),StructField('idunit',StringType(),True),StructField('transferstime2',StringType(),True),StructField('receivetime2',StringType(),True),StructField('inserttime2',StringType(),True),StructField('transfersname2',StringType(),True),StructField('zjyxq',StringType(),True),StructField('certvalue_ktl',StringType(),True),StructField('rzty',StringType(),True),StructField('rksj',StringType(),True),StructField('f_transmit',StringType(),True),StructField('yingyezhizhao',StringType(),True),StructField('jwzrqid',StringType(),True),StructField('jwzrq',StringType(),True),StructField('jglevel',StringType(),True),StructField('ruzhudanhao',StringType(),True),StructField('gabcode',StringType(),True),StructField('gattxzh',StringType(),True),StructField('txzqfcs',StringType(),True),StructField('transmit',StringType(),True),StructField('t_field',StringType(),True)])
	ori_df = (sql_context.read.csv(path=path,sep='\001',schema=struct)).coalesce(10).cache()
	res = res_sql(ori_df,sql,'hotel').coalesce(10)
	#res.show()
	res.write.format('org.elasticsearch.spark.sql').option('es.resource','%s/%s' % ('re_qwjs_lgza_gnlkb_stb_new-%s' % dt,'lgza_gnlkb_stb_new')).option('es.nodes',es_nodes).option('es.mapping.id','id').option('es.index.auto.create','false').mode('append').save()
	ori_df.unpersist()
	end = time.time()
	logging.warning('hotel:%s,%f' % (this_time,end-start))

if __name__ == '__main__':
	start = time.time()
	registTable('yn_dict')
	registTable('xzqhmc')
	getHotel()
	getHcry()
	getHccl()	
	end = time.time()
	logging.warning('用时:%f' % (end-start))
