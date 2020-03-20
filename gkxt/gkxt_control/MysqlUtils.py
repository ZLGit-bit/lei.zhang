#!/usr/bin/env python
# -*- coding: utf-8


import datetime
import inspect

import pandas as pd
import yaml
from redisUtils import RedisUtils
#from pyspark import SparkConf, SparkContext
#from pyspark.sql import HiveContext
from pyspark.sql import HiveContext,HiveContext
from sqlalchemy import create_engine,text
import sqlalchemy
def get_data(conf, tb):
    try:
	df = pd.read_sql(tb,create_engine(conf["list_url"]))
        return df
    except Exception, e:
        print "error:   ", inspect.stack()[0][1], inspect.stack()[0][3], ":", e, "\n\n\n"
        exit(-1)

def get_data_spark(sql_context, conf, tb):
    try:
        url = conf['url']

        df = sql_context.read.format("jdbc").options(url=url, driver="com.mysql.jdbc.Driver", dbtable=tb,
                                                     user=conf["user"], password=conf["password"]).load()
        return df
    except Exception, e:
        print "error:   ", inspect.stack()[0][1], inspect.stack()[0][3], ":", e, "\n\n\n"
        exit(-1)

def judge_table_update(mysql_conf,redis_conf,tb):
    redisUtil = RedisUtils(redis_conf)
    flag = False
    sql = "select concat(update_time) update_time from information_schema.tables where table_schema='controls' and table_name='%s'" % tb
    try:
        t_df = pd.read_sql(sql,create_engine(mysql_conf["list_url"]))
	this_time = t_df.loc[:1]['update_time']
	up_time= this_time.tolist()[0]
	last_time = redisUtil.getRedis('%s_update_time' % tb)
	print last_time
	redisUtil.setRedis('%s_update_time' % tb,up_time)
        if last_time == up_time:
	    flag = True

    except Exception, e:
        print "error:   ", inspect.stack()[0][1], inspect.stack()[0][3], ":", e, "\n\n\n"
        exit(-1)
    return flag	
    
#spark注册表
def regist_tb(sql_context, m_conf,r_conf, tb):
    try:
        sql_context.sql('select 1 from %s' % tb)
	r_flag = judge_table_update(m_conf,r_conf,tb)
	if not r_flag:
            t_df = get_data_spark(sql_context,m_conf,tb)
	    t_df.registerTempTable(tb)
    except:
	t_df = get_data_spark(sql_context,m_conf,tb)
	#t_df.show()
	r_flag = judge_table_update(m_conf,r_conf,tb)
	if not r_flag:
            t_df.registerTempTable(tb)
	else:
	    t_df.registerTempTable(tb)
            HiveContext(sql_context.sparkContext).cacheTable(tb)
	

def set_data(conf):
    from sqlalchemy import create_engine
    return create_engine(conf['list_url'])

def save_last_place(conf,last_df):
    engine = set_data(conf)
    rs_df = last_df.toPandas()
    tb = conf['last_place_tb']
    tmp_tb = tb+'_temp'
    last_sql = conf['last_place_sql'] % (tmp_tb,tb,tmp_tb,tb,tmp_tb,tb)
    #print last_sql
    if rs_df.shape[0]>0:
       rs_df.to_sql(tmp_tb, engine, if_exists='replace', index=False,
                             dtype={'name' : sqlalchemy.types.NVARCHAR(length=255),
				'idcard' : sqlalchemy.types.NVARCHAR(length=255),
				'cur_col_code' : sqlalchemy.types.NVARCHAR(length=255),
				'cur_col_code_type' : sqlalchemy.types.NVARCHAR(length=255),
				'cur_col_time' : sqlalchemy.types.NVARCHAR(length=255),
				'cur_col_locale' : sqlalchemy.types.NVARCHAR(length=255),
				'cur_data_source' : sqlalchemy.types.NVARCHAR(length=255),
				'cur_behave_flag' : sqlalchemy.types.NVARCHAR(length=255),
				'cur_long' : sqlalchemy.types.NVARCHAR(length=255),
				'cur_lat' : sqlalchemy.types.NVARCHAR(length=255),
				'cur_dev_code' : sqlalchemy.types.NVARCHAR(length=255),
				'cur_dev_name' : sqlalchemy.types.NVARCHAR(length=255),
				'pre_col_code' : sqlalchemy.types.NVARCHAR(length=255),
				'pre_col_code_type' : sqlalchemy.types.NVARCHAR(length=255),
				'pre_col_time' : sqlalchemy.types.NVARCHAR(length=255),
				'pre_col_locale' : sqlalchemy.types.NVARCHAR(length=255),
				'pre_data_source' : sqlalchemy.types.NVARCHAR(length=255),
				'pre_behave_flag' : sqlalchemy.types.NVARCHAR(length=255),
				'pre_long' : sqlalchemy.types.NVARCHAR(length=255),
				'pre_lat' : sqlalchemy.types.NVARCHAR(length=255),
				'pre_dev_code' : sqlalchemy.types.NVARCHAR(length=255),
				'pre_dev_name' : sqlalchemy.types.NVARCHAR(length=255) })

 
def saveAlarmInfo(conf,df):
		df = df.drop_duplicates()
		engine = set_data(conf)
		df.loc[df['lon'] == 'None','lon'] = ''
		df.loc[df['lat'] == 'None','lat'] = ''
		df['emergency_grade'] = '1'
		df['policeman_state'] = '1'
                if df.shape[0]>0:
                       try:
                           trun_sql = 'truncate table controls.%s_tmp' % conf['alarm_info_tb']
                           pd.read_sql(text(trun_sql),create_engine(conf["list_url"]))
                       except Exception, e:
                           #print "error:   ", inspect.stack()[0][1], inspect.stack()[0][3], ":", e, "\n\n\n"
                           pass

                       df.to_sql('%s_tmp' % conf['alarm_info_tb'], engine, if_exists='append', index=False,
                             dtype={'emergency_date' : sqlalchemy.types.NVARCHAR(length=255),
                                'device_id' : sqlalchemy.types.NVARCHAR(length=255),
                                'object_business_code' : sqlalchemy.types.NVARCHAR(length=255),
				'plan_target_id' : sqlalchemy.types.NVARCHAR(length=255),
				'person_idcard_num' : sqlalchemy.types.NVARCHAR(length=255),
				'leader_state' : sqlalchemy.types.Integer,
                                'emergency_source' : sqlalchemy.types.Integer,
                                'emergency_reason' : sqlalchemy.types.NVARCHAR(length=255),
                                'policeman_state' : sqlalchemy.types.Integer,
                                'lon' : sqlalchemy.types.NVARCHAR(length=255),
                                'lat' : sqlalchemy.types.NVARCHAR(length=255),
                                'plan_id' : sqlalchemy.types.NVARCHAR(length=255),
                                'control_id' : sqlalchemy.types.NVARCHAR(length=255),
                                'emergency_location' : sqlalchemy.types.NVARCHAR(length=255),
                                'emergency_grade' : sqlalchemy.types.NVARCHAR(length=255),
                                'city' : sqlalchemy.types.NVARCHAR(length=255),
                                'city_code' : sqlalchemy.types.NVARCHAR(length=255),
                                'district' : sqlalchemy.types.NVARCHAR(length=255),
                                'district_code' : sqlalchemy.types.NVARCHAR(length=255),
                                'create_user' : sqlalchemy.types.NVARCHAR(length=255),
                                'create_time' : sqlalchemy.types.NVARCHAR(length=255),
                                'update_user' : sqlalchemy.types.NVARCHAR(length=255),
                                'update_time' : sqlalchemy.types.NVARCHAR(length=255)   ,
                                'is_delete' : sqlalchemy.types.Integer,
				'trail_source':sqlalchemy.types.NVARCHAR(length=10)
				})
	
		try:
		    update_sql = conf['alarm_update_sql']
                    pd.read_sql(text(update_sql),create_engine(conf["list_url"]))
                except Exception, e:
		    #print "error:   ", inspect.stack()[0][1], inspect.stack()[0][3], ":", e, "\n\n\n"
                    pass

#触境频次表
def saveTouchFreq(conf,df):
	engine = set_data(conf)
	if df.shape[0]>0:
		df = df[['control_id','plan_id','target_id','idcard','set_num']]
		tmp_tb = conf['touch_freq_tb']+'_temp'
		sql = 'truncate table %s' % tmp_tb
		
		df.to_sql(conf['touch_freq_tb']+'_temp', engine, if_exists='append', index=False,
                             dtype={'control_id' : sqlalchemy.types.NVARCHAR(length=255),
                                'plan_id' : sqlalchemy.types.NVARCHAR(length=255),
                                'target_id' : sqlalchemy.types.NVARCHAR(length=255),
				'icard': sqlalchemy.types.NVARCHAR(length=255),
                                'set_num' : sqlalchemy.types.NVARCHAR(length=255)})

	try:	
		print 'saveTouchFreq'
		pd.read_sql(conf['touch_freq_sql'],create_engine(conf["list_url"]))
	except  Exception, e:
		pass
