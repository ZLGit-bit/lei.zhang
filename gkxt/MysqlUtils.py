#!/usr/bin/env python
# -*- coding: utf-8


import datetime
import inspect

import pandas as pd
import yaml
from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext,HiveContext
from sqlalchemy import create_engine
import sqlalchemy
import DataProcess
def get_data(sql_context, conf, tb):
    try:
        url = conf['url']
        sql = conf[tb + '_sql']
	
        df = sql_context.read.format("jdbc").options(url=url, driver="com.mysql.jdbc.Driver", dbtable=sql,
                                                     user=conf["user"], password=conf["password"]).load()
        #df = df.dropna()
        return df
    except Exception, e:
        print "error:   ", inspect.stack()[0][1], inspect.stack()[0][3], ":", e, "\n\n\n"
        exit(-1)

def regist_tb(sql_context, conf, tb):
    #try:
        url = conf['url']

	if tb == 'union_relation':
		try:
		    sql_context.sql('select 1 from %s' % tb)
		except:
		    t_columns = ['id_num','name','card_num']
		    t_df = pd.DataFrame(columns=t_columns)
		    o_df = pd.read_sql(tb,create_engine(conf["list_url"]))
		    for col in o_df.columns:
		        if col not in['id','name','create_user','create_time','update_user','update_time','is_delete']:
			    card_df = o_df[['id_num','name','%s' % col]]
			    card_df.columns = t_columns
			    t_df = t_df.append(card_df)
		    t_df = t_df[t_df['card_num']!='0']
		    t_df = t_df.loc[t_df.card_num.notnull(),:]
		    import time
                    df = sql_context.createDataFrame(t_df)
		    df.registerTempTable(tb)
		    HiveContext(sql_context.sparkContext).cacheTable(tb)
	elif tb == 'device_info':
		d_sql = '''
		select 
		id,
		type,
		case when x(geo_position)<y(geo_position) then concat(if(y(geo_position) is null,'',y(geo_position)),'') else concat(if(x(geo_position) is null,'',x(geo_position)),'') end as lon,
                case when x(geo_position)<y(geo_position) then concat(if(x(geo_position) is null,'',x(geo_position)),'') else concat(if(y(geo_position) is null,'',y(geo_position)),'') end as lat,
                A.code,
                name,
                province_code,
                if(province is null,'',province) province,
                if(city_code is null,'',city_code) city_code,
                if(city is null,'',city) city,
                if(district_code is null,'',district_code) district_code,
                if(district is null,'',district) district,
                if(town is null,'',town) town,
                if(street is null,'',street) street,
		A.type behavior
                from device_info A
		'''
		try:
		    sql_context.sql('select 1 from device_info')
		except:
                    d_df = pd.read_sql(d_sql,create_engine(conf["list_url"]))
                    df = sql_context.createDataFrame(d_df)
		    df.registerTempTable('device_info')
		    HiveContext(sql_context.sparkContext).cacheTable('device_info')
	elif tb == conf['last_place_tb']:
		init_d = [{"name":"1","idcard":"1","cur_col_code":"1","cur_col_code_type":"1","cur_col_time":"1","cur_col_locale":"1","cur_data_source":"1","cur_behave_flag":"1","cur_long":"1","cur_lat":"1","cur_dev_code":"1","cur_dev_name":"1","cur_pic":"1","pre_col_code":"1","pre_col_code_type":"1","pre_col_time":"1","pre_col_locale":"1","pre_data_source":"1","pre_behave_flag":"1","pre_long":"1","pre_lat":"1","pre_dev_code":"1","pre_pic":"1","pre_dev_name":"1","is_delete":"1"}]
		i_df = pd.read_sql(conf['last_place_tb'],create_engine(conf["list_url"]))
		if i_df.shape[0] == 0:
		    pd_df = pd.DataFrame(init_d)
		    df = sql_context.createDataFrame(pd_df)
		else:
		    i_df['cur_col_time'] = i_df['cur_col_time'].astype('string')
		    i_df['pre_col_time'] = i_df['pre_col_time'].astype('string')
		    df = sql_context.createDataFrame(i_df,DataProcess.last_tb_struct())
		    df.registerTempTable(tb)
	else:
        	df = sql_context.read.format("jdbc").options(url=url, driver="com.mysql.jdbc.Driver", dbtable=tb,
                                 user=conf["user"], password=conf["password"]).load()
		df.registerTempTable(tb)
        #df.registerTempTable(tb)

    #except Exception, e:
	#pass
        #print "error:   ", inspect.stack()[0][1], inspect.stack()[0][3], ":", e, "\n\n\n"
        #exit(-1)

def set_data(conf):
    from sqlalchemy import create_engine
    return create_engine(conf['list_url'])

def save_last_place(conf,last_df):
    engine = set_data(conf)
    rs_df = last_df.toPandas()
    #print rs_df
    tb = conf['last_place_tb']
    tmp_tb = tb+'_temp'
    last_sql = conf['last_place_sql'] % (tb,tmp_tb,tb,tmp_tb,tb)
    #print last_sql
    try:
        trun_sql = 'truncate table controls.%s' % tmp_tb
        pd.read_sql(trun_sql,create_engine(conf["list_url"]))
    except Exception, e:
        pass

    if rs_df.shape[0]>0:
       rs_df.to_sql(tmp_tb, engine, if_exists='append', index=False,
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
				'cur_pic' : sqlalchemy.types.NVARCHAR(length=255),
				'pre_col_code' : sqlalchemy.types.NVARCHAR(length=255),
				'pre_col_code_type' : sqlalchemy.types.NVARCHAR(length=255),
				'pre_col_time' : sqlalchemy.types.NVARCHAR(length=255),
				'pre_col_locale' : sqlalchemy.types.NVARCHAR(length=255),
				'pre_data_source' : sqlalchemy.types.NVARCHAR(length=255),
				'pre_behave_flag' : sqlalchemy.types.NVARCHAR(length=255),
				'pre_long' : sqlalchemy.types.NVARCHAR(length=255),
				'pre_lat' : sqlalchemy.types.NVARCHAR(length=255),
				'pre_dev_code' : sqlalchemy.types.NVARCHAR(length=255),
				'pre_pic' : sqlalchemy.types.NVARCHAR(length=255),
				'pre_dev_name' : sqlalchemy.types.NVARCHAR(length=255),
				'is_delete' : sqlalchemy.types.NVARCHAR(length=10) })
    
    try:
	import time
	t1 = time.time()		
	#print last_sql
    	pd.read_sql(last_sql,create_engine(conf["list_url"]))
	t2 = time.time()
	print t2-t1
	print '------------------'
    except Exception, e:
        #print "error:   ", inspect.stack()[0][1], inspect.stack()[0][3], ":", e, "\n\n\n"
        pass

