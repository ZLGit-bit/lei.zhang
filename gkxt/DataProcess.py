# -*- coding:utf-8 -*-
from pyspark.sql.types import StructType,StructField,StringType
from pyspark.sql import functions
import time
import sys
reload(sys)
sys.setdefaultencoding('utf-8')


#数据整理成elasticsearch所需要的结构
def es_data_clgj(rs_df):
        t_df = rs_df.select(rs_df['id'],rs_df['car_type'],rs_df['name'],rs_df['idcard'],rs_df['cur_col_code'],rs_df['cur_col_code_type'],rs_df['cur_col_time'],rs_df['cur_data_source'],rs_df['cur_behave_flag'],rs_df['cur_dev_code'],rs_df['cur_dev_name'],rs_df['pic1'],rs_df['pic2'],rs_df['pic3'],rs_df['etl_time'],rs_df['device_location'],rs_df['device_province'],rs_df['device_city'],rs_df['device_district'],rs_df['device_town'],rs_df['device_street'],rs_df['device_loc_str'],rs_df['brand'],rs_df['sub_brand'])
	#if t_df.rdd.isEmpty():
	#    return 0
	dt = str(time.strftime('%Y-%m',time.localtime(time.time())))
	t_df = t_df.where("length(cur_col_time)>=10")
        t_df = t_df.selectExpr('id','car_type','name','idcard','cur_col_code as card_num','cur_col_code_type as card_type','cur_col_time as col_time','cur_data_source as data_source','cur_behave_flag as behavior','cur_dev_code as device_id','cur_dev_name as device_name','pic1','pic2','pic3','etl_time','device_location','device_loc_str','device_province','device_city','device_district','device_town','device_street','brand as car_brand','sub_brand as car_brand_subclass')
	t_df = t_df.withColumn('cpzl',functions.lit(''))
        return t_df

#数据整理成elasticsearch所需要的结构
def es_data(rs_df):
        t_df = rs_df.select(rs_df['id'],rs_df['car_type'],rs_df['name'],rs_df['idcard'],rs_df['cur_col_code'],rs_df['cur_col_code_type'],rs_df['cur_col_time'],rs_df['cur_data_source'],rs_df['cur_behave_flag'],rs_df['cur_dev_code'],rs_df['cur_dev_name'],rs_df['pic1'],rs_df['pic2'],rs_df['pic3'],rs_df['etl_time'],rs_df['device_location'],rs_df['device_province'],rs_df['device_city'],rs_df['device_district'],rs_df['device_town'],rs_df['device_street'],rs_df['device_loc_str'])
        #if t_df.rdd.isEmpty():
        #    return 0
        dt = str(time.strftime('%Y-%m',time.localtime(time.time())))
        t_df = t_df.where("length(cur_col_time)>=10")
        t_df = t_df.selectExpr('id','car_type','name','idcard','cur_col_code as card_num','cur_col_code_type as card_type','cur_col_time as col_time','cur_data_source as data_source','cur_behave_flag as behavior','cur_dev_code as device_id','cur_dev_name as device_name','pic1','pic2','pic3','etl_time','device_location','device_loc_str','device_province','device_city','device_district','device_town','device_street')
        t_df = t_df.withColumn('cpzl',functions.lit(''))
        return t_df

#数据整理成mysql所需要的结构
def mysql_data(rs_df):
        m_df = rs_df.select(rs_df['name'],rs_df['idcard'],rs_df['cur_col_code'],rs_df['cur_col_code_type'],rs_df['cur_col_time'],rs_df['cur_col_locale'],rs_df['cur_data_source'],rs_df['cur_behave_flag'],rs_df['cur_long'],rs_df['cur_lat'],rs_df['cur_dev_code'],rs_df['pic1'],rs_df['cur_dev_name'],rs_df['pre_col_code'],rs_df['pre_col_code_type'],rs_df['pre_col_time'],rs_df['pre_col_locale'],rs_df['pre_data_source'],rs_df['pre_behave_flag'],rs_df['pre_long'],rs_df['pre_lat'],rs_df['pre_dev_code'],rs_df['pre_pic'],rs_df['pre_dev_name'],rs_df['rank'])
        m_df = m_df.where("rank=1 and name is not null and cur_long is not null and cur_long!=''")
        m_df = m_df.drop('rank')
	m_df = m_df.withColumnRenamed('pic1','cur_pic')
	m_df = m_df.withColumn('is_delete',functions.lit('0'))
        return m_df

#把数据处理成kafka所需要的机构
def kafka_data(rs_df):
        t_df = rs_df.select(rs_df['name'],rs_df['idcard'],rs_df['person_type'],rs_df['dev_id'],rs_df['cur_col_code'],rs_df['cur_col_code_type'],rs_df['cur_col_time'],rs_df['data_source'],rs_df['cur_behave_flag'],rs_df['cur_dev_code'],rs_df['cur_dev_name'],rs_df['pic1'],rs_df['pic2'],rs_df['pic3'],rs_df['etl_time'],rs_df['cur_long'],rs_df['cur_lat'],rs_df['province_code'],rs_df['device_province'],rs_df['city_code'],rs_df['device_city'],rs_df['district_code'],rs_df['device_district'],rs_df['device_town'],rs_df['device_street'])
        t_df = t_df.selectExpr('name','idcard','person_type','cur_col_code as card_num','cur_col_code_type as card_type','cur_col_time as col_time','data_source as data_source','cur_behave_flag as behavior','cur_dev_code as device_id','cur_dev_name as device_name','pic1','pic2','pic3','etl_time','cur_long as jd','cur_lat wd','province_code','device_province','city_code','device_city','district_code','device_district','device_town','device_street','dev_id')

        t_df =t_df.where('name is not null')
        #print t_df.toPandas()
        k_df = t_df.toPandas().to_json(orient='records',lines=False)
        return k_df

#源数据的结构
def sourceStruct():
	struct = StructType([StructField("card_type",StringType(),True),StructField("car_type",StringType(),True),StructField("card_num",StringType(),True),StructField("col_time",StringType(),True),StructField("data_source",StringType(),True),StructField("behavior",StringType(),True),StructField("device_id",StringType(),True),StructField("device_name",StringType(),True),StructField("pic1",StringType(),True),StructField("pic2",StringType(),True),StructField("pic3",StringType(),True),StructField("etl_time",StringType(),True)])
	return struct


def last_tb_struct():
	struct = StructType([StructField("name",StringType(),True),StructField("idcard",StringType(),True),StructField("cur_col_code",StringType(),True),StructField("cur_col_code_type",StringType(),True),StructField("cur_col_time",StringType(),True),StructField("cur_col_locale",StringType(),True),StructField("cur_data_source",StringType(),True),StructField("cur_behave_flag",StringType(),True),StructField("cur_long",StringType(),True),StructField("cur_lat",StringType(),True),StructField("cur_dev_code",StringType(),True),StructField("cur_dev_name",StringType(),True),StructField("cur_pic",StringType(),True),StructField("pre_col_code",StringType(),True),StructField("pre_col_code_type",StringType(),True),StructField("pre_col_time",StringType(),True),StructField("pre_col_locale",StringType(),True),StructField("pre_data_source",StringType(),True),StructField("pre_behave_flag",StringType(),True),StructField("pre_long",StringType(),True),StructField("pre_lat",StringType(),True),StructField("pre_dev_code",StringType(),True),StructField("pre_pic",StringType(),True),StructField("pre_dev_name",StringType(),True),StructField("is_delete",StringType(),True)])
	return struct
