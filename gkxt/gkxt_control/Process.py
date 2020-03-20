# -*- coding:utf-8 -*-
import json
import datetime
import time
import pandas as pd
from sqlalchemy import create_engine
import sys
import os
import MysqlUtils
import yn_place_list
from redisUtils import RedisUtils
from esUtils import EsUtils
reload(sys)
sys.setdefaultencoding('utf-8')

class ControlProcess:
	def __init__(self,conf,sql_context):
		self.sql_context = sql_context
		self.conf = conf
		self.redisUtil = RedisUtils(conf['redis'])
		self.esUtil = EsUtils(conf['es'])

	#判断方法是否执行,如果继续执行返回数据时间和规则时间,不执行返回0
	def judgeExe(self,data,rule):
                prog_rule = rule['prog_rule']
                start_time = str(rule['start_time'])
                end_time = str(rule['end_time'])
                col_time = str(data['col_time'])
		#数据时间如果不在布控范围内，则跳过
                if (col_time>=start_time and col_time<=end_time) or start_time == 'NaT' or end_time == 'None' or end_time is None:
		    try:
                        col_h = datetime.datetime.strftime(datetime.datetime.strptime(col_time,'%Y-%m-%d %H:%M:%S'),'%H:%M')
		    except:
		        return 0
                    prog_rule_json = json.loads(str(prog_rule))
                    control_times = prog_rule_json['date_time']
		    return control_times,col_h
		else:
		    return 0
	
	def listappend(self,data_l,rule_l,lr):
		if lr!=0 and len(lr)>0:
		    for data in lr:
		        data_l.append(data[1])
                        rule_l.append(data[0])
		return data_l,rule_l

	#遍历每条数据和每个规则
	def executeFunc(self,data,rule):
		print 'start->execute'
		data_l = []
		rule_l = []
		data_columns = ['behavior','card_num','card_type','city_code','col_time','data_source','dev_id','device_city','device_district','device_id','device_name','device_province','device_street','device_town','district_code','etl_time','idcard','jd','name','person_type','pic1','pic2','pic3','province_code','wd']
		rule_columns = ['control_id','control_name','prog_id','name','person_type','target_id','idcard','last_device_id','last_col_time','police','control_prog_id','control_prog','prog_target','prog_rule','target_type','target_json','start_time','end_time','leader_state','emergency_grade','create_user','create_time','update_user','update_time','is_delete','duty_organ']
		gather_r_df = rule[rule['control_prog_id'].isin(['5'])]
		pos_r_df = rule[rule['control_prog_id'].isin(['1','11'])]
		per_r = rule[rule['control_prog_id'].isin(['10','2','3','4','6','7','8'])]
		noTrack_r = rule[rule['control_prog_id'].isin(['9'])]
		per_d = data.merge(per_r,on=['idcard'],how='inner',suffixes=['','_r'])
		per_df = per_d[data_columns]
		noTrack_d = noTrack_r.merge(data,on=['idcard'],how='left',suffixes=['','_r'])
		device_df = (MysqlUtils.get_data(self.conf['mysql'],'device_info'))[['id','code']]
		t1 = time.time()
		#print rule
		for index,row_data in per_d.iterrows():
		        #for index,row in per_r.iterrows():
			func_type = str(row_data['control_prog_id'])
			r_idcard = row_data['idcard']
			i_data = row_data[data_columns]
			row = row_data[rule_columns]
			#print row_data
			#1:离开区域,2:票务预警,3:住宿预警,4:上网预警,5:人员聚集,6:进入区域,7:进入区县,8:随动随报,9:轨迹消失,10:离开阵地,11:进入阵地
		        if func_type in ['10']:
			    lr = self.leaveArea(i_data,row)
			    data_l,rule_l = self.listappend(data_l,rule_l,lr)
			if func_type in ['6','7']:
			    ir = self.intoArea(i_data,row)
			    data_l,rule_l = self.listappend(data_l,rule_l,ir)
			if func_type in ['8']:
			    ar = self.appearAlarm(i_data,row)
			    data_l,rule_l = self.listappend(data_l,rule_l,ar)
			if func_type in ['2','3','4']:
			    br = self.behaviorAlarm(i_data,row)
			    data_l,rule_l = self.listappend(data_l,rule_l,br)
	   	        for p_i,p_row in pos_r_df.iterrows():
                            pr = self.positionAlarm(i_data,p_row)
                            data_l,rule_l = self.listappend(data_l,rule_l,pr) 

		t2 = time.time()
		for index,no_track_data in noTrack_d.iterrows():
		    n_data = no_track_data[data_columns]
		    r_d = no_track_data[rule_columns]
                    pr = self.noTrackAlarm(n_data,r_d,device_df)
                    data_l,rule_l = self.listappend(data_l,rule_l,pr)
		t3 = time.time()
		for index,g_data in data.iterrows():
                    for g_i,g_d in gather_r_df.iterrows():
                        gr = self.gatherAlarm(g_data,g_d)
		t4 = time.time()
		#更新最后一次位置
		self.setLastPlace(data)
		rule_df = pd.DataFrame(rule_l)	
		data_df = pd.DataFrame(data_l)
		print 't2:%f' % (t2-t1)
		print 't3:%f' % (t3-t2)
		print 't4:%f' % (t4-t3)
		if data_df.shape[0]>0:
		    self.updateFreq(rule_df,data_df)
		t5 = time.time()
		if data.shape[0]>0:
		    self.judgeAlarmInfo(gather_r_df)
		t6 = time.time()
		print 't5:%f' % (t5-t4)
		print 't6:%f' % (t6-t5)

	#根据redis存储的结果，判断是否触发聚集
	def judgeAlarmInfo(self,rule):
		print 'gather_start'
		r_l = []
                if rule.shape[0]>0:
                    gather_data_df = pd.DataFrame()
                    gather_rule_df = pd.DataFrame()
                    for ind,r_per in rule.iterrows():
                        target_id = r_per['target_id']
                        redis_id = 'target_id_%s' % target_id
                        redis_idcard = r_per['idcard']
                        prog_rule_json = json.loads(str(r_per['prog_rule']))
                        people_number = prog_rule_json['people_number']
			if people_number == 'None' or people_number == None:
			    people_number = 2
                        stay_time = prog_rule_json['stay_time']
			if stay_time == 'None' or stay_time == None:
			    stay_time = 5
			r_l.append(self.ruleRes(r_per))
                        #从redis中获取在区域内停留的所有人的数据
                        r_data = self.redisUtil.getGatherInfo(redis_id,stay_time)
			print 'target_id:%s,%d,%d' % (redis_id,len(r_data),int(people_number))
			#print redis_id
                        if self.redisUtil.existsKey(redis_id,redis_idcard):
                            if len(r_data)>=int(people_number):
                                gather_data = pd.DataFrame(r_data)
                                #print gather_data
                                gather_rule = pd.DataFrame(r_l)
				#print gather_rule.head(10)
                                #判断布控人和方案目标是否在停留区域名单中，如果存在更新预警频次
                                #print target_id
                                gather_data_df = gather_data[gather_data['idcard'].isin([redis_idcard])]
                    if gather_data_df.shape[0]>0:
			print 'gather_data'
                        gather_data_df['col_time'] = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
			#gather_rule.rename(columns={'prog_id':'plan_id'},inplace=True) 
                        self.updateFreq(gather_rule,gather_data_df)
	
	#获取每个人的最后一次位置
	def getLastPlace(self,idcard,rule):
		last_dict = self.redisUtil.getRedisValue('control_last_place',idcard)
		last_device_id = rule['last_device_id']
		if last_dict != 0:
		    last_device_id = last_dict['dev_id']
		return last_device_id
	
	#更新每个人的最后一次位置
	def setLastPlace(self,data):
		for index,row in data.iterrows():
		    idcard = row['idcard']
		    v_df = row
		    v_df = v_df.to_dict()
		    value = json.dumps(v_df)
		    #print data
		    self.redisUtil.setRedisData('control_last_place',idcard,value)

	#规范化开始和结束时间格式
	def getTime(self,json_value):
		start_h = json_value['start_time'].replace(' ','')
		end_h = json_value['end_time'].replace(' ','')
		if len(start_h.split(':')[0])== 1:
		    start_h = '0%s' % start_h
                if len(end_h.split(':')[0])== 1:
                    end_h = '0%s' % end_h
		return start_h,end_h
	    
	#离开区域
	def leaveArea(self,data,rule):
		#print data
        	idcard = data['idcard']
		res = self.judgeExe(data,rule)
		rs = []
		if res == 0 or rule['idcard'] != idcard:
			return 0
		control_times = res[0]
		col_h = res[1]
		target_name = rule['prog_target']
                device_name = data['device_name']
		device_city = data['device_city']
		last_device_id = self.getLastPlace(idcard,rule)
		#print last_device_id
        	for json_key in control_times:
                    if json_key == 'value':
		        for json_value in control_times[json_key]:
			    j_time = self.getTime(json_value)  
                            start_h = j_time[0]
                            end_h = j_time[1]
			    #数据时间要在规则设定时间内,而且此人的上次位置在区域内，此次位置不在区域内
                            if (col_h >= start_h and col_h<=end_h):
				#区域内的全部设备id
				#print col_h
                                dev_list = str(rule['target_json']).replace('[','').replace(']','').split(',')
                                device_id = str(data['dev_id'])
				flag1 = False
				flag2 = False
				#判断有没有离开云南
				if  target_name == '云南省':
				    if '->' in device_name:
				        #print '离开云南'
				        #print device_name
				        devices = device_name.split('->')
				        ksdd = devices[0]
				        jsdd = devices[1]
				
				        flag1 = yn_place_list.yn_flag(ksdd)
				        flag2 = yn_place_list.yn_flag(jsdd)
					flag3 = yn_place_list.yn_flag(device_city)
				        if (not flag1) or (not flag2) or (not flag3):
					    print device_name
					    rs.append((self.ruleRes(rule),self.dataRes(rule,data)))
				else:
                                    if ((device_id not in dev_list ) and (last_device_id in dev_list)):
                                        rs.append((self.ruleRes(rule),self.dataRes(rule,data)))
				        print 'leaveArea'
                            else:
				pass
		return rs

        #进入区域
        def intoArea(self,data,rule):
                idcard = data['idcard']
                res = self.judgeExe(data,rule)
		rs = []
                if res == 0 or rule['idcard'] != idcard:
                        return 0
                control_times = res[0]
                col_h = res[1]
		target_name = rule['prog_target']
		device_name = data['device_name']
		device_city = data['device_city']
		duty_organ = rule['duty_organ']
		behavior = data['behavior']
		behavior_dict = {"机场":"8,20,21","高铁":"7,22,23","火车站":"7,22,23","政府":"25"}
		last_device_id = self.getLastPlace(idcard,rule)
		#print target_name
                for json_key in control_times:
                    if json_key == 'value':
                        for json_value in control_times[json_key]:
                            j_time = self.getTime(json_value)
                            start_h = j_time[0]
                            end_h = j_time[1]
			    #数据时间要在规则设定时间内,而且此人的上次位置不在区域内，此次位置在区域内
                            if (col_h >= start_h and col_h<=end_h):
				#区域内的全部设备id
                                dev_list= str(rule['target_json']).replace('[','').replace(']','').split(',')
                                device_id = data['dev_id']
				#进入机场
				if target_name == u'昆明长水机场' and behavior in ['8','20','21']:
				    if '->长水机场' in device_name or '->昆明' in device_name:
				        rs.append((self.ruleRes(rule),self.dataRes(rule,data)))
					return rs
				#进入昆明站 昆明南站 
                                if target_name in [u'昆明站','昆明南站'] and behavior in ['7','22','23']:
                                    if '->昆明' in device_name:
                                        rs.append((self.ruleRes(rule),self.dataRes(rule,data)))
					return rs
				#进入政府
                                if target_name == u'云南省人民政府' and behavior in ['25']:
                                    if '政府' in device_name:
                                        rs.append((self.ruleRes(rule),self.dataRes(rule,data)))
					return rs
				#到达北京
				if target_name == u'北京市':
				    if ("->北京") in device_name:
					rs.append((self.ruleRes(rule),self.dataRes(rule,data)))
					return rs
				#到达昆明
				if target_name == '昆明市':
				    if '->' in device_name:
				        if (('昆明' not in duty_organ) and ((('->昆明') in device_name))):
				            rs.append((self.ruleRes(rule),self.dataRes(rule,data)))
				            return rs
				    else:
					if (('昆明' not in duty_organ) and  (target_name in device_city)):
					    rs.append((self.ruleRes(rule),self.dataRes(rule,data)))
					    return rs

				#常规进入区域规则
                                if ((device_id in dev_list) and (last_device_id not in dev_list)):
                                    rs.append((self.ruleRes(rule),self.dataRes(rule,data)))
				    print 'intoArea'
                                else:
                                    pass
		return rs	

	#随动随报
	def appearAlarm(self,data,rule):
		idcard = data['idcard']
                res = self.judgeExe(data,rule)
		rs = []
		rule['idcard'] != idcard
                if res == 0 or rule['idcard'] != idcard:
                    return 0
                control_times = res[0]
                col_h = res[1]
		behavior = data['data_source']
		target_name = rule['prog_target']
		if behavior != target_name:
		    return 0
                for json_key in control_times:
                    if json_key == 'value':
                        for json_value in control_times[json_key]:
                            j_time = self.getTime(json_value)
                            start_h = j_time[0]
                            end_h = j_time[1]
			    #数据时间要在规则设定时间内,如果目标类型行为在布控行为中,该行为类型的数据全部预警
                            if (col_h >= start_h and col_h<=end_h):
                                if behavior == target_name:
                                    rs.append((self.ruleRes(rule),self.dataRes(rule,data)))
				    print 'appearAlarm'
                                else:
                                    pass
		return rs

	#行为预警:票务预警,上网预警,住宿预警
	def behaviorAlarm(self,data,rule):
		idcard = data['idcard']
                res = self.judgeExe(data,rule)
		rs = []
                if res == 0 or rule['idcard'] != idcard:
                        return 0
                control_times = res[0]
                col_h = res[1]
		target_type = rule['prog_id']
		prog_target = rule['prog_target']
		data_type = data['data_source']
                for json_key in control_times:
                    if json_key == 'value':
                        for json_value in control_times[json_key]:
                            j_time = self.getTime(json_value)
                            start_h = j_time[0]
                            end_h = j_time[1]
			    dev_list= str(rule['target_json']).replace('[','').replace(']','').split(',')
                            device_id = data['dev_id']
			    flag_hc = False
			    flag_qc = False
			    flag_jp = False
			    #print 'tocket_alarm'
			    #print data_type
			    if ('火车' in str(data_type)) or ('铁路' in str(data_type)):
				if str(prog_target) == '火车票':
				    flag_hc = True
                            if '客运' in str(data_type):
                                if str(prog_target) == '汽车票':
                                    flag_qc = True
                            if '民航' in str(data_type):
                                if str(prog_target) == '机票':
                                    flag_jp = True

			    if (flag_hc or flag_qc or flag_jp) and str(target_type)=='2' and data_type == prog_target:
				rs.append((self.ruleRes(rule),self.dataRes(rule,data)))
				return rs
			    #数据时间要在规则设定时间内
                            if col_h >= start_h and col_h<=end_h:
				#如果票务目标等于布控票务目标,则预警
				if (prog_target == data_type) and str(target_type)=='2':
                                    rs.append((self.ruleRes(rule),self.dataRes(rule,data)))
			            print 'behaviorAlarm_ticket'
				#如果出现在布控范围内则预警
				elif device_id in dev_list and str(target_type)=='3':
				    rs.append((self.ruleRes(rule),self.dataRes(rule,data)))
                                    print 'behaviorAlarm_hotel_intenet'
				else:
				    pass
		            else:
			        pass
		return rs

        #阵地预警
        def positionAlarm(self,data,rule):
		#print data
                res = self.judgeExe(data,rule)
		rs = []
                if res == 0:
                    return 0
                control_times = res[0]
                col_h = res[1]
		idcard = data['idcard']
		#print data
		data_person_type = data['person_type']
		rule_person_type = rule['person_type']
                target_name = rule['prog_target']
                device_name = data['device_name']
                device_city = data['device_city']
                duty_organ = rule['duty_organ']
                behavior = data['behavior']
                behavior_dict = {"机场":"8,20,21","高铁":"7,22,23","火车站":"7,22,23","政府":"25"}

		if data_person_type != rule_person_type:
			return 0
		last_device_id = self.getLastPlace(idcard,rule)
                for json_key in control_times:
                    if json_key == 'value':
                        for json_value in control_times[json_key]:
                            j_time = self.getTime(json_value)
                            start_h = j_time[0]
                            end_h = j_time[1]
			    dev_list= str(rule['target_json']).replace('[','').replace(']','').split(',')
                            device_id = data['dev_id']
                            flag1 = False
                            flag2 = False

                            #判断有没有离开云南
                            if  target_name == '云南省':
                                if '->' in device_name:
                                    #print '离开云南'
                                    #print device_name
                                    devices = device_name.split('->')
                                    ksdd = devices[0]
                                    jsdd = devices[1]
                                    flag1 = yn_place_list.yn_flag(ksdd)
                                    flag2 = yn_place_list.yn_flag(jsdd)
				    flag3 = yn_place_list.yn_flag(device_city)
                                    if (not flag1) or (not flag2) or (not flag3):
					print device_name
					if str(rule['control_prog_id']) == '10':
					    rs.append((self.ruleRes(rule),self.dataRes(rule,data)))
					    return rs
			    #------进入阵地判断------
                            flag  = False
                            flag_bj = False
                            #进入机场
                            if target_name == u'昆明长水机场' and behavior in ['8','20','21']:
                                if '->长水机场' in device_name and str(rule['control_prog_id']) == '11':
                                    rs.append((self.ruleRes(rule),self.dataRes(rule,data)))
                                    return rs
                            #进入昆明站 昆明南站 
                            if target_name in [u'昆明站','昆明南站'] and behavior in ['7','22','23']:
                                if '->昆明' in device_name and str(rule['control_prog_id']) == '11':
                                    rs.append((self.ruleRes(rule),self.dataRes(rule,data)))
                                    return rs
                            #进入政府
                            if target_name == u'云南省人民政府' and behavior in ['25']:
                                if '政府' in device_name and str(rule['control_prog_id']) == '11':
                                    rs.append((self.ruleRes(rule),self.dataRes(rule,data)))
                                    return rs

                            #到达北京
                            if target_name == u'北京市':
                                if ("->北京") in device_name:
                                    flag_bj = True
				    if str(rule['control_prog_id']) == '11':
				        rs.append((self.ruleRes(rule),self.dataRes(rule,data)))
					return rs
			    #进入昆明
			    if target_name == '昆明市':
				if '->' in device_name:
				    if (('昆明' in duty_organ) and ((('->%s' % target_name) in device_name))):
				        if str(rule['control_prog_id']) == '11':
			    	            rs.append((self.ruleRes(rule),self.dataRes(rule,data)))
		                    return rs
				else:
				    if (('昆明' in duty_organ) and  (target_name in device_city)):
					if str(rule['control_prog_id']) == '11':
					    rs.append((self.ruleRes(rule),self.dataRes(rule,data)))

                            #数据时间要在规则设定时间内,如过数据中人员类型等于方案目标的人员类型,则预警,10代表离开阵地,11代表进入阵地
                            if col_h >= start_h and col_h<=end_h and str(rule['control_prog_id']) == '10':
				if (device_id not in dev_list) and (last_device_id in dev_list):
                                    rs.append((self.ruleRes(rule),self.dataRes(rule,data)))
				    print 'positionAlarm_leave'
                            if col_h >= start_h and col_h<=end_h and str(rule['control_prog_id']) == '11':
				if (device_id in dev_list) and (last_device_id not in dev_list):
                                    rs.append((self.ruleRes(rule),self.dataRes(rule,data)))
                                    print 'positionAlarm_into'
		return rs

	#轨迹消失
	def noTrackAlarm(self,data,rule,device_df):
                idcard = rule['idcard']
		rs = []
		prog_rule = rule['prog_rule']
		prog_rule_json = json.loads(str(prog_rule))
                control_times = prog_rule_json['date_time']
                for json_key in control_times:
                    if json_key == 'value':
                        for json_value in control_times[json_key]:
                            j_time = self.getTime(json_value)
                            start_h = j_time[0]
                            end_h = j_time[1]
                            devids_l = (str(rule['target_json']).replace('[','').replace(']','')).split(',')
			    devids_df = pd.DataFrame(devids_l,columns=['id'])
			    devids_df = devids_df.merge(device_df,on=['id'],how='inner',suffixes=['','_r'])
			    devids = ','.join(devids_df['code'].values.tolist())
                            prog_rule_json = json.loads(str(rule['prog_rule']))
                            no_track_day = prog_rule_json['no_track_day']
			    print devids
                            #数据时间要在规则设定时间内
                            #如果轨迹中不存在此人的数据,则预警
			    if no_track_day == 'None' or no_track_day == None:
				no_track_day = 2
			    es_data = self.esUtil.getEsData(idcard,devids,no_track_day,start_h,end_h)
                            if len(es_data) == 0:
				data = self.redisUtil.getRedisValue('control_last_place',idcard)
				try:
                                    rs.append((self.ruleRes(rule),self.dataRes(rule,data)))
				except:
				    return rs
                                print 'noTrackAlarm'
                            else:
                                pass
                    else:
                        pass
                return rs
	
	#聚集预警	
	def gatherAlarm(self,data,rule):
		res = self.judgeExe(data,rule)
		rs = []
                if res == 0:
                        return 0
                control_times = res[0]
                col_h = res[1]
		col_time = str(data['col_time'])
		idcard = data['idcard']
		device_id = data['dev_id']
		keep_time = '0'
		first_time = col_time
		last_device_id = device_id

                for json_key in control_times:
                    if json_key == 'value':
                        for json_value in control_times[json_key]:
                            j_time = self.getTime(json_value)
                            start_h = j_time[0]
                            end_h = j_time[1]
			    devs = str(rule['target_json']).replace('[','').replace(']','')
			    if devs == 'None' or devs == None:
				return 0
			    dev_list= devs.split(',')
			    target_id = rule['target_id']
			    
			    #redis存储了区域的id和进入区域的所有人的idcard,first_time(首次进入区域时间),上次经过的设备id,停留时长
			    last_rs = self.redisUtil.getRedisValue('target_id_%s' % target_id,idcard)
			    if last_rs != 0:
			        last_device_id = last_rs['dev_id']
			    #如果此人离开区域就把此人从进入区域人员列表中删除
			    if (device_id not in dev_list ) and (last_device_id in dev_list):
				self.redisUtil.delRedisHashKey('target_id_%s' % target_id,idcard)
                            #数据时间要在规则设定时间内,如果在目标的停留时间超过设定值则预警
                            if (col_h >= start_h and col_h<=end_h and (device_id in dev_list)):
				#0代表在redis中没有找到此人的信息
				if last_rs == 0:
				    keep_time = '0'
				else:
				    first_time = last_rs['first_time']
				data['first_time'] = first_time
				data = self.dataRes(rule,data)
				d_data = json.dumps(data)
                                self.redisUtil.setRedisData('target_id_%s' % target_id,idcard,d_data)
				print 'gatherAlarm'
                            else:
                                pass


	#把每条规则的参数返回成一个dict
	def ruleRes(self,rule_df):
		control_id = rule_df['control_id']
		plan_id = rule_df['prog_id']
                target_id = int(rule_df['target_id'])
                prog_rule = rule_df['prog_rule']
                prog_rule_json = json.loads(prog_rule)
                times = prog_rule_json['times']
		if times == None or times == 'None':
		    times = 1
		return {"control_id":control_id,"plan_id":plan_id,"target_id":target_id,"idcard":rule_df['idcard'],"prog_target":rule_df['prog_target'],"set_num":times,"control_prog":rule_df['control_prog'],"leader_state":rule_df['leader_state'],"emergency_grade":rule_df['emergency_grade'],"create_user":rule_df['create_user'],"update_user":rule_df['update_user'],"is_delete":rule_df['is_delete']}
	
	#把每条数据加上布控的id信息，方便预警信息插入
	def dataRes(self,rule_df,data):
		print data['device_id']
                control_id = rule_df['control_id']
                plan_id = rule_df['prog_id']
                target_id = rule_df['target_id']
		data['control_id'] = str(control_id)
		data['plan_id'] = str(plan_id)
		data['target_id'] = str(target_id)
		#dict类型
		if type(data) == dict:
		    return data
		return data.to_dict()

	def typeChange(self,data):
	    data['control_id'] = data['control_id'].astype("int")
	    data['plan_id'] = data['plan_id'].astype("int")
	    data['target_id'] = data['target_id'].astype("int")
	    return data

	#更新触警频次表
	def updateFreq(self,rule_df,data_df):
        	print 'updateFreq'
		mysql_conf = self.conf['mysql']
		touch_df_t = rule_df
		touch_df_t = touch_df_t.drop_duplicates()
		if touch_df_t.shape[0]>0:
	            MysqlUtils.saveTouchFreq(mysql_conf,touch_df_t)
		touch_df = MysqlUtils.get_data(mysql_conf,mysql_conf['touch_freq_tb'])
		touch_df = touch_df[touch_df['touch_num']%touch_df['set_num']==0]
		#touch_df = touch_df[touch_df['touch_num']%1==0]
		touch_df = self.typeChange(touch_df)
		rule_df = self.typeChange(rule_df)
		if touch_df.shape[0]>0 and rule_df.shape[0]>0: 
		    rules = touch_df.merge(rule_df,on=['control_id','plan_id','target_id','idcard'],how='inner')
		    if rules.shape[0]>0:
			data_df = self.typeChange(data_df)
		        res_df = rules.merge(data_df,on=['control_id','plan_id','target_id','idcard'],how='inner')
			if res_df.shape[0]>0:
		            self.saveAlarmInfo(res_df)
			

	#告警信息写入
	def saveAlarmInfo(self,data): 
		print 'saveAlarmInfo'
		cur_time = str(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())))
		res_l = []
		#数据整理为mysql表所需要的结构
		for index,row in data.iterrows():
		    if row['emergency_grade'] is None or row['emergency_grade']=='None':
		        row['emergency_grade'] = '1'
		    t = {"emergency_date":row['col_time'],"device_id":row['device_id'],"object_business_code":row['idcard'],"plan_target_id":row['target_id'],"person_idcard_num":row['idcard'],"leader_state":row['leader_state'],"emergency_source":'1',"emergency_reason":'%s 身份证号:%s触发%s告警;告警目标:%s 告警位置:%s'%(row['name'],row['idcard'],row['control_prog'],row['prog_target'],row['device_name']),"policeman_state":'2',"lon":row['jd'],"lat":row['wd'],"plan_id":row['plan_id'],"control_id":row['control_id'],"emergency_location":row['device_name'],"emergency_grade":row['emergency_grade'],"city":row['device_city'],"city_code":row['city_code'],"district":row['device_district'],"district_code":row['district_code'],"create_user":row['create_user'],"create_time":cur_time,"update_user":row['update_user'],"update_time":cur_time,"is_delete":row['is_delete'],'trail_source':row['behavior']}
		    res_l.append(t)
		df = pd.DataFrame(res_l)
		#print df['emergency_reason'].head(10)
		#告警信息写入
		df = df.drop_duplicates(subset=['control_id','plan_id','plan_target_id','person_idcard_num','emergency_date'])
		mysql_conf = self.conf['mysql']
		MysqlUtils.saveAlarmInfo(mysql_conf,df)
			
