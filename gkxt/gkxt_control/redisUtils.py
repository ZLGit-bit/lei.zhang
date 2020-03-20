# -*- coding:utf-8 -*-
import redis
import json
import yaml
import sys
import datetime
reload(sys)
sys.setdefaultencoding('utf-8')

class RedisUtils:
	def __init__(self,redis_conf):
	    self.redis_conf = redis_conf
	    self.pool = redis.ConnectionPool(host=redis_conf['host'],password=redis_conf['pass'],port=redis_conf['port'])

	def getRedisConn(self):
	    r = redis.Redis(connection_pool=self.pool)
	    return r

	#获取kafka offset
	def getRedisData(self,key):
	    r_dict = {}
	    jr = self.getRedisConn()
	    rs = jr.hgetall(key)
	    for key in rs:
		r_dict[int(key)] = int(rs[key])
	    return r_dict

	def setRedis(self,key,value):
	    jr = self.getRedisConn()
	    jr.set(key,value)

        def getRedis(self,key):
            jr = self.getRedisConn()
            rs = jr.get(key)
	    if rs is None:
		return 0
	    return rs

	def getGatherInfo(self,key,stay_time):
            r_l = []
            jr = self.getRedisConn()
            rs = jr.hgetall(key)
            for k in rs:
		k_value = self.getRedisValue(key,k)
		if k_value != 0:
		    first_time = k_value['first_time']
                    #停留时长
                    seds =  (datetime.datetime.now()-datetime.datetime.strptime(first_time,'%Y-%m-%d %H:%M:%S')).seconds
                    m,s =divmod(seds,60) 
		    keep_stay_time = int(m)
		    #返回停留时间大于stay_time的数据
		    if keep_stay_time >= int(stay_time):
			#print k_value
		        r_l.append(k_value)
            return r_l

	def setRedisData(self,key,part,value):
            jr = self.getRedisConn()
	    jr.hset(key,part,value)

	def existsKey(self,key,idcard):
	    jr = self.getRedisConn()
	    return jr.hexists(key,idcard)

	def getRedisValue(self,key,idcard):
	    r_dict = {}
	    jr = self.getRedisConn()
	    rs = jr.hget(key,idcard)
	    if rs is None:
	        return 0
	    else:
	        d_v = json.loads(rs)
	        return d_v


	def delRedisHashKey(self,key,part):
            r_dict = {}
            jr = self.getRedisConn()
	    jr.hdel(key,part)


if __name__ == '__main__':
	jr = getRedisConn()
        jr.delete('control_info_update_time')
 	#setRedisData('hahaha','1','1,1')
	#setRedisData('hahaha','1','1,2')
	#print getRedisValue('hahaha','1')
	#print jr.hget('control_last_place','452601195906010916')
	#delRedisHashKey('hahaha','1')
	#print getGatherInfo('target_id_51','60')
	#for x in jr.hgetall('target_id_51'):
	#    print x
	#existsKey('realtime_track_source_kafkaOffset','121')
	#print getGatherInfo('target_id_51',60)
	#print rs
