import redis
import yaml
import sys
import os
reload(sys)
sys.setdefaultencoding('utf-8')

class RedisUtils:
	def __init__(self,redis_conf):
	    self.redis_conf = redis_conf
	    self.pool = redis.ConnectionPool(host=redis_conf['host'],password=redis_conf['pass'],port=redis_conf['port'])

	def getRedisConn(self):
	    r = redis.Redis(connection_pool=self.pool)
	    return r

	def getRedisData(self,key):
	    r_dict = {}
	    jr = self.getRedisConn()
	    rs = jr.hgetall(key)
	    for key in rs:
		r_dict[int(key)] = long(rs[key])
	    return r_dict

	def setRedisData(self,key,part,value):
            jr = self.getRedisConn()
	    jr.hset(key,part,value)


if __name__ == '__main__':
	import yaml
	f = open('/opt/bfd/gkxt/conf/gkxt.yaml' )
	yaml_conf = yaml.load(f)
	redis_conf = yaml_conf['redis']
	redisUtil = RedisUtils(redis_conf)
	jr = redisUtil.getRedisConn()
 	#setRedisData('realtime_track_source_kafkaOffset','1','1')
	#print jr.set('jqlk_last_time','2018-05-10 00:00:00')
	#print jr.hgetall('target_id_116')
	#print jr.hgetall('clgj_kafkaOffset')
	print jr.delete('clgj_kafkaOffset')
