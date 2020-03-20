import redis
import yaml
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

f = open('../conf/gkxt.yaml')
yaml_conf = yaml.load(f)
redis_conf = yaml_conf['redis']
#print redis_conf['host']
pool = redis.ConnectionPool(host=redis_conf['host'],password=redis_conf['pass'],port=redis_conf['port'])

def getRedisConn():
	r = redis.Redis(connection_pool=pool)
	return r

def getRedisData(key):
	r_dict = {}
	jr = getRedisConn()
	rs = jr.hgetall(key)
	for key in rs:
		r_dict[int(key)] = int(rs[key])
	return r_dict

def setRedisData(key,part,value):
        jr = getRedisConn()
	jr.hset(key,part,value)


if __name__ == '__main__':
	jr = getRedisConn()
 	#setRedisData('realtime_track_source_kafkaOffset','1','1')
	print jr.set('jqlk_last_time','2018-05-13 10:38:58')
	#print jr.get('jqlk_last_time')
	#print jr.set('zcxx_last_time','2017-05-06 10:51:22')
	#print jr.set('kydp_last_time','2018-05-06 10:51:22')
