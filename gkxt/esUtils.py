from elasticsearch import Elasticsearch
import yaml
import time
import datetime
import sys
import os
reload(sys)
sys.setdefaultencoding('utf-8')
class EsUtils:
        def __init__(self,es_conf):
                self.es_conf = es_conf
		self.es = Elasticsearch(str(es_conf['nodes'].split(',')[0])+':'+str(es_conf['port']))
		self.index = es_conf['index']
		self.index_type = es_conf['type']

	def createIndex(self):
		try:	
                    index = self.index
                    index_type = self.index_type
                    es = self.es
		    dt = str(time.strftime('%Y%m',time.localtime(time.time())))
		    index_dt = "%s_%s" % (index,dt)
		    print index_dt
		    mappings={"mappings":{"%s" % index_type:{"properties":{"id":{"type":"keyword","index":"no"},"name":{"type":"keyword"},"idcard":{"type":"keyword"},"cpzl":{"type":"keyword"},"card_type":{"type":"keyword"},"car_type":{"type":"keyword"},"car_brand":{"type":"keyword"},"car_brand_subclass":{"type":"keyword"},"card_num":{"type":"keyword"},"col_time":{"format":"yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis","type":"date"},"data_source":{"type":"keyword"},"behavior":{"type":"keyword"},"device_id":{"type":"keyword"},"device_name":{"type":"keyword"},"pic1":{"type":"keyword","index":"no"},"pic2":{"type":"keyword","index":"no"},"pic3":{"type":"keyword","index":"no"},"etl_time":{"format":"yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis","type":"date"},"device_location":{"type":"geo_point"},"device_loc_str":{"type":"keyword"},"device_province":{"type":"keyword"},"device_city":{"type":"keyword"},"device_district":{"type":"keyword"},"device_town":{"type":"keyword"},"device_street":{"type":"keyword"}}}},"settings":{"index":{"number_of_shards":"5","number_of_replicas":"0"}}}
		    if not self.existsIndex(index_dt):
		        print 'create index %s' % index_dt
		    	res= es.indices.create(index=index_dt,body=mappings)
		    	print res
		except:
			pass

	def deleteIndex(self):
		try:
		    days = es_conf['days']
		    index = self.index
		    es = self.es
		    times = -int(days)
		    l_dt = str((datetime.datetime.now()+datetime.timedelta(days=times)).strftime('%Y%m'))
		    print l_dt
		    index_dt = index+"_"+l_dt
		    es.indices.delete(index=index_dt)
		except:
		    pass	

	def existsIndex(self,index):
		index = self.index
		es = self.es
        	return es.indices.exists(index=index)

	if __name__ == '__main__':
		createIndex()
		#es.indices.delete(index='person_track')
		#deleteIndex()
