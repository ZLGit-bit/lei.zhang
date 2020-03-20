from elasticsearch import Elasticsearch
import time
import datetime
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

class EsUtils:
	def __init__(self,es_conf):
	    self.es_conf = es_conf
	    self.index = es_conf['index']
	    self.index_type = es_conf['type']
	    self.es = Elasticsearch(str(es_conf['nodes'].split(',')[0])+':'+str(es_conf['port']))

	def getEsData(self,idcard,devids,no_track_day,start_h,end_h):
	    es = self.es
	    devid_l = []
	    for devid in devids.split(','):
	        devid_l.append('"%s"' % devid)
	    devids = ','.join(devid_l)
	    body = '{"query":{"bool":{"must":[{"term":{"idcard":"%s"}},{"terms":{"device_id":[%s]}}]}}}' % (idcard,devids)
	    #print body
	    times = int(no_track_day)
	    index_l = []
	    date_l = []
	    for i_day in range(times):
	        min_time = str((datetime.datetime.now()+datetime.timedelta(days=-i_day)).strftime('%Y%m'))
		min_date = str((datetime.datetime.now()+datetime.timedelta(days=-i_day)).strftime('%Y-%m-%d'))
		date_l.append(min_date)
	        #print min_time
	        index = 'person_track_%s' % min_time
	        if (index not in index_l) and (self.existsIndex(index)):
	            index_l.append(index)
	    indexs = ','.join(index_l)
	    #print indexs
	    hits = es.search(index=indexs,doc_type='person_track',body=body)['hits']['hits']
	    hit_l = []
	    for e_data in hits:
	        for e_time in date_l:
	            start_time = '%s %s:00' % (e_time,start_h)
		    end_time = '%s %s:00' % (e_time,end_h)
		    col_time = e_data['_source']['col_time']
		    if col_time>=start_time and col_time<=end_time:
			hit_l.append(col_time)	
	    return hit_l

	def existsIndex(self,index):
	    es = self.es
	    return es.indices.exists(index=self.index)

if __name__ == '__main__':
	#print getEsData('452626198709095130','452626198709095130','3')
	print existsIndex('person_track_201905')
