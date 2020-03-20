from elasticsearch import Elasticsearch
import yaml
import time
import datetime
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

f = open('./conf/control.yaml')
yaml_conf = yaml.load(f)
es_conf = yaml_conf['es']
ips = es_conf['nodes']
port = es_conf['port']
global index
index = es_conf['index']
index_type = es_conf['type']
es = Elasticsearch(str(ips.split(',')[0])+':'+str(port))


def getEsData():
	body='''
	{
  "query": {
    "match_all": {}
  },"size":300
}
	'''
	#print body
	hits = es.search(index='vehicle_archive',doc_type='vehicle_archive',body=body)['hits']['hits']
	return hits


if __name__ == '__main__':
	import pandas as pd
	l=[]
	df = getEsData()
	print len(df)
	for x in df:
	    source = x['_source']
	    fzjg = source['FZJG'][:1]
	    hphm = source['HPHM']
	    fzjg_n = '%s%s' % (fzjg,hphm[:1])
	    id = x['_id']
	    con = '{"doc":{"FZJG":"%s"}}' % fzjg_n
	    print con
	    es.update(index='vehicle_archive',doc_type='vehicle_archive',id=id,body=con)
	#print pd.DataFrame(l)
	#print df['CSYS'].head(10)
	#print existsIndex('person_track_201905')
