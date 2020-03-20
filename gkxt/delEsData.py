#-*- coding:utf-8 -*-
import yaml
import json
import time
import sys
import pandas as pd
from sqlalchemy import create_engine
import sqlalchemy
import os
from elasticsearch import Elasticsearch
reload(sys)
sys.setdefaultencoding('utf-8')
f = open('/opt/bfd/gkxt/conf/gkxt.yaml')
yaml_conf = yaml.load(f)
mysql_conf = yaml_conf['mysql']
es_conf = yaml_conf['es']
ips = es_conf['nodes']
port = es_conf['port']
es = Elasticsearch(str(ips.split(',')[0])+':'+str(port))

def getData():
    sql = "select vehicle_num from union_relation where vehicle_num!=''"
    d_df = pd.read_sql(sql,create_engine(mysql_conf["list_url"]))
    count=0
    l = []
    for index,row in d_df.iterrows():
        code = row[0]
        code = '"%s"' % code
        l.append(code)
        count=count+1
        if count==100:
            codes = ','.join(l)
            print codes
            init_sfzdr(codes,1)
            count = 0
            l = []
    init_sfzdr(codes,1)


def init_sfzdr(code,type2):
        body = '''
{
  "query": {
    "bool": {
      "must": [
        {
          "terms": {
            "card_num": [%s]
          }
        },
        {
          "term": {
            "behavior": "%s"
          }
        }
      ]
    }
  },
  "script": {
    "inline": "ctx._source.device_location=params.para",
    "params": {
      "para": ""
    }
  }
}
        ''' % (code,type2)


        body2 = '''
{
  "query": {
    "bool": {
      "must": [
        {
          "terms": {
            "card_num": [%s]
          }
        },
        {
          "term": {
            "behavior": "%s"
          }
        }
      ]
    }
  }
}
        ''' % (code,type2)
        print body2
        #es.delete_by_query(index='person_track_201911',doc_type='person_track',body=body2)

        try:
            print 1
            es.delete_by_query(index='person_track_kxcl*',doc_type='person_track',body=body2)
            #es.update_by_query(index='person_track_201911',doc_type='person_track',body=body)
        except:
            pass

if __name__ == '__main__':
    getData()
                                 
