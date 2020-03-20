import os 
import time
import datetime 

def delEsData():
    this_time = str((datetime.datetime.now()+datetime.timedelta(days=-365)).strftime('%Y-%m-%d'))
    comm = '''
curl -XPOST 'http://10.166.114.151:9200/person_track_201*,person_track_zt_his/_delete_by_query' -d '
{
  "query": {
    "bool": {
      "must": [
        {
          "range": {
            "col_time": {
              "lte": "%s 00:00:00"
            }
          }
        },
        {
          "term": {
            "data_source": "1"
          }
        }
      ]
    }
  }
}'
''' % (this_time)
    print comm
    os.system(comm)


if __name__ == '__main__':
    delEsData()
