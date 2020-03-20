last_mon_dt=$(date -d "today-30day" +"%Y-%m-%d 00:00:00")
echo $last_mon_dt
x="{ \"query\": {   \"range\": {\"ordertime\": {      \"lte\": \"${last_mon_dt}\"}  } }}"

curl -XPOST "10.166.114.151:9200/wu_qwjs_kxclgj_stb_new/_delete_by_query" -d "${x}"
