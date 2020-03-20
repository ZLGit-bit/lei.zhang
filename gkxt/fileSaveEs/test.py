import os
comm = "curl -XPUT 'http://10.166.114.151:9200/%s/_settings' -d '{\"settings\":{\"refresh_interval\":\"30s\"}}'" % 'wu_qwjs_xhc_hccl_stb'
print comm
os.system(comm)
