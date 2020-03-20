
nohup /usr/hdp/2.6.2.14-5/spark2/bin/spark-submit  --master local[2]  --driver-memory 2g --executor-memory 2g --conf spark.streaming.kafka.maxRatePerPartition=3000   /opt/bfd/gkxt/yn_clgj.py > /opt/bfd/gkxt/yn_clgj.log &
