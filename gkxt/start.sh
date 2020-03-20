
nohup /usr/hdp/2.6.2.14-5/spark2/bin/spark-submit  --master local[2]  --driver-memory 10g --executor-memory 2g --conf spark.streaming.kafka.maxRatePerPartition=1000   /opt/bfd/gkxt/gkxt_run.py > /opt/bfd/gkxt/run.log &
