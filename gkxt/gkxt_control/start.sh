base_path=/opt/bfd/gkxt/gkxt_control
nohup /usr/hdp/2.6.2.14-5/spark2/bin/spark-submit --master local[2]  --driver-memory 5g --executor-memory 2g --conf spark.streaming.kafka.maxRatePerPartition=25 --jars $base_path/elasticsearch-spark-20_2.11-5.2.1.jar $base_path/gkxt_control.py > $base_path/control.log  &
