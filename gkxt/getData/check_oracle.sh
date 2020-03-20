#!/bin/sh
source ~/.bashrc

. /etc/profile

. ~/.bash_profile
base_path=$(cd `dirname $0`;pwd)
echo $base_path
log_path=$base_path/logs

for file in $log_path/*;do 
    t_file=${file/.log/}
    p_file=${t_file/logs/}.py
    echo $p_file
    file_mod=`stat -c %Y $file`

    file_dt=`date -d "@${file_mod}" +"%Y-%m-%d %H:%M:%S"`
    now=$(date -d "today" +"%Y-%m-%d %H:%M:%S")

    time1=$(($(date +%s -d "$now") - $(date +%s -d "$file_dt")));

    times=$[$time1/60]
    echo $times
   if [ $times -gt 60 ]; then
      echo 'error'
      nohup /opt/anaconda2/bin/python $p_file > $file &
   else
      echo 'data normal'
   fi
done
