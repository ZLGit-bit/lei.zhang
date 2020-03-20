#!/bin/sh
source ~/.bashrc

. /etc/profile

. ~/.bash_profile
base_path=$(cd `dirname $0`;pwd)
echo $base_path
log_path=$base_path/yn_clgj.log

file_mod=`stat -c %Y $log_path`

file_dt=`date -d "@${file_mod}" +"%Y-%m-%d %H:%M:%S"`

now=$(date -d "today" +"%Y-%m-%d %H:%M:%S")

time1=$(($(date +%s -d "$now") - $(date +%s -d "$file_dt")));

times=$[$time1/60]

if [ $times -gt 1 ]; then
echo 'error'
sh $base_path/start_clgj.sh
else
echo 'job normal'
fi
