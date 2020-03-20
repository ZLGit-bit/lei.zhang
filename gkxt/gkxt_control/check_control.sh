#!/bin/sh
source ~/.bashrc

. /etc/profile

. ~/.bash_profile
base_path=/opt/bfd/gkxt/gkxt_control
num=`ps -ef|grep gkxt_control.py|wc -l`
echo $num

if [ $num -lt 3 ]; then
echo 'error'
echo $now >> $base_path/error.log
sh $base_path/start.sh
else
echo 'job normal'
fi
