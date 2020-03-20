ps -ef | grep "gkxt_control.py" | grep -v grep | awk '{print $2}' | xargs kill -9
