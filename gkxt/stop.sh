ps -ef | grep "gkxt_run.py" | grep -v grep | awk '{print $2}' | xargs kill -9
