#!/usr/bin/env python
# -*- coding: utf-8


import datetime
import inspect
import pandas as pd
import yaml
from sqlalchemy import create_engine
import sqlalchemy

def get_data(tb):
    try:
	url = 'mysql+mysqldb://root:bbi!zmkj0520@86.1.41.98:3306/test?charset=utf8'
        df = pd.read_sql(tb,create_engine(url))
        return df
    except Exception, e:
        print "error:   ", inspect.stack()[0][1], inspect.stack()[0][3], ":", e, "\n\n\n"
        exit(-1)

def set_data(conf):
    from sqlalchemy import create_engine
    return create_engine(conf['mysql_url_to'])

