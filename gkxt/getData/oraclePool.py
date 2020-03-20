# -*- coding:utf-8 -*-
import cx_Oracle
from DBUtils.PooledDB import PooledDB

class oracle(object):
    """数据连接对象，产生数据库连接池.
    此类中的连接采用连接池实现获取连接对象：conn = oracle.getConn()
    释放连接对象;conn.close()或del conn
    """
    # 连接池对象
    __pool = None

    def __init__(self,conf):
        # 数据库构造函数，从连接池中取出连接，并生成操作游标
        self._conn = oracle.get_conn(conf)
        self._cursor = self._conn.cursor()

    @staticmethod
    def get_conn(conf):
        """ 静态方法，从连接池中取出连接
        return oracle.connection
        """
        if oracle.__pool is None:
            user = conf['user']
            pwd = conf['pwd']
            ip = conf['ip']
            db = conf['db']
            dsn = ip + "/" + db
            __pool = PooledDB(creator=cx_Oracle, mincached=1, maxcached=20, user=user, password=pwd,
                              dsn=dsn)
        return __pool.connection()

    def execute(self, sql):
        """执行指定sql语句"""
        self._cursor.execute(sql)  # 执行语句
        self._conn.commit()  # 然后提交

    def close(self):
        """释放连接池资源"""
        self._cursor.close()
        self._conn.close()
