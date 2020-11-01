import requests
import json
import config
import logging
import pymysql
import sys

host = config.host
user = config.user
password = config.password
database = config.database
port = config.port

def connect2RDS():
    try:
        conn = pymysql.connect(host, user=user, passwd=password, db=database, port=3306, use_unicode=True, charset='utf8')
        cursor = conn.cursor()
    except:
        logging.error("fail to connect MYSQL")
        sys.exit(1)
    return conn, cursor

if __name__=='__main__':
    connect2RDS()
