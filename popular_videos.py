import config
import requests
import json
import util
import time
import datetime
import sys
import warnings
import logging
import concurrent.futures
from threading import Thread
from multiprocessing import Process,Queue,Pool

warnings.filterwarnings('ignore')

API_KEY = config.API_KEY
ranking_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def run_each_categories(cat_id):
    global q,cursor,conn
    ranking=1
    url = 'https://www.googleapis.com/youtube/v3/videos'
    params={
        'key':API_KEY,
        'part':'id, snippet,statistics',
        'chart' : 'mostPopular',
        'maxResults' : 50,
        'regionCode':'KR',
        'videoCategoryId':cat_id
    }
    response = requests.get(url,params)
    response = json.loads(response.text)
    data = []
    for ranking,resp in enumerate(response['items']):
        data.append(
            (
            resp['id'],
            resp['snippet']['publishedAt'][:10],
            resp['snippet']['title'].replace('\"','\''),
            resp['snippet']['channelId'],
            resp['snippet']['channelTitle'],
            cat_id,
            ranking,
            ranking_time
            )
        )
        ranking+=1
    sql = '''
        INSERT INTO popular_videos
        (id, published_at, title, channel_id, channel_title,
        category_id,ranking,ranking_time)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
        '''

    cursor.executemany(sql,data)

def getCategories():
    global cursor, category_ids
    sql = 'select id from categories'
    cursor.execute(sql)
    category_ids = list(map(lambda x: x[0],cursor.fetchall()))
    return category_ids

def run():
    stime = time.time()
    global conn,cursor, category_ids
    try:
        conn, cursor = util.connect2RDS()
        category_ids = getCategories()
        with Pool(processes=8) as pool:
            pool.map(run_each_categories,category_ids)
        conn.commit()
    except:
        logging.error("FAILED TO UPDATE POPULAR_VIDEOS")
        conn.rollback()
    finally:
        conn.close()
    print(time.time()-stime)

if __name__=='__main__':
    run()
