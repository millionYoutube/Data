import requests
import json
import config
import util
import time
import datetime
import sys
import warnings
from threading import Thread
from multiprocessing import Process,Queue

warnings.filterwarnings('ignore')

category_ids = [1,10,15,17,20,24]
API_KEY = config.API_KEY

now = datetime.datetime.now().now()
ranking_time = '{}-{:02}'.format(now.strftime('%y%m%d'),now.hour)


def multiProcessing(cat_id,q):
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
    for ranking,resp in enumerate(response['items']):
        q.put(
            (
                resp['id'],
                resp['snippet']['publishedAt'],
                resp['snippet']['title'].replace('\"','\''),
                resp['snippet']['channelId'],
                cat_id,
                ranking+1
            )
        )

def consume(q):
    global conn
    count = 0
    while True:
        data = q.get()
        th = Thread(target=insert2RDS, args=(data,))
        th.start()
        th.join()
        count +=1
        if count>=300:
            conn.commit()
            conn.close()
            break

def insert2RDS(data):
    global cursor
    sql = '''
        INSERT INTO popular_videos
        (id, published_at, title, channel_id, category_id, ranking, ranking_time)
        VALUES ("{}", "{}", "{}", "{}", {}, {}, "{}");
        '''.format(*data,ranking_time)
    cursor.execute(sql)

def run():
    stime = time.time()
    global conn,cursor
    conn, cursor = util.connect2RDS()

    q = Queue()
    process_list = [Process(target=multiProcessing, args=(id,q)) for id in category_ids]
    consumer = Process(target=consume, args=(q,))
    for process in process_list:
        process.start()
    consumer.start()
    q.close()
    q.join_thread()
    for process in process_list:
        process.join()
    consumer.join()
    print(stime-time.time())

if __name__=='__main__':
    run()
