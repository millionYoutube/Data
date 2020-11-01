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

def get_not_in_ids():
    global cursor
    cursor.execute('''
    SELECT distinct(channel_id)
    FROM popular_videos
    WHERE channel_id not in
        (
            SELECT distinct(id)
            FROM channels
        )
    ''')
    return list(map(lambda x:x[0],cursor.fetchall()))

def update_channel(id):
    global cursor
    url = 'https://www.googleapis.com/youtube/v3/channels'
    params={
        'key':API_KEY,
        'part':'id, snippet,statistics',
        'id' : id
    }
    response = requests.get(url,params)
    response = json.loads(response.text)
    data = make_data(response)
    cursor.execute('''
        INSERT INTO channels (id,name,published_at,thumbnails,
        view_count,subscriber_count,video_count)
        VALUES(%s,%s,%s,%s,%s,%s,%s)
    ''',data)

def make_data(resp):
    resp = resp['items'][0]
    data = (
        resp['id'],
        resp['snippet']['title'],
        resp['snippet'].get('publishedAt','')[:10],
        resp['snippet']['thumbnails']['default']['url'],
        resp['statistics'].get('viewCount',0),
        resp['statistics'].get('subscriberCount',0),
        resp['statistics'].get('videoCount',0)
    )
    return data

if __name__ == '__main__':
    global cursor
    conn, cursor = util.connect2RDS()
    ids = get_not_in_ids()

    with Pool(processes=4) as pool:
        pool.map(update_channel,ids)
    conn.commit()
    conn.close()
