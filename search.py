import requests
import json
import config
import util
import threading
import time
from multiprocessing import Process,Queue,Pool

channels_cols = 'id,title,publishedAt,thumbnails,viewCount,subscriberCount,videoCount'
API_KEY = config.API_KEY

def search(keyword):
    url = 'https://www.googleapis.com/youtube/v3/search'
    params={
        'key':API_KEY,
        'part':'id, snippet',
        'filter' : 'relatedToVideoId',
        'maxResults' : 10,
        'q':keyword
    }
    response = requests.get(url,params)
    return json.loads(response.text)

#Input : element of response['items']
def checkIsIn(item):
    global cursor
    channelId = item['snippet']['channelId']
    cursor.execute('select id from channels where id="{}"'.format(channelId))
    li = cursor.fetchall()
    if li == ():
        url = 'https://www.googleapis.com/youtube/v3/channels'
        params = {
            'key':API_KEY,
            'part':'snippet,statistics',
            'id' : channelId
        }
        resp = requests.get(url,params)
        resp = json.loads(resp.text)['items'][0]
        contents = {
            'id' : channelId,
            'title' : resp['snippet']['title'],
            'publishedAt':resp['snippet']['publishedAt'][:10],
            'thumbnails':resp['snippet']['thumbnails']['default']['url'],
            'viewCount':resp['statistics']['viewCount'],
            'subscriberCount':resp['statistics']['subscriberCount'],
            'videoCount':resp['statistics']['videoCount']
        }
        cursor.execute('''
        INSERT INTO channels
        ({})
        VALUES ("{}","{}","{}","{}",{},{},{})
        ON DUPLICATE KEY UPDATE
        viewCount={}, subscriberCount={}, videoCount={}
        '''.format(channels_cols,
            *list(map(lambda x:contents[x],channels_cols.split(','))),
            *list(map(lambda x:contents[x],channels_cols.split(',')))[-3:]
            )
        )

def run(keyword):
    global cursor
    conn, cursor = util.connect2RDS()
    response_json = search(keyword)

    with Pool(processes=4) as pool:
        pool.map(checkIsIn,response_json['items'])
    conn.commit()
    conn.close()

if __name__=='__main__':
    run('사미라')
