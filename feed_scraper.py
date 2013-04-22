import logging
import feedparser
from datetime import datetime
import hashlib
import MySQLdb as mysql
import socket

#TODO: 
#download all images
#remove ad
#optimize feed database

def hash_feed_item(feed_url, item_id):
    item_id = "%s%s"%(feed_url, item_id)
    return hashlib.sha1(item_id).hexdigest()

def download_entries(scraper_info, data, db):
    cursor = db.cursor()
    items = []
    for entry in data.entries:
        item = {
            "id": "",
            "title": "",
            "content": "",
            "date": datetime(*entry.published_parsed[0:6]),
        }
        if entry.has_key("id"):
            item['id'] = entry.id
        elif entry.has_key("link"):
            item['id'] = entry.link
            
        if entry.has_key("content"):
            item['content'] = entry.content[0].value;
        elif entry.has_key("summary"):
            item['content'] = entry.summary;
        
        if entry.has_key("title"):
            item['title'] = entry.title
        items.append(item)
    # sort by date
    items.sort(key=lambda item: item['date'], reverse=True)
    # insert till end or meet last scrap id
    first_id = None
    insert_num = 0
    for item in items:
        hash_id = hash_feed_item(data.feed.link, item['id'])
        if first_id is None:
            first_id = hash_id
        
        if hash_id == scraper_info['last_scrap_id']:
            break
            
        logging.debug("saving entry(%s) id=%s content", hash_id, item['id'])
        # save feed content
        cursor.execute("insert item_contents set id=%s,content=%s,title=%s", [hash_id, item['content'], item['title']])
        cursor.execute("insert items set id=%s, feed_id=%s, pub_date=%s", [hash_id, scraper_info['id'], item['date']])
        insert_num += 1
    # update last scrap info
    if insert_num > 0:
        cursor.execute("update scraper_info set last_scrap_id=%s where id=%s", [first_id, scraper_info['id']])
        if cursor.rowcount != 1:
            logging.warn("cant update scraper_info[%d], this may cause dup items, affected=%d", scraper_info['id'], cursor.rowcount)
        
    return insert_num

def download_feeds(db):
    cursor = db.cursor()
    cursor.execute("select * from scraper_info")
    row_num = cursor.rowcount
    for i in range(row_num):
        row = cursor.fetchone()
        sinfo = {
            "id": row[0],
            "url": row[1],
            "last_scrap_id": row[2],
        }
        
        logging.debug("begin to download [%d/%d] %s ..."%(i+1, row_num, sinfo['url']))
        try:
            data = feedparser.parse(sinfo['url'])
        except Exception, e:
            logging.error("parse feed %s error %s"%(feed, e))
            continue
        logging.debug("%s downloaded!"%(sinfo['url']))
        #TODO: detect data.bozo
        download_entries(sinfo, data, db);

if __name__ == "__main__":
    #prevent feedparser timeout
    socket.setdefaulttimeout(3.0)
    db = mysql.connect(host="192.168.0.147", user="root", db="feed_scraper", use_unicode=True)
    #avoid encode error
    db.set_character_set('utf8')
    db.autocommit(True)
    logging.basicConfig(format='%(asctime)s|%(message)s', level=logging.DEBUG, datefmt='%Y-%m-%d %H:%M:%S')
    download_feeds(db)