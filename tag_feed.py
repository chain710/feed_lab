import jieba.analyse
import MySQLdb as mysql
import logging
from bs4 import BeautifulSoup

def load_feeds(db, num, offset):
    cursor = db.cursor()
    cursor.execute("select * from items order by pub_date desc limit %s offset %s", (num, offset))
    feeds = [{'id': row[0], 'feed_id':row[1], 'pub_date': row[2], 'content':None, 'title':None} for row in cursor.fetchall()]
    
    
    fid_str = ",".join(['"%s"'%f['id'] for f in feeds])
    cursor.execute("select * from item_contents where id in (%s)"%(fid_str))
    rows = cursor.fetchall()
    feed_contents = {}
    for row in rows:
        feed_contents.update({row[0]:{"content":row[1], "title":row[2]}})
        
    for f in feeds:
        if feed_contents.has_key(f['id']):
            f['content'] = feed_contents[f['id']]['content']
            f['title'] = feed_contents[f['id']]['title']
            
    feeds.sort(key=lambda f:f['pub_date'], reverse=True)
    logging.info("load feeds data finished!")
    return feeds
if __name__ == "__main__":
    db = mysql.connect(host="192.168.0.147", user="root", db="feed_scraper", use_unicode=True)
    db.set_character_set('utf8')
    db.autocommit(True)
    logging.basicConfig(format='%(asctime)s|%(message)s', level=logging.DEBUG, datefmt='%Y-%m-%d %H:%M:%S')
    feeds = load_feeds(db, 10, 0)
    
    for f in feeds:
        if f['content'] == None:
            continue
            
        soup = BeautifulSoup(f['content'])
        #print soup.get_text()
        tags = jieba.analyse.extract_tags(soup.get_text(), topK=10)
        print 'tags are %s'%(','.join(tags))