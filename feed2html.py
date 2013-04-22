import logging
from datetime import datetime
import MySQLdb as mysql
import codecs
from jinja2 import Template

OUTPUT_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width" />
<meta http-equiv="X-UA-Compatible" content="chrome=1" />
<title>{{ page_title }}</title>
<style>
#wrapper {
    margin: 0 auto;
    width: 700px;
}

#bookmark_list {
    width: auto;
}

.tableViewCell {
    background-color: #F8F8F8;
}

.clear {
    clear: both;
    font-size: 1px;
    line-height: 1px;
    margin: 1px;
}
</style>
</head>
<body>
<div id="wrapper">
<div id="bookmark_list">
{% for entry in entries %}
<div class="tableViewCell">
<div class="cornerControls">
</div>
<div class="titleRow">{{ entry.title }}</div>
<div class="summaryRow">{{ entry.content }}</div>
<div class="secondaryControls">
<span class="host">{{ entry.date }}</span>
</div>
<div class="clear">&nbsp;</div>
</div>
{% endfor %}
</div>
</div>
</body>
</html>
"""


def render_html(feeds, template):
    tpl_engine = Template(template)
    tpl_data = {
        "page_title": "auto paper",
        "entries":[{"title": f['title'], "content": f['content'], "date": f['pub_date'].strftime("%Y-%m-%d %H:%M:%S")} for f in feeds],
    }
    
    return tpl_engine.render(tpl_data)

def load_feeds(db, num):
    cursor = db.cursor()
    cursor.execute("select * from items order by pub_date desc limit %s", num)
    feeds = [{'id': row[0], 'feed_id':row[1], 'pub_date': row[2], 'content':'DATA MISSING', 'title':'DATA MISSING'} for row in cursor.fetchall()]
    
    
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
    logging.info("generate feeds data finished!")
    return feeds
if __name__ == "__main__":
    db = mysql.connect(host="192.168.0.147", user="root", db="feed_scraper", use_unicode=True)
    db.set_character_set('utf8')
    db.autocommit(True)
    logging.basicConfig(format='%(asctime)s|%(message)s', level=logging.DEBUG, datefmt='%Y-%m-%d %H:%M:%S')
    feeds = load_feeds(db, 10)
    
    fp = codecs.open("rss.html", "wb", "utf-8")
    fp.write(render_html(feeds, OUTPUT_TEMPLATE))
    fp.close()
    logging.info("generate html page finished!")