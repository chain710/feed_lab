from flask import Flask, request, jsonify, current_app
from flask import json, make_response, send_file
import os
import logging
from datetime import datetime
import MySQLdb as mysql
from jinja2 import Template

app_log_path = '%s%s%s.log'%(os.path.dirname(os.path.abspath(__file__)), os.sep, os.path.splitext(os.path.basename(__file__))[0])
#filename=app_log_path,
logging.basicConfig(format='%(asctime)s|%(message)s', level=logging.DEBUG, datefmt='%Y-%m-%d %H:%M:%S')

app = Flask(__name__)
with app.app_context():
    db = mysql.connect(host="192.168.0.147", user="root", db="feed_scraper", use_unicode=True)
    db.set_character_set('utf8')
    db.autocommit(True)
    current_app.db_conn = db
app.debug = True

def load_template(tpl_path):
    content = None
    with open(tpl_path, 'rb') as f:
        content = f.read()
    return content.encode('utf8')

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
    logging.info("load feeds data(%d,%d) finished!"%(num, offset))
    return feeds

@app.before_request
def before_request():
    current_app.db_conn.ping()
    current_app.logger.debug("before_request")
    
@app.teardown_request
def teardown_request(exception):
    current_app.logger.debug("teardown_request")
    
@app.route('/reader/ui/<int:num>/<int:offset>', methods=['GET'])
def reader(num, offset):
    feeds = load_feeds(app.db_conn, num, offset)
    tpl_data = load_template("template.html")
    app.logger.debug("tpl data type %s", type(tpl_data))
    tpl_engine = Template(load_template("template.html"))
    tpl_data = {
        "page_title": "auto paper",
        "entries":[{"title": f['title'], "content": f['content'], "date": f['pub_date'].strftime("%Y-%m-%d %H:%M:%S")} for f in feeds],
    }
    
    return tpl_engine.render(tpl_data)
    
if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000, debug=True)