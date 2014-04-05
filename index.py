import elementtree.ElementTree as ET
import pymysql

conn = pymysql.connect(host='127.0.0.1', user='root', passwd='', db='dblp', charset='utf8') #pymysql and utf stuff
cur = conn.cursor()
keys = ['title', 'journal', 'year', 'conf']
author_keys = ['name', 'article_id']
delimiter_keys = ['www', 'incollection', 'collection','inproceedings','proceedings', 'article'] #add the extra stuff

def initialize_row():
  to_insert = {}
  for k in keys:
    to_insert[k] = ''
  return to_insert

def save_data(to_insert):
  # print to_insert
  cur.execute("INSERT INTO articles(title, journal, year, conf) VALUES (%s, %s, %s, %s)", (to_insert['title'], to_insert['journal'], to_insert['year'], to_insert['conf']))
  print cur.lastrowid
  conn.commit()
  to_insert['article_id'] = cur.lastrowid
  save_author_data(to_insert)

def save_author_data(to_insert):
  # print to_insert
  try:
    if 'author' in to_insert:
      cur.execute("INSERT INTO authors(name, article_id) VALUES (%s, %s)", (to_insert['name'], to_insert['article_id']))
      print cur.lastrowid
      conn.commit()

  except pymysql.MySQLError:
    print "error, but continuing"

to_insert = initialize_row()

for event, elem in ET.iterparse('dblp.xml'):
  if elem.tag in delimiter_keys:
    save_data(to_insert)
    to_insert = initialize_row()
  
  if elem.tag in keys:
    #print elem.tag, elem.text
    to_insert[elem.tag] = elem.text

  if elem.tag in author_keys:
    #print elem.tag, elem.text
    to_insert[elem.tag] = elem.text
    