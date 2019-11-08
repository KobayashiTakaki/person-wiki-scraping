import os
from os.path import join, dirname
from dotenv import load_dotenv
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
import sqlite3
from contextlib import closing
import datetime

dbname = 'database.sqlite3'

def main():
  create_tables()
  rows = get_articles_from_sqlite()
  if len(rows) == 0:
    return

  dotenv_path = join(dirname(__file__), '/.env')
  load_dotenv(dotenv_path)
  cred = credentials.ApplicationDefault()
  firebase_admin.initialize_app(cred, {
    'projectId': os.environ.get('FIREBASE_PROJECT_ID')
  })
  db = firestore.client()

  with closing(sqlite3.connect(dbname)) as conn:
    for row in rows:
      data = {
        u'title': row['title'],
        u'school_year': row['school_year'],
        u'birthday': row['birthday'],
        u'pageviews': row['pageviews']
      }
      print(data)
      db.collection(u'articles').document(str(row['pageid'])).set(data)
      value = (row['id'], datetime.datetime.now())
      sql = 'insert into firebase_logs (done_article_id, done_at) values (?, ?);'
      conn.execute(sql, value)
      conn.commit()

def get_articles_from_sqlite():
  with closing(sqlite3.connect(dbname)) as conn:
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    sql = 'select id, pageid, title, school_year, birthday, pageviews from ' \
          + '(select *, row_number() over (partition by school_year order by pageviews desc) rownum from articles ' \
          + 'where title is not null and school_year is not null and birthday is not null and pageviews is not null) ' \
          + 'where rownum < 501;'
    cursor.execute(sql)
    return cursor.fetchall()

def create_tables():
  with closing(sqlite3.connect(dbname)) as conn:
    cursor = conn.cursor()
    sql = 'create table if not exists firebase_logs (' \
          + 'id integer primary key autoincrement, done_article_id integer, done_at text' \
          + ');'
    cursor.execute(sql)
    conn.commit()
    conn.close()

if __name__ == '__main__':
  main()
