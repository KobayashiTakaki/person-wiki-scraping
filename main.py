import requests
import json
import time
import sqlite3
from contextlib import closing
import urllib
import re
import datetime

dbname = 'database.sqlite3'

def main():
  url = 'http://ja.wikipedia.org/w/api.php'
  params = {
    'action': 'query',
    'format': 'json',
    'generator': 'categorymembers',
    'gcmnamespace': '0',
    'gcmlimit': 200,
    'prop': 'pageviews|extracts',
    'pvipdays': 30,
    'exintro': 'true',
    'explaintext': 'true'
  }

  years = [
    1991
  ]

  create_tables()

  # continue paramが残っていれば使う
  with closing(sqlite3.connect(dbname)) as conn:
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute('select * from continue_params order by created_at desc limit 1;')
    record = cursor.fetchone()
    if record is not None:
      continue_param = json.loads(str(record['json_string']))
      print(str(continue_param))
      params.update(continue_param)

  for year in years:
    complete = False
    continue_param_keys = []
    count = 0
    params['gcmtitle'] = 'Category:' + str(year) + '年生'

    while not complete:
      count += 1
      # # 回数制限
      # if count > 1:
      #   complete = True
      print(count)
      print(params)

      res = requests.get(url, params=params)
      data = json.loads(res.content)
      if 'continue' in data:
        print(data['continue'])
        for continue_param_key in continue_param_keys:
          # 前回のcontinue paramsを消す
          if continue_param_key in params:
            del params[continue_param_key]

        continue_param_keys = []
        for continue_param_key in data['continue'].keys():
          continue_param_keys.append(continue_param_key)
          params[continue_param_key] = data['continue'][continue_param_key]
          print(str(continue_param_key) + ': ' + str(data['continue'][continue_param_key]))

      else:
        for continue_param_key in continue_param_keys:
          # 前回のcontinue paramsを消す
          if continue_param_key in params:
            del params[continue_param_key]

        continue_param_keys = []
        complete = True

      pages = [value for value in data['query']['pages'].values()]

      with closing(sqlite3.connect(dbname)) as conn:
        conn.row_factory = sqlite3.Row

        # continue paramsをDBに保存しておく
        if 'continue' in data:
          sql = 'insert into continue_params (json_string, created_at) values (?, ?);'
          continue_param_dict = {}
          for continue_param_key in continue_param_keys:
            continue_param_dict[continue_param_key] = data['continue'][continue_param_key]
          continue_param_json = json.dumps(continue_param_dict)
          value = (continue_param_json, datetime.datetime.now())
          conn.execute(sql, value)
          conn.commit()

        for page in pages:
          pageid = page['pageid']
          title = page['title']
          pageviews = None
          birthday = None
          school_year = None
          if 'pageviews' in page:
            pageviews = sum_pageviews(page['pageviews'])
          if 'extract' in page:
            school_year = detect_school_year(page['extract'])
            birthday = detect_birthday(page['extract'])
          cursor = conn.cursor()
          cursor.execute('select * from articles where pageid = ?;', (pageid,))
          record = cursor.fetchone()
          if record is not None:
            if pageviews is not None:
              print('update: ' + str(pageid) + ' pageviews = ' + str(pageviews))
              sql = 'update articles set pageviews = ? where pageid = ?;'
              value = (pageviews, pageid)
              conn.execute(sql, value)
              conn.commit()

            if school_year is not None:
              print('update: ' + str(pageid) + ' school_year = ' + str(school_year))
              sql = 'update articles set school_year = ? where pageid = ?;'
              value = (school_year, pageid)
              conn.execute(sql, value)
              conn.commit()

            if birthday is not None:
              print('update: ' + str(pageid) + ' birthday = ' + str(birthday))
              sql = 'update articles set birthday = ? where pageid = ?;'
              value = (birthday, pageid)
              conn.execute(sql, value)
              conn.commit()

          else:
            value = (pageid, title, pageviews, birthday, school_year)
            sql = 'insert into articles (pageid, title, pageviews, birthday, school_year) values (?, ?, ?, ?, ?);'
            print('insert: ' + str(value))
            conn.execute(sql, value)
            conn.commit()

      time.sleep(1)

    with closing(sqlite3.connect(dbname)) as conn:
      conn.execute('delete from continue_params;')
      conn.commit()

def sum_pageviews(page_dict):
  values = [value for value in page_dict.values() if value is not None]
  return sum(values)

def create_tables():
  with closing(sqlite3.connect(dbname)) as conn:
    cursor = conn.cursor()
    sql = 'create table if not exists articles (' \
          + 'id integer primary key autoincrement, pageid integer, title text, ' \
          + 'pageviews integer, birthday text, school_year integer' \
          + ');'
    cursor.execute(sql)
    sql = 'create table if not exists continue_params (' \
          + 'id integer primary key autoincrement, json_string string, created_at text' \
          + ');'
    cursor.execute(sql)
    conn.commit()
    conn.close()

def detect_school_year(text):
  birthday_year_text_match = re.search('([0-9]{4}年)[^日]*', text)
  if birthday_year_text_match is None:
    return None
  year = int(birthday_year_text_match.groups()[0].split('年')[0])
  birthday_date_text_match = re.search('([0-9]{1,2}月[0-9]{1,2}日)', text)
  if birthday_date_text_match is None:
    return None
  month = int(birthday_date_text_match.groups()[0].split('月')[0])
  if month < 4:
    return year - 1
  else:
    return year
  
def detect_birthday(text):
  print(text)
  birthday_year_text_match = re.search('([0-9]{4}年)[^日]*', text)
  if birthday_year_text_match is None:
    return None
  year = int(birthday_year_text_match.groups()[0].split('年')[0])
  birthday_date_text_match = re.search('([0-9]{1,2}月[0-9]{1,2}日)', text)
  if birthday_date_text_match is None:
    return None
  month = int(birthday_date_text_match.groups()[0].split('月')[0])
  day = int(birthday_date_text_match.groups()[0].split('月')[1].split('日')[0])
  print('year: ' + str(year) + ' month: ' + str(month) + ' day: ' + str(day))
  try:
    return datetime.date(year, month, day)
  except ValueError:
    return None

if __name__ == '__main__':
  main()
