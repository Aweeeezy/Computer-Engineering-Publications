import os
import sys
import time
import math
import pickle
import sqlite3
from sqlite3 import Error

"""
    Takes in a function and list of arguments, times the execution of that
    function on those arguments, prints the elapsed time, and returns the
    result of the evaluated function.
"""
def timer(fn, args):
  print("\nExecuting {0}...".format(fn.__name__))
  start = time.time()
  result = fn(args)
  print("Elapsed time: {0}".format(time.time() - start))
  return result

"""
  Converts the 'pubs.txt' file into a list of 'record' dictionaries.
"""
def parsePublications(path):

  """
    Returns a list of partially processed records (lists of attribute tags).
  """
  def readFile():
    with open(path, 'r') as f:
      data = f.read()
    for string in ['\n', '<pub>', '<authors>', '</authors>']:
      data = data.replace(string, '')
    data = data.split('</pub>')

    return list(filter(None,
      [list(filter(None, l)) for l in [r.split('\t') for r in data]]))

  """
    Strips attribute tags off of data, enters them into record dictionaries,
    and returns a list of dictionaries.
  """
  def processData():
    records = []
    trash = []
    for d in data:
      record = {'authors':[]}
      dirty = {'authors':[]}
      for i in d:
        attr = ''.join(i[1:-1].split('>')[1:]).split('</')
        if attr[1] == 'author':
          record['authors'].append(attr[0])
          dirty['authors'].append(attr[0])
        else:
          record[attr[1].lower()] = attr[0]
          dirty[attr[1].lower()] = attr[0]
      if 'title' in record.keys():
        records.append(record)
      else:
        trash.append(dirty)

    return records + cleanUp(trash)

  """
    Fixes the broken title attributes and returns a list of dictionaries.
  """
  def cleanUp(trash):
    allowed_keys = ['author', 'authors', 'pages', 'id', 'year', 'booktitle']
    for t in trash:
      dirty_key = [k for k in t.keys() if k not in allowed_keys][0]
      key = dirty_key[1:] if dirty_key[0] == 'i' else dirty_key
      value = t[dirty_key]
      t.pop(dirty_key)

      for string in ['<i', 'sup', '<sub', '<']:
        value, key = value.replace(string, ''), key.replace(string, '')

      t['title'] = value + key

    return trash

  data = readFile()
  return processData()


"""
  Creates a SQLite3 connection from a file path to a database. Returns the
  connection.
"""
def connectToDB(db_file):
  try:
    conn = sqlite3.connect(db_file)
  except Error as e:
    print(e)

  return conn

"""
  Evaluates the 'create_table_sql' string to create tables.
"""
def createTables(conn):
  current_year = time.strftime("%Y")
  create_table_sql = """PRAGMA foreign_keys = ON;

                        CREATE TABLE IF NOT EXISTS publication(
                          id INT PRIMARY KEY,
                          title VARCHAR(400) NOT NULL,
                          year INT CHECK(year >= 1835 and (year <= {0})),
                          booktitle VARCHAR(150),
                          pages VARCHAR(50)
                        );

                        CREATE TABLE IF NOT EXISTS author(
                          id INT PRIMARY KEY,
                          name VARCHAR(150) NOT NULL
                        );

                        CREATE TABLE IF NOT EXISTS written_by(
                          pub_id INT,
                          author_id INT,
                          FOREIGN KEY (pub_id) REFERENCES publication
                          ON DELETE CASCADE,
                          FOREIGN KEY (author_id) REFERENCES author
                          ON DELETE CASCADE,
                          PRIMARY KEY (pub_id, author_id)
                        );
                     """.format(current_year)

  try:
    c = conn.cursor()
    c.executescript(create_table_sql)
    c.close()
  except Error as e:
    print(e)

"""
  Inserts records into the 'publication', 'author', and 'written_by' relations.
"""
def insertRows(conn):
  """
    Inserts a publication ID and author ID into the 'written_by' relation.
  """
  def insertWrittenBy(pub, author):
    written_by_sql = """INSERT INTO written_by VALUES({0}, {1});""".format(
        pub['id'],
        authors[author]
      )

    try:
      c.execute(written_by_sql)
    except Error as e:
      pass

  """
    Inserts an author ID and author name into the 'author' relation.
  """
  def insertAuthors(pub):
    for author in pub['authors']:
      if author not in authors:
        authors[author] = authors['author_id']
        authors['author_id'] += 1
        author_sql = """INSERT INTO author VALUES({0}, "{1}");""".format(
          authors[author],
          author.replace('\"', '\'')
          )

        try:
          c.execute(author_sql)
        except Error as e:
          print(e, author_sql)

      insertWrittenBy(pub, author)

  """
    Inserts a publication ID, title, year, booktitle (journal), and page range
    into the 'publication' relation.
  """
  def insertPublication(pub):
    if "Engineering Advanced Web Applications: Proceedings of Workshops in connection with the 4th International Conference on Web Engineering (ICWE 2004)" in pub['title']:
      string = pub['title'].split(',')[0]
      title, year = string.split('(ICWE ')[0], string.split('(ICWE ')[1].replace(')', '')
      pub['title'], pub['year'] = title, year

    pub_sql = """INSERT INTO publication VALUES({0}, "{1}", {2}, "{3}", "{4}");""".format(
      pub['id'],
      pub['title'].replace('\"', '\''),
      pub['year'],
      pub['booktitle'].replace('\"', '\''),
      pub['pages']
      )

    try:
      c.execute(pub_sql)
    except Error as e:
      print(e, "|||",  pub)

    insertAuthors(pub)

  try:
    c = conn.cursor()
    authors = {'author_id': 0}
    for index, pub in enumerate(records):
      insertPublication(pub)
      if index in [math.floor((f * .1) * len(records)) for f in range(0, 10)]:
        print("{0}%".format(int(math.ceil( float(index) / float(len(records)) * 100 )))) ,
      if index == len(records)-1: print('100%')
    conn.commit()
    c.close()
  except Error as e:
    print(e)


if __name__ == '__main__':
  start = time.time()
  if os.path.isfile('database.db'):
    print("Already created database.\n \
        Delete 'database.db' to recreate the database.\n \
        Delete 'pubs.dat' to parse 'pubs.txt' from scratch.")
  else:
    if os.path.isfile('pubs.dat'):
      print("Already parsed records...these will be used to write to the database.")
      records = timer(pickle.load, open('pubs.dat', 'rb'))
    else:
      print("Parsing records...these will be used to write to the database.")
      records = timer(parsePublications, sys.argv[1])
      pickle.dump(records, open('pubs.dat', 'wb'))

    print("\nCreating database tables and inserting records...")
    db = timer(connectToDB, 'database.db')
    timer(createTables, db)
    timer(insertRows, db)
    num_pubs = db.cursor().execute("""SELECT count(*) FROM publication""").fetchone()[0]
    num_authors = db.cursor().execute("""SELECT count(*) FROM author""").fetchone()[0]
    num_written_by = db.cursor().execute("""SELECT count(*) FROM written_by""").fetchone()[0]
    print("\nTotal elapsed time: {0}".format(time.time()-start))
    print("\nNumber of records:\n\tpublication: {0}\n\tauthor:      {1}\n\twritten_by:  {2}".format(num_pubs, num_authors, num_written_by))
