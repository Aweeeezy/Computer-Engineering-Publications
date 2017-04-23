import sqlite3
from sqlite3 import Error

class PublicationAPI:
  __conn = None
  __next_id = None

  """
    Connects to the database named in the parameter, fetches the maximum ID in
    order to set __next_id, and turns on foreign key contraints.
  """
  def __init__(self, database):
    try:
      self.__conn = sqlite3.connect(database)
      cursor = self.__conn.cursor()
      self.__next_id = cursor.execute("""SELECT count(*) FROM publication;
                                      """).fetchone()[0] + 1
      cursor.execute("""PRAGMA foreign_keys = ON""").close()
    except Error as e:
      print(e)

  """
    Replaces \" characters with \' for every attribute in the input list
    record. Creates an INSERT sql string for the publication relation and
    executes it. List record must be of the form:
    [title, year, journal, pages].
  """
  def insertPublication(self, record):
    record = [str(r).replace('\"', '\'') for r in record]
    sql = """INSERT INTO publication VALUES({0}, "{1}", {2}, "{3}", "{4}");""".format(self.__next_id, *record)

    try:
      self.__conn.cursor().execute(sql).close()
      self.__conn.commit()
      self.__next_id += 1
    except Error as e:
      print(e)

  """
    Deletes authors. If exact=True, only deletes exact name matches. If
    exact=False, deletes all authors where the author's name has any of the
    paramter name's substrings in it.
  """
  def deleteAuthor(self, author, exact=True):
    if exact:
      sql = """DELETE FROM author WHERE name = '{0}'""".format(author)
    else:
      if len(author.split()) == 1:
        sql = """DELETE FROM author WHERE name LIKE '%{0}%'""".format(author)
      elif len(author.split()) == 2:
        sql = """DELETE FROM author WHERE name LIKE '%{0}%{1}%'""".format(*author.split())
      else:
        author = author.split()
        author[1] = author[1][0] # Removes the '.' from an initial
        sql = """DELETE FROM author WHERE name LIKE '%{0}%{1}%{2}%'""".format(*author)

    try:
      self.__conn.cursor().execute(sql).close()
      self.__conn.commit()
    except Error as e:
      print(e)

  """
    Deletes all publications matching the author, year, and journal name in the
    parameter list. List record must be of the form: [author, year, journal].
  """
  def deletePublication(self, record):
    sql = """DELETE FROM publication WHERE id in (
             SELECT p.id FROM publication as p, written_by as w, author as a
             WHERE p.id = w.pub_id AND w.author_id = a.id AND a.name = '{0}'
             AND p.year = {1} AND p.booktitle = '{2}')""".format(*record)

    try:
      self.__conn.cursor().execute(sql).close()
      self.__conn.commit()
    except Error as e:
      print(e)

  """
    Updates the names to new_name for authors with a name matching old_name.
  """
  def updateAuthor(self, old_name, new_name):
    sql = """UPDATE author SET name = '{0}' WHERE name = '{1}';""".format(new_name, old_name)

    try:
      self.__conn.cursor().execute(sql).close()
      self.__conn.commit()
    except Error as e:
      print(e)

  """
    Updates the journal, title, or year of a publication by matching the title,
    year, and journal. Matching criteria is a parameterized as the list old and
    the update values are parameterized as new. Old and new are of the form:
    [title, year, journal]. Use None or empty strings for values of new that
    are not to be updated.
  """
  def updatePublication(self, old, new):
    new_title, new_year, new_journal = new[0], new[1], new[2]
    old_title, old_year, old_journal = old[0], old[1], old[2]
    set1 = '' if not new_title else "title = '{0}' ".format(new_title)
    set2 = '' if not new_year else "year = {0} ".format(new_year)
    set3 = '' if not new_journal else "booktitle = '{0}'".format(new_journal)
    set1 = set1 + "," if set1 and (set2 or set3) else set1
    set2 = set2 + "," if set2 and set3 else set2
    sql = """UPDATE publication SET {0}{1}{2} WHERE title = "{3}" and year = {4} and booktitle = "{5}";""".format(set1, set2, set3, old_title, old_year, old_journal)

    try:
      self.__conn.cursor().execute(sql).close()
      self.__conn.commit()
    except Error as e:
      print(e)

  """
    Queries publications matching an author, title, year, or journal. If exact
    equals True, then only match return exact matches for the author, title,
    and journal. If exact equals False, then return similar mathces for the
    author, title, and journal. Record is a list of the form:
    [author, title, year, journal].
  """
  def queryPublication(self, record, exact=True):
    author, title, year, journal = record[0], record[1], record[2], record[3]
    if exact:
      cond1 = '' if not author else "a.name = '{0}'".format(author)
      cond2 = '' if not title else "p.title = '{0}'".format(title)
      cond3 = '' if not year else "p.year = {0}".format(year)
      cond4 = '' if not journal else "p.booktitle = '{0}'".format(journal)
    else:
      cond1 = '' if not author else "a.name = '%{0}%'".format(author)
      cond2 = '' if not title else "p.title = '%{0}%'".format(title)
      cond3 = '' if not year else "p.year = {0}".format(year)
      cond4 = '' if not journal else "p.booktitle = '%{0}%'".format(journal)

    cond1 = cond1 + " and " if cond1 and (cond2 or cond3 or cond4) else cond1
    cond2 = cond2 + " and " if cond2 and (cond3 or cond4 ) else cond2
    cond3 = cond3 + " and " if cond3 and cond4 else cond3
    sql = """SELECT p.id, p.title, a.name, p.year, p.booktitle
             FROM publication as p, written_by as w, author as a
             WHERE p.id = w.pub_id and w.author_id = a.id
             AND {0}{1}{2}{3}""".format(cond1, cond2, cond3, cond4)

    try:
      cursor = self.__conn.cursor()
      result = cursor.execute(sql).fetchall()
      cursor.close()
      return result
    except Error as e:
      print(e)

if __name__ == '__main__':
  api = PublicationAPI('../database.db')
