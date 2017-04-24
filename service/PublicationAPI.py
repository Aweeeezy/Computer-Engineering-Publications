import sqlite3
from sqlite3 import Error

class PublicationAPI:
  __conn = None
  __next_pub_id = None
  __next_author_id = 0
  __authors = {}

  """
    Connects to the database named in the parameter, fetches the maximum ID in
    order to set __next_pub_id, and turns on foreign key contraints.
  """
  def __init__(self, database):
    try:
      self.__conn = sqlite3.connect(database)
      cursor = self.__conn.cursor()
      self.__next_pub_id = cursor.execute("""SELECT max(id) FROM publication;
                                          """).fetchone()[0] + 1
      self.__authors = {k[1]:k[0] for k in cursor.execute("""SELECT id, name FROM author;
                                      """).fetchall()}
      self.__next_author_id = max(self.__authors.values()) + 1
      cursor.execute("""PRAGMA foreign_keys = ON""").close()
    except Error as e:
      print(e)

  """
    Replaces \" characters with \' for every attribute in the input list
    record. Creates an INSERT sql string for the publication relation and
    executes it. List record must be of the form:
    [title, year, journal, pages, authors] where 'authors' is a list of authors.
  """
  def insertPublication(self, record):
    pub = [str(r).replace('\"', '\'') for r in record[:-1]]
    authors = record[-1]
    sql = """INSERT INTO publication VALUES({0}, "{1}", {2}, "{3}", "{4}");""".format(self.__next_pub_id, *pub)

    for a in authors:
      if a not in self.__authors:
        self.__authors[a] = self.__next_author_id
        sql += """INSERT INTO author VALUES({0}, '{1}');""".format(self.__next_author_id, a)
        self.__next_author_id += 1
        sql += """INSERT INTO written_by VALUES({0}, {1});""".format(self.__next_pub_id, self.__next_author_id-1)
      else:
        sql += """INSERT INTO written_by VALUES({0}, {1});""".format(self.__next_pub_id, self.__authors[a])

    try:
      self.__conn.cursor().executescript(sql).close()
      self.__conn.commit()
      self.__next_pub_id += 1
    except Error as e:
      print(e)

  """
    Deletes authors. If exact=True, only deletes exact name matches. If
    exact=False, deletes all authors where the author's name has any of the
    paramter name's substrings in it.
  """
  def deleteAuthor(self, author, exact=True):
    if exact:
      sql = """DELETE FROM author WHERE lower(name)= '{0}'""".format(author.lower())
    else:
      if len(author.split()) == 1:
        sql = """DELETE FROM author WHERE lower(name) LIKE '%{0}%'""".format(author.lower())
      elif len(author.split()) == 2:
        sql = """DELETE FROM author WHERE lower(name) LIKE '%{0}%{1}%'""".format(*[a.lower() for a in author.split()])
      else:
        author = author.split()
        author[1] = author[1][0] # Removes the '.' from an initial
        sql = """DELETE FROM author WHERE name LIKE '%{0}%{1}%{2}%'""".format(*[a.lower() for a in author])

    try:
      self.__conn.cursor().execute(sql).close()
      self.__conn.commit()
    except Error as e:
      print(e)

  """
    Deletes all publications matching the title, year, and journal name in the
    parameter list. List record must be of the form: [title, year, journal].
  """
  def deletePublication(self, record):
    sql = """DELETE FROM publication WHERE id in (
             SELECT p.id FROM publication as p, written_by as w, author as a
             WHERE p.id = w.pub_id AND w.author_id = a.id AND lower(p.title) = '{0}'
             AND p.year = {1} AND lower(p.booktitle) = '{2}')""".format(
               *[r.lower() if type(r) is not int else r for r in record])

    try:
      self.__conn.cursor().execute(sql).close()
      self.__conn.commit()
    except Error as e:
      print(e)

  """
    Updates the names to new_name for authors with a name matching old_name.
  """
  def updateAuthor(self, old_name, new_name):
    sql = """UPDATE author SET name = '{0}' WHERE lower(name) = '{1}';""".format(new_name, old_name.lower())

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
    set1 = '' if not new_title else "title = '{0}'".format(new_title)
    set2 = '' if not new_year else "year = {0}".format(new_year)
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
    and journal. If exact equals False, then return similar matches for the
    author, title, and journal. Record is a list of the form:
    [author, title, year, journal].
  """
  def queryPublication(self, record, exact=True, output_format='JSON',
      sorted_order='title', reverse=False, queryRange="0,50"):
    author, title, year, journal = record[0], record[1], record[2], record[3]
    start, end = queryRange.split(',')[0], queryRange.split(',')[1]
    if exact:
      cond1 = '' if not author else "lower(a.name) = '{0}'".format(author.lower())
      cond2 = '' if not title else "lower(p.title) = '{0}'".format(title.lower())
      cond3 = '' if not year else "p.year = {0}".format(year)
      cond4 = '' if not journal else "lower(p.booktitle) = '{0}'".format(journal.lower())
    else:
      cond1 = '' if not author else "lower(a.name) LIKE '%{0}%'".format(author.lower())
      cond2 = '' if not title else "lower(p.title) LIKE '%{0}%'".format(title.lower())
      cond3 = '' if not year else "p.year = {0}".format(year)
      cond4 = '' if not journal else "lower(p.booktitle) LIKE '%{0}%'".format(journal.lower())

    cond1 = cond1 + " AND " if cond1 and (cond2 or cond3 or cond4) else cond1
    cond2 = cond2 + " AND " if cond2 and (cond3 or cond4 ) else cond2
    cond3 = cond3 + " AND " if cond3 and cond4 else cond3
    cond5 = "AND " if cond1 or cond2 or cond3 or cond4 else ""
    cond6 = "ORDER BY " + sorted_order + " DESC" if reverse else "ORDER BY " + sorted_order

    sql_pubs = """SELECT DISTINCT p.id, p.title, p.year, p.booktitle
                  FROM publication as p, written_by as w, author as a
                  WHERE p.id = w.pub_id and w.author_id = a.id
                  {4} {0}{1}{2}{3} {5} LIMIT {6},{7};
              """.format(cond1, cond2, cond3, cond4, cond5, cond6, start, end)

    sql_authors = """SELECT p.id, a.name
                     FROM publication as p, written_by as w, author as a
                     WHERE p.id = w.pub_id and w.author_id = a.id
                     {4} {0}{1}{2}{3} {5};
                  """.format(cond1, cond2, cond3, cond4, cond5, cond6)

    try:
      cursor = self.__conn.cursor()
      result_pubs = cursor.execute(sql_pubs).fetchall()
      result_authors = cursor.execute(sql_authors).fetchall()
      cursor.close()

      def formatOutput(output_format):
        def convertToJSON():
          if sorted_order != 'name':
            return [{
              'title': r[1],
              'authors': list(filter(None, [a[1] if a[0] == r[0] else None for a in result_authors])),
              'year': r[2],
              'journal': r[3],
              } for r in result_pubs]
          else:
            return [{
              'title': list(filter(None, [p[1] if p[0] == r[0] else None
                for p in result_pubs]))[0],
              'authors': r[1],
              'year': list(filter(None, [p[2] if p[0] == r[0] else None
                for p in result_pubs]))[0],
              'journal':
                  list(filter(None, [p[3]
                    if p[0] == r[0] else None
                    for p in result_pubs]))[0]
                  if len(list(filter(None, [p[3]
                      if p[0] == r[0] else None
                      for p in result_pubs])))
                  else ""
              } for r in result_authors]
            return result

        def convertToXML():
          if sorted_order != 'name':
            results = []
            for r in result_pubs:
              res = '<pub>\n\t<title>' + r[1] + '</title>\n\t<authors>'
              for a in result_authors:
                res += '\n\t\t<author>{0}</author>'.format(a[1]) \
                  if a[0] == r[0] else ''
              res += '\n\t</authors>\n\t<year>' + str(r[2]) + \
                '</year>\n\t<booktitle>' + r[3] + '</booktitle>'
              results.append(res)
            return results
          else:
            results = []
            for r in result_pubs:
              results.append('<pub>\n\t<title>' + r[1] +
                '</title>\n\t<authors>' + '\n\t\t<author>' + r[1] + \
                '</author>\n\t</authors>\n\t<year>' + str(r[2]) + \
                '</year>\n\t<booktitle>' + r[3] + '</booktitle>')
            return results

            return [{
              'title': list(filter(None, [p[1] if p[0] == r[0] else None
                for p in result_pubs]))[0],
              'authors': r[1],
              'year': list(filter(None, [p[2] if p[0] == r[0] else None
                for p in result_pubs]))[0],
              'journal':
                  list(filter(None, [p[3]
                    if p[0] == r[0] else None
                    for p in result_pubs]))[0]
                  if len(list(filter(None, [p[3]
                      if p[0] == r[0] else None
                      for p in result_pubs])))
                  else ""
              } for r in result_authors]
            return result


        return {
            'JSON': convertToJSON(),
            'XML': convertToXML()
            }[output_format]

      return formatOutput(output_format), len(result_pubs)
    except Error as e:
      print(e)

if __name__ == '__main__':
  api = PublicationAPI('../database.db')

  print("\n*********** Testing queryPublication and insertPublication ***********")

  first_attempt = api.queryPublication(["", "Big Data and Recommender Systems", 2016, ""])

  if not first_attempt:
    print("\nCalling queryPublication(['', 'Big Data and Recommender Systems', 2016, ''])...\nReturned nothing...\n\nCalling insertPublication(['Big Data and Recommender Systems', 2017, '', '', ['David C. Anastasiu', 'Evangelia Christakopoulou', 'Shaden Smith', 'Mohit Sharma']])...\n")

    api.insertPublication(["Big Data and Recommender Systems", 2016, "", "",
    ["David C. Anastasiu", "Evangelia Christakopoulou", "Shaden Smith", "Mohit Sharma"]])

    print("Calling queryPublication(['', 'Big Data and Recommender Systems', 2016, ''])...")
    print(api.queryPublication(['', 'Big Data and Recommender Systems', 2016, '']))
  else:
    print("Calling queryPublication(['', 'Big Data and Recommender Systems', 2016, ''])..")
    print(first_attempt)
    print("\nCalling insertPublication(['Neural Dust: An Ultrasonic, Low Power Solution for Chronic Brain-Machine Interfaces', 2013, 'arXiv', '', ['Dongjin Seo', 'Jose M Carmena', 'Jan M Rabaey', 'Elad Alon', 'Michel M Maharbiz']])...")

    api.insertPublication(["Neural Dust: An Ultrasonic, Low Power Solution for "
     + "Chronic Brain-Machine Interfaces", 2013, "arXiv", "",  ["Dongjin Seo",
       "Jose M Carmena", "Jan M Rabaey", "Elad Alon", "Michel M Maharbiz"]])

    print("\nCalling queryPublication(['', 'Neural Dust: An Ultrasonic, Low Power Solution for Chronic Brain-Machine Interfaces', 2013, 'arXiv'])...")
    print(api.queryPublication(['', 'Neural Dust: An Ultrasonic, Low Power Solution for Chronic Brain-Machine Interfaces', 2013, 'arXiv']))


  print("\n\n*********** Testing queryPublication with sorted_order=name ***********")
  print("\nCalling queryPublication(['', 'Big Data and Recommender Systems', 2016, ''], sorted_order='name')...")
  print(api.queryPublication(['', 'Big Data and Recommender Systems', 2016, ''], sorted_order='name'))


  print("\n\n*********** Testing updateAuthor and updatePublication ************")

  print("\nCalling updateAuthor('David C. Anastasiu', 'David Anastasiu')...")
  api.updateAuthor("David C. Anastasiu", "David Anastasiu")

  print("\nCalling queryPublication(['David Anastasiu', '', None, ''])...")
  print(api.queryPublication(["David Anastasiu", "", None, ""]))

  print("\nCalling updatePublication(['Big Data and Recommender Systems', 2016, ''], ['Really BIG Data and Recommender Systems', 2016, 'Some Journal'])...")
  api.updatePublication(["Big Data and Recommender Systems", 2016, ""], ["Really BIG Data and Recommender Systems", 2016, "Some Journal"])

  print("\nCalling queryPublication(['', 'Really BIG Data and Recommender Systems', 2016, 'Some Journal'])...")
  print(api.queryPublication(['', 'Really BIG Data and Recommender Systems', 2016, 'Some Journal']))

  print("\n\n****** Testing deletePublication and deleteAuthor and foreign key constraints *******")

  print("\nCalling queryPublication(['', 'Really BIG Data and Recommender Systems', 2016, ''])...")
  print(api.queryPublication(["", "Really BIG Data and Recommender Systems", 2016, ""]))

  print("\nCalling deletePublication(['Really BIG Data and Recommender Systems', 2016, ''])...")
  api.deletePublication(["Really BIG Data and Recommender Systems", 2016, "Some Journal"])

  print("\nCalling queryPublication(['', 'Really BIG Data and Recommender Systems', 2016, ''])...")
  print(api.queryPublication(["", "Really BIG Data and Recommender Systems", 2016, ""]))

  api.insertPublication(["Test Publication", 2017, "", "", ["David C. Anastasiu"]])

  print("\nCalling queryPublication(['David Anastasiu', '', None, ''])...")
  print(api.queryPublication(["David Anastasiu", "", None, ""]))

  print("\nCalling deleteAuthor('David Anastasiu')...")
  api.deleteAuthor("David Anastasiu")
  api.deletePublication(["Test Publication", 2017, ""])

  print("\nCalling queryPublication(['David Anastasiu', '', None, ''])...")
  print(api.queryPublication(["David Anastasiu", "", None, ""]))

  print("\nCalling queryPublication(['', 'Test Publication', None, ''])...")
  print(api.queryPublication(["", "Test Publication", None, ""]))

  print("\n\n*********** Testing limited range query ************")

  print("\nCalling queryPublication(['', '', 2000, 'NIPS'], queryRange='0,5')...")
  print(api.queryPublication(['', '', 2000, 'NIPS'], queryRange='0,5'))

  print("\nCalling queryPublication(['', '', 2000, 'NIPS'], queryRange='3,2')...")
  print(api.queryPublication(['', '', 2000, 'NIPS'], queryRange='3,2'))

  print("\n\n*********** Testing XML output format ************")

  print("\nCalling queryPublication(['', '', 2000, 'NIPS'], output_format='XML',queryRange='3,1')...")
  print(api.queryPublication(['', '', 2000, 'NIPS'], output_format='XML', queryRange='3,1'))
