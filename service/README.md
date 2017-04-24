PublicationAPI
========================

This is a wrapper class that provides basic functionalities for accessing and
manipulating the database layer of a web applilcation that utilized the
Publication database.

Constructing PublicationAPI
-----------------------------

To construct this wrapper class call

`api = PublicationAPI("database.db")`

to initialized the PublicationAPI object.

This will set the instance variable `__conn` to an SQLite connection to
"database.db", set the instance variables `__next_pub_id` and `__next_author_id` to the maximum publication ID and author ID respectively, and create a dictionary of authors as the instance variable `__authors` before turning on foreign keys.

Inserting a New Publication
----------------------------

To insert a new publication call

`api.insertPublication(record)`

where `record` is a list of the form [title, year, journal, pages, authors]
where `authors` is a list of author names.

This will clean the input record of double quotes (") by replacing them with
single quotes ('), insert the new publication into the 'publication' relation,
add unseen authors into the 'author' relation, and add records to 'written_by'
that connect the publication to the authors.

Deleting an Author
-----------------------

To delete an author call

`api.deleteAuthor(name, exact=True)`

This will delete authors matching `name` exactly (ignoring case) from the
`author` relation. Alternatively, setting `exact` to `False` will delete all
authors from the `author` relation who have the substrings (space delimited) of
`name` anywhere in their name.

Deleting a Publication
--------------------------

To delete a publication call

`api.deletePublication(record)`

where `record` is a list of the form [title, year, journal].

All parameters must be provided and are matched exactly (ignoring case).


Updating an Author
--------------------------

To update an author call

`api.updateAuthor(old_name, new_name)`

This will replace `new_name` the names of authors whose name matches exactly
(ignoring case) `old_name`.

Updating a Publication
--------------------------

To update a publication call

`api.updatePublication(old, new)`

where `old` and `new` are both lists of the form [title, year, journal]. All
parameters in `old` must be provided and will be matched exactly (ignoring
case), but only the parameters you wish to update must be provided in `new`.

Querying a Publication
-----------------------

To query a publication call

`api.queryPublication(record, exact=True, output_format='JSON', sorted_order='title', reverse=False, queryRange="0,50")`

where `record` is a list of the form [author, title, year, journal]. None of
the parameters in `record` have to be provided, but all records will be returne
if they are left empty. Setting `exact` to `False` will allow fuzzy matching of
`author`, `title`, and `journal` for values that include these parameters
inside of them. `output_format` is used to specify whether the results are
retuned in JSON or XML format. `sorted_order` can be set to `title`, `year`,
`journal`, or `name` (author name). While normally `queryPublication` returns distinct
records, using `sorted_order=name` returns a multiset so that publications
written by multiple authors can be seen for each of those authors. Setting
`reverse` to `True` orders the results in descending order. `queryRange` is a
comma-delimited string of two integer numbers that are used for the query's
LIMIT clause; the first value is the offsize and the second value is the number
of records to retrieve.

Testing
-------------------------

When not import PublicationAPI as a module and, instead, executing it as a
script, a series of tests are run utilizing all the included functions
effectively demonstrating that all requirements are met.
