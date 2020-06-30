Utilities
=========

Handy utilities you can use in your projects.

Truncate Date
-------------
Truncate the date to start of the week or month.

Function ::

	truncate_date(date: datetime.date, timescale: str) -> datetime.datetime

	timescale: one of day/week/month or daily/weekly/monthly

Usage ::

    >>> import datetime
    >>> from toolkit import truncate_date
    >>> truncate_date(datetime.date.today(), 'monthly')
    Timestamp('2020-06-01 00:00:00')
    >>> truncate_date(datetime.date.today(), 'weekly')
    Timestamp('2020-06-22 00:00:00')
    >>> truncate_date(datetime.date.today(), 'daily')
    Timestamp('2020-06-26 00:00:00')

Relative Date
-------------
Get the date relative to the truncated date of the given date.

Function ::

	relative_date(date_string: str, reference_date: datetime.date) -> datetime.date

	datestr: format is (date|week|month)(+|-)(n)


Sample code ::

	>>> from toolkit import relative_date
	>>> relative_date('month+0', '2020-01-10') # Returns truncated month
	datetime.date(2020, 1, 1)
	>>> relative_date('month+1', '2020-01-10') # Returns truncated month + 1 month
	datetime.date(2020, 2, 1)
	>>> relative_date('week+0', '2020-01-10') # Returns truncated week
	datetime.date(2020, 1, 6)
	>>> relative_date('week+1', '2020-01-10') # Returns truncated week + 1 week
	datetime.date(2020, 1, 13)
	>>> relative_date('date+0', '2020-01-10')
	datetime.date(2020, 1, 10)
	>>> relative_date('date+1', '2020-01-10')
	datetime.date(2020, 1, 11)


Memoize
-------
Memoizes a function, caching its return values for each input.

Function ::

	memoize(f: callable) -> callable

Sample code::

	>>> from toolkit import memoize
	>>> import random
	>>> @memoize
	... def test_cache():
	...     return random.randint(1, 100)
	...
	>>> test_cache()
	50
	>>> test_cache()
	50

JSON API
--------
Utilities to work with JSON APIs. Works with Bearer authentication too.
You can get/post/delete resources using this utility.


Sample code ::

	>>> from toolkit import API
	>>> api =  API(base_url='https://jsonplaceholder.typicode.com')
	>>> api.get('/todos/1')
	{'userId': 1, 'id': 1, 'title': 'delectus aut autem', 'completed': False}


timeit
------
Times execution of a block of code.

Sample code ::

	>>> from toolkit import timeit
	>>> with timeit('importing pandas'):
	...     import pandas
	...
	importing pandas: 0.962


lazy import
-----------
Imports a module lazily. The module is loaded when an attribute it is accessed from that module for the first time.

This only works with absolute imports and does not work with relative imports.

Sample code ::

	>>> from toolkit import lazy_import
	>>> pd = lazy_import("pandas")
	>>> pd
	<LazyModule: pandas>

Setup logger
------------
Setup logger to print the logs to stdout.

By default, the logging level is set to INFO. If the verbose is True,
then the logging level is set to DEBUG.

The format specifies how the log message is formatted. There are two
supported formats.

short:
    [HH:MM:SS] message
long:
    YYYY-MM-DD HH:MM:SS logger-name [INFO] message

By default the short format is used.

Sample code ::

	>>> from toolkit import logging
	>>> logger =  logging.get_logger("Test")
	>>> logging.setup_logger(format='long')
	>>> logger.info("Log info")
	2020-28-06 15:22:06 Test [INFO] Log info

signals
-------
The signal system allows parts of an application to get notified
when events occur elsewhere in the application, without a tight coupling.

Sample code ::

	>>> from toolkit import Signal
	>>> signal_loggin = Signal("LoggedIn") # Creates a new signal
	>>> @signal_loggin.connect
	... def notify_user(username): # Connect the function to signal named signal_loggin
	...     print("Notify user about new logging through mail")
	...
	>>> signal_loggin.send("test_user") # Send the signal from any where in the system
	Notify user about new logging through mail

db
--
DB utilities to work with web.py databases.

This module provides utilities to inspect the database schema to find the
available tables, coulmns and constraints.

This is tested only on postgres db.


Sample Code ::

	>>> from toolkit.db import Schema
	>>> schema = Schema(db)
	>>> tables = schema.get_tables() # Get all the tables
	>>> enum_types = schema.get_enum_types() # Get all the enum types
	>>> table = schema.get_table('t1')  # Get t1 table
	>>> table.get_indexes()

fileformats
-----------
validate and transform the given data as per the fileformat specifications.

Given input data is passed through a pipeline of validators. These validators
can check structure and schema, for example are their blank rows or columns, is the data of right type etc.

In return, the client receives a report on processing performed.


Sample code ::

	>>> from toolkit import FileFormat
	>>> formatter = FileFormat.from_file("fileformats/customer-master.yml")
	>>> formatter.process_file("master-files/customer-master/customer-master-20200601.csv")
	<Report:status=ACCEPTED #errors=0>

You can use command line tool as well for the validation (this comes along with the package).

Command line tool ::

	$ validate-fileformat --help
	Usage: validate-fileformat [OPTIONS] FILE
	....
	$ validate-fileformat -f fileformats/customer-master.yml customer-master/customer-master-20200601.csv
	File validation status: ACCEPTED
	....
	$ validate-fileformat -f fileformats/customer-master.yml customer-master/customer-master-20200601.csv -o report.json
	File validation status: ACCEPTED
