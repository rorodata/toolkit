Welcome to Toolkit!
===================

Toolkit is an assorted utilities that comes handy to use in all our projects.

How to install
--------------

Installation ::

  $ pip install algoshelf-toolkit --extra-index-url https://algoshelf:37iiOxaUQM9kJn93@pypi-dot-algoshelf-staging.appspot.com/

**Note**: extra-index-url above might change in near future. Incase of failure please refer to `notion page <https://www.notion.so/rorodata/Python-Private-Packages-399f5f9c122e4ad8b7d3e81aa4c7d743>`_ to get updated one.

How to use
----------

Here is the sample code ::

  >>> from toolkit import truncate_date
  >>> import datetime
  >>> truncate_date(datetime.date.today(), 'monthly')
  Timestamp('2020-06-01 00:00:00')

Documentation:
--------------
Here you can find detailed docs of all utilities

.. toctree::
   :maxdepth: 2

   utils

API Reference
-------------
If you are looking for information on a specific function, class, or method, this part of the documentation is for you.

.. toctree::
   :maxdepth: 1

   api
   cache
   dateutil
   dev
   db
   imports
   logging
   signals
   fileformat
