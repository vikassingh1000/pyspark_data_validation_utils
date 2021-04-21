pyspark_data_validation_utils
#####
 |PyPI-version| |Build-Status| |LICENCE| |codecov|


Data cleansing is needed for all ETL/Data lake solutions.
Spark is preferred technology for custom solutions.

In most of the case we usally perform following validation on data

.. contents::

.. section-numbering::

Not Null 
============
Checking if Column value is not null or not. Please refer sample example. 


Empty
=======
Checking if Column value is not empty or not. Please refer sample example. 

Date format
===========
To check if source has passed correct date format. It can validate all format supported by SimpleDateDormat in Java

Validate allowed domain values
===============================
This can be used to check if record have one of value from validate dataset.
Ex: In some case for account_type column ['Saving', 'Current'] could be only validate values.

Validate forbid domain values
===============================
This can be used to check if record does not have one of value from invalidate dataset.

 
.. |Build-Status| image:: https://travis-ci.com/vikassingh1000/pyspark_data_validation_utils.svg?branch=master
    :target: https://travis-ci.com/vikassingh1000/pyspark_data_validation_utils
.. |LICENCE| image:: https://img.shields.io/badge/License-MIT-yellow.svg
  :target: https://pypi.python.org/pypi/strct
.. |codecov| image:: https://codecov.io/gh/vikassingh1000/pyspark_data_validation_utils/branch/master/graph/badge.svg
  :target: https://codecov.io/gh/vikassingh1000/pyspark_data_validation_utils
.. |PyPI-version| image::  https://badge.fury.io/py/pyspark-data-validation-utils.svg
  :target: https://badge.fury.io/py/pyspark-data-validation-utils

 
