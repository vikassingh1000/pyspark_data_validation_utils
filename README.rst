*********************
Data Cleansing Tool
*********************

 |PyPI-version| |Build-Status| |LICENCE| |codecov|


Data cleansing is needed for all ETL/Data lake solutions.
Spark is preferred technology for custom solutions.



.. contents::


Type of Validators
##################
In most of the case we usually perform following validation on data. This tool can be extended to define new validator easily. 


Not Null 
****
Checking if Column value is not null or not. Please refer sample example. 


Empty
********
Checking if Column value is not empty or not. Please refer sample example. 

Date format
***********
To check if source has passed correct date format. It can validate all format supported by SimpleDateDormat in Java

Validate allowed domain values
********************************
This can be used to check if record have one of value from validate dataset.
Ex: In some case for account_type column ['Saving', 'Current'] could be only validate values.

Validate forbid domain values
********************************
This can be used to check if record does not have one of value from invalidate dataset.

Data type check
****************
This can be used to check if column has correct datatype or not. (Currently it is not supporting complex Struct type)

Regex check
****************
This can be sued to check if String columns has validate value as per regax. Ex: email Id validation or phone number validation etc.

At Least one column present check
****************************************
In some source provide multiple columns for same purpose, this validator can be used to check if atleast one column has value.

How to use it
##################
Builder class can be use to prepare validator( entry point). 

Configuration
#############
It accept dictionary To pass column and validate

Custom message
****************
All validators has commons exception message, this can be changes in following way.


**Ex-1:**
 Create a python lambda function and pass it in constructor
 
 .. code-block:: python

  custom_msg_lmda_field1 = lambda p_clm, validate_against : sf.lit(f"{p_clm} is empty ")
  field1": [NotNullValidator(custom_msg_lmda = custom_msg_lmda_field1)]
 
.. |Build-Status| image:: https://travis-ci.com/vikassingh1000/pyspark_data_validation_utils.svg?branch=master
    :target: https://travis-ci.com/vikassingh1000/pyspark_data_validation_utils
.. |LICENCE| image:: https://img.shields.io/badge/License-MIT-yellow.svg
  :target: https://pypi.python.org/pypi/strct
.. |codecov| image:: https://codecov.io/gh/vikassingh1000/pyspark_data_validation_utils/branch/master/graph/badge.svg
  :target: https://codecov.io/gh/vikassingh1000/pyspark_data_validation_utils
.. |PyPI-version| image::  https://badge.fury.io/py/pyspark-data-validation-utils.svg
  :target: https://badge.fury.io/py/pyspark-data-validation-utils
