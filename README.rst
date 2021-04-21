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
Checking if Column value is not null or not. Below is the sample code for it

.. code-block:: python
 column_to_validation_dict = {
        "a": [NotNullValidator()],
        "b": [NotNullValidator()],
    }


    test_df = spark_session.createDataFrame([[1, "dummy"], [2, ""]], "a: int, b: string")

    excep_record_handler = DefaultExceptionRecordHandler()
    val_builder = ValidatorBuilder()
    data_val = val_builder.add_excp_rec_handler(excep_record_handler).add_validation_map(column_to_validation_dict).add_validate_rec_df(test_df).build()
    valid_df,invalid_df = data_val.validate()


Use
===

``strct`` is divided into five sub-modules:


 
.. |Build-Status| image:: https://travis-ci.com/vikassingh1000/pyspark_data_validation_utils.svg?branch=master
    :target: https://travis-ci.com/vikassingh1000/pyspark_data_validation_utils
.. |LICENCE| image:: https://img.shields.io/badge/License-MIT-yellow.svg
  :target: https://pypi.python.org/pypi/strct
.. |codecov| image:: https://codecov.io/gh/vikassingh1000/pyspark_data_validation_utils/branch/master/graph/badge.svg
  :target: https://codecov.io/gh/vikassingh1000/pyspark_data_validation_utils
.. |PyPI-version| image::  https://badge.fury.io/py/pyspark-data-validation-utils.svg
  :target: https://badge.fury.io/py/pyspark-data-validation-utils

 
