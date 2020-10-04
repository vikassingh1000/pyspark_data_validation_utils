from pyspark.sql import functions as sf
from script.org.validator.data_validator import DefaultExceptionRecordHandler, ValidatorBuilder

import logging

from script.org.validator.date_format_validator import DateFormatValidator
from tests.test_util import compare_df

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)
logging.info('Admin logged in')


def test_date_format_custom_msg_non_agg_mutlie_clms(spark_session):

    date_format ="yyyy-mm-dd"

    msg_part1="Column {} value's '"
    msg_part2=f"'does not match with provided format: {date_format}"
    custom_msg_lmda = lambda p_clm, date_format : sf.concat(sf.lit(msg_part1.format(p_clm)), sf.col(p_clm), sf.lit(msg_part2))
    VALIDATE_AGAINST_MAP = {
        "dob": [DateFormatValidator(p_format=date_format)],
        "doj": [DateFormatValidator(p_format=date_format, custom_msg_lmda=custom_msg_lmda)],
    }

    test_df = spark_session.createDataFrame([[1, "dummy", "2020-10-12", "2012-12-12"], [2,None,"12-10-2012", "dude never joined"]], "field1: int, field2: string, dob:String, doj:string")

    excep_record_handler = DefaultExceptionRecordHandler()
    val_builder = ValidatorBuilder()
    data_val = val_builder.add_excp_rec_handler(excep_record_handler).add_validation_map(VALIDATE_AGAINST_MAP).add_validate_rec_df(test_df).build()
    valid_df,invalid_df = data_val.validate()

    invaild_df_expected = spark_session.createDataFrame([ [2,None,"12-10-2012","dude never joined","dob is not a validate date as per format yyyy-mm-dd|Column doj value's 'dude never joined'does not match with provided format: yyyy-mm-dd|"]], "field1: int, field2: string, dob:String, doj:string, exception_desc:String")


    vaild_df_expected = spark_session.createDataFrame([ [1, 'dummy',"2020-10-12","2012-12-12" ]], "field1: int, field2: string, dob:String, doj:string")

    print(invalid_df.collect())
    assert compare_df(invalid_df,invaild_df_expected)
    assert compare_df(valid_df,vaild_df_expected)

def test_date_format_custom_msg_non_agg(spark_session):

    date_format ="yyyy-mm-dd"
    msg_part1="Column {} value's '"
    msg_part2=f"'does not match with provided format: {date_format}"
    custom_msg_lmda = lambda p_clm, date_format : sf.concat(sf.lit(msg_part1.format(p_clm)), sf.col(p_clm), sf.lit(msg_part2))
    VALIDATE_AGAINST_MAP = {
        "dob": [DateFormatValidator(p_format=date_format, custom_msg_lmda=custom_msg_lmda)],
    }

    test_df = spark_session.createDataFrame([[1, "dummy", "2020-10-12"], [2,None,"invaild"]], "field1: int, field2: string, dob:String")

    excep_record_handler = DefaultExceptionRecordHandler()
    val_builder = ValidatorBuilder()
    data_val = val_builder.add_excp_rec_handler(excep_record_handler).add_validation_map(VALIDATE_AGAINST_MAP).add_validate_rec_df(test_df).build()
    valid_df,invalid_df = data_val.validate()

    invaild_df_expected = spark_session.createDataFrame([ [2,None,"invaild","Column dob value's 'invaild'does not match with provided format: yyyy-mm-dd|"]], "field1: int, field2: string, dob:String, exception_desc:String")


    vaild_df_expected = spark_session.createDataFrame([ [1, 'dummy',"2020-10-12" ]], "field1: int, field2: string, dob:String")

    assert compare_df(invalid_df,invaild_df_expected)
    assert compare_df(valid_df,vaild_df_expected)

def test_date_format_custom_msg(spark_session):

    date_format ="yyyy-mm-dd"
    msg_part1="Column {} value's '"
    msg_part2=f"'does not match with provided format: {date_format}"
    custom_msg_lmda = lambda p_clm, date_format : sf.concat(sf.lit(msg_part1.format(p_clm)), sf.col(p_clm), sf.lit(msg_part2))
    VALIDATE_AGAINST_MAP = {
        "dob": [DateFormatValidator(p_format=date_format, custom_msg_lmda=custom_msg_lmda)],
    }

    test_df = spark_session.createDataFrame([[1, "dummy", "2020-10-12"], [2,None,"invaild"]], "field1: int, field2: string, dob:String")

    excep_record_handler = DefaultExceptionRecordHandler()
    val_builder = ValidatorBuilder()
    data_val = val_builder.add_excp_rec_handler(excep_record_handler).add_validation_map(VALIDATE_AGAINST_MAP).add_validate_rec_df(test_df).build()
    valid_df,invalid_df = data_val.validate()

    invaild_df_expected = spark_session.createDataFrame([ [2,None,"invaild","Column dob value's 'invaild'does not match with provided format: yyyy-mm-dd|"]], "field1: int, field2: string, dob:String, exception_desc:String")


    vaild_df_expected = spark_session.createDataFrame([ [1, 'dummy',"2020-10-12" ]], "field1: int, field2: string, dob:String")

    print(invalid_df.collect())
    assert compare_df(invalid_df,invaild_df_expected)
    assert compare_df(valid_df,vaild_df_expected)

def test_date_format_defaulf_msg(spark_session):

    VALIDATE_AGAINST_MAP = {
        "dob": [DateFormatValidator(p_format="yyyy-mm-dd")],
    }


    test_df = spark_session.createDataFrame([[1, "dummy", "2020-10-12"], [2,None,"invaild"]], "field1: int, field2: string, dob:String")

    excep_record_handler = DefaultExceptionRecordHandler()
    val_builder = ValidatorBuilder()
    data_val = val_builder.add_excp_rec_handler(excep_record_handler).add_validation_map(VALIDATE_AGAINST_MAP).add_validate_rec_df(test_df).build()
    valid_df,invalid_df = data_val.validate()

    invaild_df_expected = spark_session.createDataFrame([ [2,None,"invaild","dob is not a validate date as per format yyyy-mm-dd|"]], "field1: int, field2: string, dob:String, exception_desc:String")


    vaild_df_expected = spark_session.createDataFrame([ [1, 'dummy',"2020-10-12" ]], "field1: int, field2: string, dob:String")

    assert compare_df(invalid_df,invaild_df_expected)
    assert compare_df(valid_df,vaild_df_expected)