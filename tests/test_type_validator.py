from script.org.validator.data_validator import DefaultExceptionRecordHandler, ValidatorBuilder

import logging

from script.org.validator.type_validator import TypeValidator
from tests.test_util import compare_df

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)
logging.info('Admin logged in')



def test_type_validator(spark_session):
    VALIDATE_AGAINST_MAP = {
        "id": [TypeValidator(clm_type="int")]
    }

    test_df = spark_session.createDataFrame([[1, "dummy", "11"], [2,None,"not int"]], "field1: int, field2: string, id:string")

    excep_record_handler = DefaultExceptionRecordHandler()
    val_builder = ValidatorBuilder()
    data_val = val_builder.add_excp_rec_handler(excep_record_handler).add_validation_map(VALIDATE_AGAINST_MAP).add_validate_rec_df(test_df).build()
    valid_df,invalid_df = data_val.validate()


    invaild_df_expected = spark_session.createDataFrame([ [2,None,"not int","id is not of given type int |"]], "field1: int, field2: string, id:string, exception_desc:String")


    vaild_df_expected = spark_session.createDataFrame( [[1, "dummy", "11" ]], "field1: int, field2: string, id:string")

    print(invalid_df.collect())
    assert compare_df(invalid_df,invaild_df_expected)
    assert compare_df(valid_df,vaild_df_expected)



def test_type_validator_float(spark_session):
    VALIDATE_AGAINST_MAP = {
        "id": [TypeValidator(clm_type="float")]
    }

    test_df = spark_session.createDataFrame([[1, "dummy", "12345678901211"], [2,None,"not int"]], "field1: int, field2: string, id:string")

    excep_record_handler = DefaultExceptionRecordHandler()
    val_builder = ValidatorBuilder()
    data_val = val_builder.add_excp_rec_handler(excep_record_handler).add_validation_map(VALIDATE_AGAINST_MAP).add_validate_rec_df(test_df).build()
    valid_df,invalid_df = data_val.validate()


    invaild_df_expected = spark_session.createDataFrame([ [2,None,"not int","id is not of given type float |"]], "field1: int, field2: string, id:string, exception_desc:String")


    vaild_df_expected = spark_session.createDataFrame( [[1, "dummy", "12345678901211" ]], "field1: int, field2: string, id:string")

    print(invalid_df.collect())
    assert compare_df(invalid_df,invaild_df_expected)
    assert compare_df(valid_df,vaild_df_expected)



def test_type_validator_decimal_10_2(spark_session):
    VALIDATE_AGAINST_MAP = {
        "id": [TypeValidator(clm_type="decimal(10,2)")]
    }

    test_df = spark_session.createDataFrame([[1, "dummy", "123.21"], [2,None,"123456789"]], "field1: int, field2: string, id:string")

    excep_record_handler = DefaultExceptionRecordHandler()
    val_builder = ValidatorBuilder()
    data_val = val_builder.add_excp_rec_handler(excep_record_handler).add_validation_map(VALIDATE_AGAINST_MAP).add_validate_rec_df(test_df).build()
    valid_df,invalid_df = data_val.validate()


    invaild_df_expected = spark_session.createDataFrame([ [2,None,"123456789","id is not of given type decimal(10,2) |"]], "field1: int, field2: string, id:string, exception_desc:String")


    vaild_df_expected = spark_session.createDataFrame( [[1, "dummy", "123.21" ]], "field1: int, field2: string, id:string")

    print(invalid_df.collect())
    assert compare_df(invalid_df,invaild_df_expected)
    assert compare_df(valid_df,vaild_df_expected)

