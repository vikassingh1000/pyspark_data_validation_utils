from pyspark.sql import functions as sf

from script.org.validator.context import ExceptionValidatorContext
from script.org.validator.data_validator import DefaultExceptionRecordHandler, ValidatorBuilder
from script.org.validator.not_null_validator import NotNullValidator
from pyspark.sql import  Row

import logging
logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)
logging.info('Admin logged in')


def test_null_check_custom_msg(spark_session):

    custom_msg_lmda1 = lambda p_clm, validate_against : sf.concat( sf.lit(f"{p_clm} is empty ".format(p_clm,str(validate_against))))
    validation_map = {
        "field1": [NotNullValidator(custom_msg_lmda = custom_msg_lmda1)],
        "field2": [NotNullValidator(custom_msg_lmda = custom_msg_lmda1)],
    }

    test_df = spark_session.createDataFrame([[1, "dummy"], [2,None ]], "field1: int, field2: string")

    excep_record_handler = DefaultExceptionRecordHandler()
    val_builder = ValidatorBuilder()
    data_val = val_builder.add_excp_rec_handler(excep_record_handler).add_validation_map(validation_map).add_validate_rec_df(test_df).build()
    valid_df,invalid_df = data_val.validate()

    invaild_df_expected = [Row(a=2, b=None, exception_desc='field2 is empty |')]
    vaild_df_expected = [Row(a=1, b='dummy')]
    print("This is test")

    assert invalid_df.collect()== invaild_df_expected
    assert valid_df.collect()== vaild_df_expected

def test_empty_check_with_custom_excp_clm(spark_session):
    C_FACT_CLASS_ENTITY_FIELD_TO_VALIDATE_AGAINST = {
        "a": [NotNullValidator()],
        "b": [NotNullValidator()],
    }


    test_df = spark_session.createDataFrame([[1, "dummy"], [2, ""]], "a: int, b: string")

    excep_record_handler = DefaultExceptionRecordHandler()
    val_builder = ValidatorBuilder()
    data_val = val_builder.add_excp_rec_handler(excep_record_handler).add_validation_map(C_FACT_CLASS_ENTITY_FIELD_TO_VALIDATE_AGAINST).add_validate_rec_df(test_df).build()
    valid_df,invalid_df = data_val.validate()

    print(invalid_df.collect())
    invaild_df_expected = [Row(a=2, b='', exception_desc='b is null |')]
    vaild_df_expected = [Row(a=1, b='dummy')]
    assert invalid_df.collect()== invaild_df_expected
    assert valid_df.collect()== vaild_df_expected


def test_empty_check(spark_session):
    C_FACT_CLASS_ENTITY_FIELD_TO_VALIDATE_AGAINST = {
        "a": [NotNullValidator()],
        "b": [NotNullValidator()],
    }


    test_df = spark_session.createDataFrame([[1, "dummy"], [2, ""]], "a: int, b: string")

    excep_record_handler = DefaultExceptionRecordHandler()
    val_builder = ValidatorBuilder()
    data_val = val_builder.add_excp_rec_handler(excep_record_handler).add_validation_map(C_FACT_CLASS_ENTITY_FIELD_TO_VALIDATE_AGAINST).add_validate_rec_df(test_df).build()
    valid_df,invalid_df = data_val.validate()

    print(invalid_df.collect())
    invaild_df_expected = [Row(a=2, b='', exception_desc='b is null |')]
    vaild_df_expected = [Row(a=1, b='dummy')]
    assert invalid_df.collect()== invaild_df_expected
    assert valid_df.collect()== vaild_df_expected

def test_null_check(spark_session):
    C_FACT_CLASS_ENTITY_FIELD_TO_VALIDATE_AGAINST = {
        "a": [NotNullValidator()],
        "b": [NotNullValidator()],
    }


    test_df = spark_session.createDataFrame([[1, "dummy"], [2, None]], "a: int, b: string")

    excep_record_handler = DefaultExceptionRecordHandler()
    val_builder = ValidatorBuilder()
    data_val = val_builder.add_excp_rec_handler(excep_record_handler).add_validation_map(C_FACT_CLASS_ENTITY_FIELD_TO_VALIDATE_AGAINST).add_validate_rec_df(test_df).build()
    valid_df,invalid_df = data_val.validate()

    print(invalid_df.collect())
    invaild_df_expected = [Row(a=2, b=None, exception_desc='b is null |')]
    vaild_df_expected = [Row(a=1, b='dummy')]

    assert invalid_df.collect()== invaild_df_expected
    assert valid_df.collect()== vaild_df_expected
def test_null_check_custom_msg_with_clm_name(spark_session):
    config  = ExceptionValidatorContext.confg

    custom_msg_lmda_field1 = lambda p_clm, validate_against : sf.lit(f"{p_clm} is empty ")
    custom_msg_lmda_field2 = lambda p_clm, validate_against : sf.concat(sf.col("foreign_key"), sf.lit(" is not present in system"))

    # p_lookup_clm_nme = "foreign_key" ,p_excep_msg = "Foreign key not present in system: "
    C_FACT_CLASS_ENTITY_FIELD_TO_VALIDATE_AGAINST = {
        "field1": [NotNullValidator(custom_msg_lmda = custom_msg_lmda_field1)],
        "field2": [NotNullValidator(custom_msg_lmda = custom_msg_lmda_field2)],
    }


    test_df = spark_session.createDataFrame([[1, "dummy", "foreign_key1"], [None,None,"foreign_key2" ]], "field1: int, field2: string, foreign_key:String")

    excep_record_handler = DefaultExceptionRecordHandler()
    val_builder = ValidatorBuilder()
    data_val = val_builder.add_excp_rec_handler(excep_record_handler).add_validation_map(C_FACT_CLASS_ENTITY_FIELD_TO_VALIDATE_AGAINST).add_validate_rec_df(test_df).build()
    valid_df,invalid_df = data_val.validate()
    invalid_df.show(10,False)
    print(invalid_df.collect())

    invaild_df_expected = [Row(a=None, b=None, c="foreign_key2" , exception_desc='field1 is empty |foreign_key2 is not present in system|')]
    print(invaild_df_expected)
    vaild_df_expected = [Row(a=1, b='dummy',foreign_key="foreign_key1")]
    invaild_row = invalid_df.collect()
    assert invaild_row == invaild_df_expected

    assert valid_df.collect()== vaild_df_expected