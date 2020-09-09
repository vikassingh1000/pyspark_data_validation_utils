from pyspark.sql import functions as sf
from script.org.validator.context import ExceptionValidatorContext
from script.org.validator.data_validator import DefaultExceptionRecordHandler, ValidatorBuilder

from pyspark.sql import  Row
import logging
logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)
logging.info('Admin logged in')

from script.org.validator.equal_validator import EqualValidator



def test_equl_check_with_list_value_non_agg_msg_custom_msg(spark_session):


    ExceptionValidatorContext.confg.excp_msg_agg= False
    custom_msg_lmda = lambda p_clm, validate_against : sf.concat(sf.lit(f"{p_clm}'s, has "),sf.when(sf.col(p_clm).isNotNull(), sf.col(p_clm) ).otherwise(sf.lit(" empty ")), sf.lit(f" value which does not fall in allwoed values {str(validate_against)} "))
    excp_msg_clm_provider = lambda clm: clm + "_error_1"
    VALIDATE_AGAINST_MAP = {
        "foreign_key1": [EqualValidator(p_validate_against=["foreign_key1","foreign_key2"], excp_msg_clm_provider =excp_msg_clm_provider,custom_msg_lmda=custom_msg_lmda)],
        "field2": [EqualValidator(p_validate_against=["dummy","foreign_key2"],excp_msg_clm_provider = excp_msg_clm_provider,custom_msg_lmda=custom_msg_lmda)],
    }

    test_df = spark_session.createDataFrame([[1, "dummy", "foreign_key1"], [2,None,"invaild"]], "field1: int, field2: string, foreign_key1:String")

    excep_record_handler = DefaultExceptionRecordHandler()
    val_builder = ValidatorBuilder()
    data_val = val_builder.add_excp_rec_handler(excep_record_handler).add_validation_map(VALIDATE_AGAINST_MAP).add_validate_rec_df(test_df).add_excp_msg_clm_provider(excp_msg_clm_provider).build()
    valid_df,invalid_df = data_val.validate()

    invaild_df_expected = [Row(a=2, b=None, c="invaild" , d="foreign_key1's, has invaild value which does not fall in allwoed values ['foreign_key1', 'foreign_key2'] |",e ="field2's, has  empty  value which does not fall in allwoed values ['dummy', 'foreign_key2'] |" )]
    print( invalid_df.collect()[0])
    print(invaild_df_expected[0])

    vaild_df_expected = [Row(a=1, b='dummy',d="foreign_key1" )]

    assert invalid_df.collect()== invaild_df_expected
    assert valid_df.collect()== vaild_df_expected
    ExceptionValidatorContext.confg.excp_msg_agg= True


def test_equl_check_with_list_value_non_agg_msg_default_msg(spark_session):


    ExceptionValidatorContext.confg.excp_msg_agg= False
    # custom_msg_lmda = lambda p_clm, validate_against, sep : sf.concat(sf.col('exception_desc'), sf.lit("Column {}, should be matching with: ".format(p_clm)), validate_against, sf.lit(sep))
    excp_msg_clm_provider = lambda clm: clm + "_error_1"
    VALIDATE_AGAINST_MAP = {
        "foreign_key1": [EqualValidator(p_validate_against=["foreign_key1","foreign_key2"], excp_msg_clm_provider =excp_msg_clm_provider)],
        "field2": [EqualValidator(p_validate_against=["dummy","foreign_key2"],excp_msg_clm_provider = excp_msg_clm_provider)],
    }


    test_df = spark_session.createDataFrame([[1, "dummy", "foreign_key1"], [2,None,"invaild"]], "field1: int, field2: string, foreign_key1:String")

    excep_record_handler = DefaultExceptionRecordHandler()
    val_builder = ValidatorBuilder()
    data_val = val_builder.add_excp_rec_handler(excep_record_handler).add_validation_map(VALIDATE_AGAINST_MAP).add_validate_rec_df(test_df).add_excp_msg_clm_provider(excp_msg_clm_provider).build()
    valid_df,invalid_df = data_val.validate()

    invaild_df_expected = [Row(a=2, b=None, c="invaild" , d="foreign_key1 is not matching in given values: ['foreign_key1', 'foreign_key2']|",e ="field2 is not matching in given values: ['dummy', 'foreign_key2']|" )]
    print( invalid_df.collect()[0])
    print(invaild_df_expected[0])

    vaild_df_expected = [Row(a=1, b='dummy',d="foreign_key1" )]

    assert invalid_df.collect()== invaild_df_expected
    assert valid_df.collect()== vaild_df_expected
    ExceptionValidatorContext.confg.excp_msg_agg= True

def test_equl_check_with_list_value_custom_msg(spark_session):

    # Check UDF.
    custom_msg_lmda_frg = lambda p_clm, validate_against :  sf.lit("Allowed value for column {} are {} ".format(p_clm,str(validate_against)))
    custom_msg_lmda_field2 = lambda p_clm, validate_against : sf.lit("Column  {}, must have value from one of allowed values:{} ".format(p_clm,str(validate_against)))
    VALIDATE_AGAINST_MAP = {
        "foreign_key1": [EqualValidator(p_validate_against=["foreign_key1","foreign_key2"],custom_msg_lmda=custom_msg_lmda_frg)],
        "field2": [EqualValidator(p_validate_against=["dummy","foreign_key2"],custom_msg_lmda=custom_msg_lmda_field2)],
    }


    test_df = spark_session.createDataFrame([[1, "dummy", "foreign_key1"], [2,None,"invaild"]], "field1: int, field2: string, foreign_key1:String")

    excep_record_handler = DefaultExceptionRecordHandler()
    val_builder = ValidatorBuilder()
    data_val = val_builder.add_excp_rec_handler(excep_record_handler).add_validation_map(VALIDATE_AGAINST_MAP).add_validate_rec_df(test_df).build()
    valid_df,invalid_df = data_val.validate()
    invalid_df.show(10,False)
    print(invalid_df.collect())

    invaild_df_expected = [Row(a=2, b=None, c="invaild" , exception_desc="Allowed value for column foreign_key1 are ['foreign_key1', 'foreign_key2'] |Column  field2, must have value from one of allowed values:['dummy', 'foreign_key2'] |")]
    print( invalid_df.collect()[0])
    print(invaild_df_expected[0])
    valid_df.show()
    vaild_df_expected = [Row(a=1, b='dummy',d="foreign_key1" )]

    assert invalid_df.collect()== invaild_df_expected
    assert valid_df.collect()== vaild_df_expected

def test_equl_check_with_column_value_custom_msg(spark_session):
    config  = ExceptionValidatorContext.confg
    custom_msg_lmda = lambda p_clm, validate_against : sf.concat(sf.lit(f"Column {p_clm}, should be matching with: "), validate_against)
    VALIDATE_AGAINST_MAP = {
        "foreign_key1": [EqualValidator(p_validate_against=sf.col("foreign_key2"), custom_msg_lmda = custom_msg_lmda)],
    }


    test_df = spark_session.createDataFrame([[1, "dummy", "foreign_key1","foreign_key1"], [2,None,"invaild","foreign_key2" ]], "field1: int, field2: string, foreign_key1:String,foreign_key2:String")

    excep_record_handler = DefaultExceptionRecordHandler()
    val_builder = ValidatorBuilder()
    data_val = val_builder.add_excp_rec_handler(excep_record_handler).add_validation_map(VALIDATE_AGAINST_MAP).add_validate_rec_df(test_df).build()
    valid_df,invalid_df = data_val.validate()
    invalid_df.show(10,False)
    print(invalid_df.collect())

    invaild_df_expected = [Row(a=2, b=None, c="invaild" ,d="foreign_key2", exception_desc='Column foreign_key1, should be matching with: foreign_key2|')]
    print( invalid_df.collect()[0])
    print(invaild_df_expected[0])
    vaild_df_expected = [Row(a=1, b='dummy',d="foreign_key1",foreign_key2="foreign_key1" )]

    assert invalid_df.collect()== invaild_df_expected
    assert valid_df.collect()== vaild_df_expected

def test_equl_check_with_column_value_default_msg(spark_session):

    # custom_msg_lmda = lambda p_clm, validate_against, sep : sf.concat(sf.col('exception_desc'), sf.lit("{} should match with allowed value {}".format(p_clm,"dummy")), sf.lit(sep))
    VALIDATE_AGAINST_MAP = {
        "foreign_key1": [EqualValidator(p_validate_against=sf.col("foreign_key2"))],
    }


    test_df = spark_session.createDataFrame([[1, "dummy", "foreign_key1","foreign_key1"], [2,None,"invaild","foreign_key2" ]], "field1: int, field2: string, foreign_key1:String,foreign_key2:String")

    excep_record_handler = DefaultExceptionRecordHandler()
    val_builder = ValidatorBuilder()
    data_val = val_builder.add_excp_rec_handler(excep_record_handler).add_validation_map(VALIDATE_AGAINST_MAP).add_validate_rec_df(test_df).build()
    valid_df,invalid_df = data_val.validate()
    invalid_df.show(10,False)
    print(invalid_df.collect())

    invaild_df_expected = [Row(a=2, b=None, c="invaild" ,d="foreign_key2", exception_desc='foreign_key1 is not matching with expected value of:foreign_key2|')]
    print( invalid_df.collect()[0])
    print(invaild_df_expected[0])
    vaild_df_expected = [Row(a=1, b='dummy',d="foreign_key1",foreign_key2="foreign_key1" )]

    assert invalid_df.collect()== invaild_df_expected
    assert valid_df.collect()== vaild_df_expected

def test_equl_check_with_default_value_custom_msg(spark_session):

    custom_msg_lmda = lambda p_clm, validate_against : sf.lit(f"{p_clm} should match with allowed value: dummy")
    # logger.info("---" + custom_msg_lmda)
    VALIDATE_AGAINST_MAP = {
        "field2": [EqualValidator(p_validate_against="dummy",custom_msg_lmda=custom_msg_lmda)],
    }


    test_df = spark_session.createDataFrame([[1, "dummy", "foreign_key1"], [2,None,"foreign_key2" ]], "field1: int, field2: string, foreign_key:String")

    excep_record_handler = DefaultExceptionRecordHandler()
    val_builder = ValidatorBuilder()
    data_val = val_builder.add_excp_rec_handler(excep_record_handler).add_validation_map(VALIDATE_AGAINST_MAP).add_validate_rec_df(test_df).build()
    valid_df,invalid_df = data_val.validate()
    invalid_df.show(10,False)
    print(invalid_df.collect())

    invaild_df_expected = [Row(a=2, b=None, c="foreign_key2" , exception_desc='field2 should match with allowed value: dummy|')]
    print( invalid_df.collect()[0])
    print(invaild_df_expected[0])
    vaild_df_expected = [Row(a=1, b='dummy',foreign_key="foreign_key1")]

    assert invalid_df.collect()== invaild_df_expected
    assert valid_df.collect()== vaild_df_expected

def test_equl_check_with_default_value(spark_session):
    VALIDATE_AGAINST_MAP = {
        "field2": [EqualValidator(p_validate_against="dummy")],
    }


    test_df = spark_session.createDataFrame([[1, "dummy", "foreign_key1"], [2,None,"foreign_key2" ]], "field1: int, field2: string, foreign_key:String")

    excep_record_handler = DefaultExceptionRecordHandler()
    val_builder = ValidatorBuilder()
    data_val = val_builder.add_excp_rec_handler(excep_record_handler).add_validation_map(VALIDATE_AGAINST_MAP).add_validate_rec_df(test_df).build()
    valid_df,invalid_df = data_val.validate()
    invalid_df.show(10,False)
    print(invalid_df.collect())

    invaild_df_expected = [Row(a=2, b=None, c="foreign_key2" , exception_desc='field2 is not matching with expected value:dummy|')]
    print( invalid_df.collect()[0])
    print(invaild_df_expected[0])
    vaild_df_expected = [Row(a=1, b='dummy',foreign_key="foreign_key1")]

    assert invalid_df.collect()== invaild_df_expected
    assert valid_df.collect()== vaild_df_expected