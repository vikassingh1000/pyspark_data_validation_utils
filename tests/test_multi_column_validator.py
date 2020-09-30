from pyspark.sql import functions as sf

from script.org.validator.config import Configuration
from script.org.validator.data_validator import DefaultExceptionRecordHandler, ValidatorBuilder

from pyspark.sql import  Row
import logging

from script.org.validator.equal_validator import EqualValidator
from script.org.validator.not_null_validator import NotNullValidator

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)
logging.info('Admin logged in')

def test_equl_check_with_list_value_non_agg_msg_default_msg(spark_session):


    config = Configuration()
    config.excp_msg_agg=False
    # custom_msg_lmda = lambda p_clm, validate_against, sep : sf.concat(sf.col('exception_desc'), sf.lit("Column {}, should be matching with: ".format(p_clm)), validate_against, sf.lit(sep))
    excp_msg_clm_provider = lambda clm: clm + "_error_1"
    VALIDATE_AGAINST_MAP = {
        "foreign_key1": [EqualValidator(p_validate_against=["foreign_key1","foreign_key2"], excp_msg_clm_provider =excp_msg_clm_provider)],
        "field2": [EqualValidator(p_validate_against=["dummy","foreign_key2"],excp_msg_clm_provider = excp_msg_clm_provider)],
    }


    test_df = spark_session.createDataFrame([[1, "dummy", "foreign_key1"], [2,None,"invaild"]], "field1: int, field2: string, foreign_key1:String")

    excep_record_handler = DefaultExceptionRecordHandler()
    val_builder = ValidatorBuilder()
    data_val = val_builder.add_excp_rec_handler(excep_record_handler).add_validation_map(VALIDATE_AGAINST_MAP) \
        .add_validate_rec_df(test_df).add_config(config).add_excp_msg_clm_provider(excp_msg_clm_provider).build()
    valid_df,invalid_df = data_val.validate()

    invaild_df_expected = [Row(a=2, b=None, c="invaild" , d="foreign_key1 is not matching in given values: ['foreign_key1', 'foreign_key2']|",e ="field2 is not matching in given values: ['dummy', 'foreign_key2']|" )]
    print( invalid_df.collect()[0])
    print(invaild_df_expected[0])

    vaild_df_expected = [Row(a=1, b='dummy',d="foreign_key1" )]

    assert invalid_df.collect()== invaild_df_expected
    assert valid_df.collect()== vaild_df_expected

def test_null_check_custom_msg_with_clm_name(spark_session):
    config = Configuration()
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
    data_val = val_builder.add_excp_rec_handler(excep_record_handler).add_config(config).add_validation_map(C_FACT_CLASS_ENTITY_FIELD_TO_VALIDATE_AGAINST).add_validate_rec_df(test_df).build()
    valid_df,invalid_df = data_val.validate()
    invalid_df.show(10,False)
    print(invalid_df.collect())

    invaild_df_expected = [Row(a=None, b=None, c="foreign_key2" , exception_desc='field1 is empty |foreign_key2 is not present in system|')]
    print(invaild_df_expected)
    vaild_df_expected = [Row(a=1, b='dummy',foreign_key="foreign_key1")]
    invaild_row = invalid_df.collect()
    assert invaild_row == invaild_df_expected

    assert valid_df.collect()== vaild_df_expected