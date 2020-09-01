from pyspark.sql import functions as sf
from src.org.validator.context import ExceptionValidatorContext
from src.org.validator.data_validator import DefaultExceptionRecordHandler, ValidatorBuilder
from src.org.validator.not_null_validator import NotNullValidator
from pyspark.sql import  Row

config  = ExceptionValidatorContext.confg

def test_null_check_custom_msg(spark_session):

    custom_msg_lmda1 = lambda p_clm, validate_against : sf.concat(sf.col(config.excep_clm_name), sf.lit(f"{p_clm} is empty ".format(p_clm,str(validate_against))),
                                                                  sf.lit(ExceptionValidatorContext.confg.exception_msg_seperator))
    validation_map = {
        "field1": [NotNullValidator(custom_msg_lmda = custom_msg_lmda1)],
        "field2": [NotNullValidator(custom_msg_lmda = custom_msg_lmda1)],
    }

    test_df = spark_session.createDataFrame([[1, "dummy"], [2,None ]], "field1: int, field2: string")

    excep_record_handler = DefaultExceptionRecordHandler()
    val_builder = ValidatorBuilder()
    data_val = val_builder.add_excp_rec_handler(excep_record_handler).add_validation_map(validation_map).add_validate_rec_df(test_df).build()
    valid_df,invalid_df = data_val.validate()

    invaild_df_expected = [Row(a=2, b=None, exception_desc='|field2 is empty |')]
    vaild_df_expected = [Row(a=1, b='dummy')]

    assert invalid_df.collect()== invaild_df_expected
    assert valid_df.collect()== vaild_df_expected