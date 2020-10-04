from pyspark.sql import functions as sf

from script.org.validator.config import Configuration
from script.org.validator.data_validator import DefaultExceptionRecordHandler, ValidatorBuilder

import logging

from script.org.validator.not_equal_validator import NotEqualValidator
from tests.test_util import compare_df

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)
logging.info('Admin logged in')


def test_not_equl_validator_custom_msg(spark_session):

    msg_part1="Column {} value's '"
    trn_msg_part1="Column trn_typ has value:"
    msg_part2=" expected values are: "
    # use concat_ws to safe gurd
    custom_msg_lmda = lambda p_clm, validate_against : sf.concat_ws("",sf.lit(trn_msg_part1), sf.col(p_clm), sf.lit(msg_part2), sf.lit(str(validate_against)))

    validation_map = {
        "trn_typ": [NotEqualValidator(['C','D'],custom_msg_lmda=custom_msg_lmda)],
    }

    test_df = spark_session.createDataFrame([[1, "Credit", "C"], [2,"Debit","D"], [3,"Invaild",None]], "trn_id: int, trn_desc: string, trn_typ:String")


    excep_record_handler = DefaultExceptionRecordHandler()
    val_builder = ValidatorBuilder()
    config = Configuration()
    #

    data_val = val_builder.add_excp_rec_handler(excep_record_handler).add_config(config).add_validation_map(validation_map).add_validate_rec_df(test_df).build()
    valid_df,invalid_df = data_val.validate()


    invaild_df_expected = spark_session.createDataFrame([ [3,"Invaild",None,"Column trn_typ has value: expected values are: ['C', 'D']|"]], \
                                                        "trn_id: int, trn_desc: string, trn_typ:String, exception_desc:String")


    vaild_df_expected = spark_session.createDataFrame([[1, "Credit", "C"], [2,"Debit","D"]], "trn_id: int, trn_desc: string, trn_typ:String")

    assert compare_df(invalid_df,invaild_df_expected)
    assert compare_df(valid_df,vaild_df_expected)



def test_not_equl_validator_custom_msg_multi_columns(spark_session):

    trn_msg_part1="Column {} has value:"
    msg_part2=" expected values are: "
    custom_msg_lmda = lambda p_clm, validate_against : sf.concat_ws("",sf.lit(trn_msg_part1.format(p_clm)), sf.col(p_clm), sf.lit(msg_part2), sf.lit(str(validate_against)))

    validation_map = {
        "trn_typ": [NotEqualValidator(['C','D'],custom_msg_lmda=custom_msg_lmda)],
        "trn_desc": [NotEqualValidator(['Credit','Debit'],custom_msg_lmda=custom_msg_lmda)],
    }

    test_df = spark_session.createDataFrame([[1, "Credit", "C"], [2,"Debit","D"], [3,"Invaild",None]], "trn_id: int, trn_desc: string, trn_typ:String")


    excep_record_handler = DefaultExceptionRecordHandler()
    val_builder = ValidatorBuilder()
    config = Configuration()
    #

    data_val = val_builder.add_excp_rec_handler(excep_record_handler).add_config(config).add_validation_map(validation_map).add_validate_rec_df(test_df).build()
    valid_df,invalid_df = data_val.validate()


    invaild_df_expected = spark_session.createDataFrame([ [3,"Invaild",None,"Column trn_typ has value: expected values are: ['C', 'D']|Column trn_desc has value:Invaild expected values are: ['Credit', 'Debit']|"]], \
                                                        "trn_id: int, trn_desc: string, trn_typ:String, exception_desc:String")


    vaild_df_expected = spark_session.createDataFrame([[1, "Credit", "C"], [2,"Debit","D"]], "trn_id: int, trn_desc: string, trn_typ:String")

    assert compare_df(invalid_df,invaild_df_expected)
    assert compare_df(valid_df,vaild_df_expected)


def test_not_equl_validator_custom_msg_multi_columns(spark_session):

    msg_part1="Column {} value's '"
    trn_msg_part1="Column {} has value:"
    msg_part2=" expected values are: "
    custom_msg_lmda = lambda p_clm, validate_against : sf.concat_ws("",sf.lit(trn_msg_part1.format(p_clm)), sf.col(p_clm), sf.lit(msg_part2), sf.lit(str(validate_against)))

    validation_map = {
        "trn_typ": [NotEqualValidator(['C','D'],custom_msg_lmda=custom_msg_lmda)],
        "trn_desc": [NotEqualValidator(['Credit','Debit'],custom_msg_lmda=custom_msg_lmda)],
    }

    test_df = spark_session.createDataFrame([[1, "Credit", "C"], [2,"Debit","D"], [3,"Invaild",None]], "trn_id: int, trn_desc: string, trn_typ:String")


    excep_record_handler = DefaultExceptionRecordHandler()
    val_builder = ValidatorBuilder()
    config = Configuration()
    #

    data_val = val_builder.add_excp_rec_handler(excep_record_handler).add_config(config).add_validation_map(validation_map).add_validate_rec_df(test_df).build()
    valid_df,invalid_df = data_val.validate()


    invaild_df_expected = spark_session.createDataFrame([ [3,"Invaild",None,"Column trn_typ has value: expected values are: ['C', 'D']|Column trn_desc has value:Invaild expected values are: ['Credit', 'Debit']|"]], \
                                                        "trn_id: int, trn_desc: string, trn_typ:String,exception_desc:String ")


    vaild_df_expected = spark_session.createDataFrame([[1, "Credit", "C"], [2,"Debit","D"]], "trn_id: int, trn_desc: string, trn_typ:String")

    assert compare_df(invalid_df,invaild_df_expected)
    assert compare_df(valid_df,vaild_df_expected)


def test_not_equl_validator_custom_msg_multi_clm_cust_clm_name(spark_session):

    msg_part1="Column {} value's '"
    trn_msg_part1="Column {} has value:"
    msg_part2=" expected values are: "

    custom_msg_lmda = lambda p_clm, validate_against : sf.concat_ws("",sf.lit(trn_msg_part1.format(p_clm)), sf.col(p_clm), sf.lit(msg_part2), sf.lit(str(validate_against)))

    validation_map = {
        "trn_typ": [NotEqualValidator(['C','D'],custom_msg_lmda=custom_msg_lmda)],
        "trn_desc": [NotEqualValidator(['Credit','Debit'],custom_msg_lmda=custom_msg_lmda)],
    }

    test_df = spark_session.createDataFrame([[1, "Credit", "C"], [2,"Debit","D"], [3,"Invaild",None]], "trn_id: int, trn_desc: string, trn_typ:String")


    excep_record_handler = DefaultExceptionRecordHandler()
    val_builder = ValidatorBuilder()
    config = Configuration()
    config.excp_msg_agg = False
    excp_msg_clm_provider = lambda clm: clm + "_error_1"

    data_val = val_builder.add_excp_rec_handler(excep_record_handler).add_config(config).add_validation_map(validation_map) \
        .add_excp_clm_provider(excp_msg_clm_provider).add_validate_rec_df(test_df).build()
    valid_df,invalid_df = data_val.validate()


    invaild_df_expected = spark_session.createDataFrame([ [3,"Invaild",None,"Column trn_typ has value: expected values are: ['C', 'D']|","Column trn_desc has value:Invaild expected values are: ['Credit', 'Debit']|"]], \
                                                        "trn_id: int, trn_desc: string, trn_typ:String,add_excp_clm_provider:String,  trn_desc_error_1:String")


    vaild_df_expected = spark_session.createDataFrame([[1, "Credit", "C"], [2,"Debit","D"]], "trn_id: int, trn_desc: string, trn_typ:String")

    assert compare_df(invalid_df,invaild_df_expected)
    assert compare_df(valid_df,vaild_df_expected)



def test_not_equl_validator_null_column(spark_session):

    validation_map = {
        "trn_typ": [NotEqualValidator(['C','D'])],
    }

    test_df = spark_session.createDataFrame([[1, "Credit", "C"], [2,"Debit","D"], [3,"Invaild",None]], "trn_id: int, trn_desc: string, trn_typ:String")


    excep_record_handler = DefaultExceptionRecordHandler()
    val_builder = ValidatorBuilder()
    config = Configuration()
    #

    data_val = val_builder.add_excp_rec_handler(excep_record_handler).add_config(config).add_validation_map(validation_map).add_validate_rec_df(test_df).build()
    valid_df,invalid_df = data_val.validate()


    invaild_df_expected = spark_session.createDataFrame([ [3,"Invaild",None,"Column trn_typ has invaild value allowed type ['C', 'D'], however it has: |"]], \
                                                        "trn_id: int, trn_desc: string, trn_typ:String, exception_desc:String")


    vaild_df_expected = spark_session.createDataFrame([[1, "Credit", "C"], [2,"Debit","D"]], "trn_id: int, trn_desc: string, trn_typ:String")

    assert compare_df(invalid_df,invaild_df_expected)
    assert compare_df(valid_df,vaild_df_expected)


def test_not_equl_validator_empty_column(spark_session):

    validation_map = {
        "trn_typ": [NotEqualValidator(['C','D'])],
    }

    test_df = spark_session.createDataFrame([[1, "Credit", "C"], [2,"Debit","D"], [3,"Invaild",""]], "trn_id: int, trn_desc: string, trn_typ:String")


    excep_record_handler = DefaultExceptionRecordHandler()
    val_builder = ValidatorBuilder()
    config = Configuration()
    #

    data_val = val_builder.add_excp_rec_handler(excep_record_handler).add_config(config).add_validation_map(validation_map).add_validate_rec_df(test_df).build()
    valid_df,invalid_df = data_val.validate()


    invaild_df_expected = spark_session.createDataFrame([ [3,"Invaild",None,"Column trn_typ has invaild value allowed type ['C', 'D'], however it has: |"]], \
                                                        "trn_id: int, trn_desc: string, trn_typ:String, exception_desc:String")


    vaild_df_expected = spark_session.createDataFrame([[1, "Credit", "C"], [2,"Debit","D"]], "trn_id: int, trn_desc: string, trn_typ:String")

    assert compare_df(invalid_df,invaild_df_expected)
    assert compare_df(valid_df,vaild_df_expected)


def test_not_equl_validator_vanilla(spark_session):

    validation_map = {
        "trn_typ": [NotEqualValidator(['C','D'])],
    }

    test_df = spark_session.createDataFrame([[1, "Credit", "C"], [2,"Debit","D"], [3,"Invaild","T"]], "trn_id: int, trn_desc: string, trn_typ:String")

    excep_record_handler = DefaultExceptionRecordHandler()
    val_builder = ValidatorBuilder()
    data_val = val_builder.add_excp_rec_handler(excep_record_handler).add_validation_map(validation_map).add_validate_rec_df(test_df).build()
    valid_df,invalid_df = data_val.validate()

    invalid_df.show(10,False)
    valid_df.show()
    invaild_df_expected = spark_session.createDataFrame([ [3,"Invaild","T","Column trn_typ has invaild value allowed type ['C', 'D'], however it has: T|"]], \
                                                        "trn_id: int, trn_desc: string, trn_typ:String, exception_desc:String")
    invaild_df_expected.show(10,False)

    vaild_df_expected = spark_session.createDataFrame([[1, "Credit", "C"], [2,"Debit","D"]], "trn_id: int, trn_desc: string, trn_typ:String")

    assert compare_df(invalid_df,invaild_df_expected)
    assert compare_df(valid_df,vaild_df_expected)
