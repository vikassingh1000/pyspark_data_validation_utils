import logging
from abc import ABC, abstractmethod

from pyspark.sql import DataFrame
from pyspark.sql import functions as sf

from script.org.validator.base_validator import Validator, IExceptionRecordHandler
from script.org.validator.common_util import is_clm_null
from script.org.validator.config import Configuration


class IDataValidator(ABC):

    @abstractmethod
    def validate(self, df_to_validate,)-> DataFrame:
        pass


# entry location for validation
class DataValidator(IDataValidator):
    clmn_to_validate_against = None
    exception_rec_handler= None
    df_to_validate = None
    excp_msg_clm_provider = None


    logger = logging.getLogger('example_logger')

    def __init__(self):
        self.add_config(Configuration())

    def add_config(self, config):
        self.confg= config
        self.excp_clm_nm =self.confg.excep_clm_name
        self.sep = self.confg.exception_msg_seperator

    def validate(self):

        self.logger.info("Starting validation")
        initial_clms = self.df_to_validate.schema.names
        p_df_to_validate = self._validate_rec(self.df_to_validate, self.clmn_to_validate_against)

        # TODO: We could configure it
        p_df_to_validate.cache()


        after_validation_clms = p_df_to_validate.schema.names

        li_dif = [i for i in after_validation_clms + initial_clms if i  in after_validation_clms and i not in initial_clms]
        self.logger.info(li_dif)
        excp_rec_cond = None

        for clm in li_dif:
            if excp_rec_cond is None:
                excp_rec_cond = (~is_clm_null(clm,self.confg) & (sf.col(clm)!=self.sep))
            else:
                excp_rec_cond =  excp_rec_cond | (~is_clm_null(clm,self.confg) &(sf.col(clm)!=self.sep))


        #TODO: Need to think if we should compare once and store boolean then use boolean for filtering
        df_with_valid_rec = p_df_to_validate.filter(~excp_rec_cond)
        df_with_invalid_rec = p_df_to_validate.filter(excp_rec_cond)

        if self.exception_rec_handler :
            df_with_invalid_rec = self.exception_rec_handler.handle(df_with_invalid_rec)

        self.logger.info("Validation Of Dataframe  done")
        return df_with_valid_rec.select(*initial_clms),df_with_invalid_rec

    ###############################
    #  This method will  iterate over passed map and new column to validate the records
    ###############################
    def _validate_rec(self,p_df_to_validate, p_clmn_to_validator):
        self.logger.info("Starting Validating records column using given validators :{}".format(p_clmn_to_validator))

        if self.confg.excp_msg_agg:
            self.logger.info("Exception message would be aggregated so, create column with name {} and value {} ".format(self.excp_clm_nm, self.sep))
            p_df_to_validate = p_df_to_validate.withColumn(self.excp_clm_nm, sf.lit(""))

        for l_clm, l_validator in p_clmn_to_validator.items():
            p_df_to_validate = self._validate_clm(l_validator, p_df_to_validate, l_clm)

        self.logger.info("Validation Of Dataframe record is done")
        return p_df_to_validate

    def _validate_clm(self, p_validator, p_df_to_validate, p_clm):
        self.logger.info("Validating column {} with {}".format(p_clm, p_validator))

        if isinstance(p_validator, list):
            for l_val in p_validator:
                p_df_to_validate = self.call_validator(l_val, p_clm, p_df_to_validate)
        elif isinstance(p_validator, Validator):
            null = self.call_validator(p_validator, p_clm, p_df_to_validate)
        else:
            raise Exception('Validator could be list of  Validators or single validator, however received: {} '.format(type(p_validator)))
        self.logger.info("Validation of column is Done")
        return p_df_to_validate

    def call_validator(self, l_val, p_clm, p_df_to_validate):
        l_val.set_excp_clm_provider(self.excp_msg_clm_provider)
        l_val.set_config(self.confg)
        p_df_to_validate = l_val.validate(p_df_to_validate, p_clm)
        return p_df_to_validate


class ValidatorBuilder(ABC):
    """
    The Builder interface specifies methods for creating the different parts of
    the Validator objects.
    """
    data_validator = DataValidator()

    def add_excp_rec_handler(self, excep_rec_handler) :

        if not isinstance(excep_rec_handler, IExceptionRecordHandler):
            raise Exception("Exception handler should implement IExceptionRecordHandler")
        self.data_validator.exception_rec_handler =excep_rec_handler
        return self


    def add_excp_msg_clm_provider(self, excp_msg_clm_provider) :

        self.data_validator.excp_msg_clm_provider = excp_msg_clm_provider
        return self


    def add_validation_map(self, clmn_to_validate_against):
        if not isinstance(clmn_to_validate_against, dict):
            raise Exception("Exception validation should be Dict")
        self.data_validator.clmn_to_validate_against = clmn_to_validate_against
        return self

    def add_validate_rec_df(self, vali_rec_Df):
        # if ~isinstance(clmn_to_validate_against, dict):
        #     raise Exception("Exception validation should be Dict")
        self.data_validator.df_to_validate = vali_rec_Df
        return self

    def add_config(self, config):
        self.data_validator.add_config(config)
        return self

    def build(self):
        return self.data_validator

class DefaultExceptionRecordHandler(IExceptionRecordHandler):
    logger = logging.getLogger('example_logger')
    def handle(self, exception_rec_df):
        self.logger.info("Handling  exception records, this is default behaviour it will simple ignore the dataframe")
        exception_rec_df.show()
        self.logger.info("Handled  exception records")
        return  exception_rec_df
