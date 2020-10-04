from pyspark.sql import functions as sf
from script.org.validator.base_validator import Validator
import logging

class TypeValidator(Validator):

    C_DEFAULT_EXCP_MSG="{} is not of given type {} "
    logger = logging.getLogger('example_logger')

    def __init__(self,  clm_type=None,custom_msg_lmda=None):
        self.clm_type = clm_type
        self.custom_msg_lmda= custom_msg_lmda

    def validate(self, p_df_to_validate, p_clm):
        self.logger.info(f"Validating column {p_clm} for data type {self.clm_type}")

        l_df = self._add_exeception_msg(p_df_to_validate,p_clm, self.clm_type)

        self.logger.info("Validation of column is done")
        return l_df


    def _compare_function(self,clm,validate_against):
        return lambda clm, clm_type : sf.col(clm).cast(clm_type).isNull()

    def _provide_custom_msg_lmda(self,p_clm,exp_clm_name):

        if self.custom_msg_lmda is None:
            return  lambda p_clm, clm_type : self._clm_sticher(exp_clm_name, sf.lit(self.C_DEFAULT_EXCP_MSG.format(p_clm,clm_type)))
        else:
            self.logger.info("Using provided Custom msg")
            return self.custom_msg_lmda

