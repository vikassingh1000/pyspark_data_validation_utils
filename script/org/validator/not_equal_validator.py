from script.org.validator.base_validator import Validator
from pyspark.sql import functions as sf
import logging

from script.org.validator.common_util import is_clm_null


class NotEqualValidator(Validator):

    C_DEFAULT_EXCP_MSG="Column {} has invaild value allowed type {}, however it has: "
    logger = logging.getLogger('example_logger')

    def __init__(self,  validate_against=None,custom_msg_lmda=None):
        self.validate_against = validate_against
        self.custom_msg_lmda = custom_msg_lmda

    def validate(self, p_df_to_validate, p_clm):
        self.logger.info(f"Validating column {p_clm} for not equal against: {self.validate_against}")

        l_df = self._add_exeception_msg(p_df_to_validate,p_clm, self.validate_against)

        self.logger.info("Validation of column is done")
        return l_df

    def _compare_function(self,clm,validate_against):
        return lambda p_clm, validate_against : self.__is_in(p_clm,validate_against)

    def __is_in(self,main_clm, cols):
        not_null_check =  (is_clm_null(main_clm,self.confg))
        if  isinstance(main_clm, str):
            main_clm = sf.col(main_clm)
        return ( not_null_check| ~(main_clm.isin(cols)))

    def _provide_custom_msg_lmda(self,p_clm,exp_clm_name):
        if self.custom_msg_lmda is None:
            self.logger.info("Using default message Custom msg")
            return  lambda p_clm, validate_against : self._clm_sticher(exp_clm_name, sf.lit(self.C_DEFAULT_EXCP_MSG.format(p_clm,validate_against)), sf.col(p_clm))
        else:
            self.logger.info("Using provided Custom msg")
            return self.custom_msg_lmda

