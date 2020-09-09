from script.org.validator.base_validator import Validator
from pyspark.sql import functions as sf
import logging

from script.org.validator.common_util import is_clm_null


class NotNullValidator(Validator):

    C_DEFAULT_EXCP_MSG="{} is null "
    logger = logging.getLogger('example_logger')

    def __init__(self,  custom_msg_lmda=None):
        self.custom_msg_lmda = custom_msg_lmda

    def validate(self, p_df_to_validate, p_clm):
        self.logger.info(f"Validating column {p_clm} for not null")

        l_df = self._add_exeception_msg(p_df_to_validate,p_clm, None)

        self.logger.info("Validation of column is done")
        return l_df

    def _compare_function(self,clm,validate_against):
        # creating wrapper around is_clm_null as compare_function suppose to recieve mutliple param, but is_null suppose to recive only one
        return lambda p_clm, *col : is_clm_null(p_clm,self.confg)

    def _provide_custom_msg_lmda(self,p_clm,exp_clm_name):

        if self.custom_msg_lmda is None:
            return  lambda p_clm, place_holder : self._clm_sticher(exp_clm_name, sf.lit(self.C_DEFAULT_EXCP_MSG.format(p_clm)))
        else:
            self.logger.info("Using provided Custom msg")
            return self.custom_msg_lmda

