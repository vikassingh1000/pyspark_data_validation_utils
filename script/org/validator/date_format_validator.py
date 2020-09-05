from pyspark.sql import functions as sf
from script.org.validator.base_validator import Validator
import logging

class DateFormatValidator(Validator):
    format = None
    C_DEFAULT_EXCP_MSG = "{p_clm} is not a validate date as per format {date_format}"
    logger = logging.getLogger('example_logger')

    def __init__(self, p_format,custom_msg_lmda=None):
        self.logger.info(f"Date format passed {p_format}")
        self.p_format = p_format
        self.custom_msg_lmda = custom_msg_lmda

    def validate(self, p_df_to_validate, p_clm):
        self.logger.info(f"Validating column {p_clm} against equal value {self.p_format}")

        l_df = self._add_exeception_msg(p_df_to_validate,p_clm, self.p_format)

        self.logger.info("Date format Validation of column is done")
        return l_df

    def _compare_function(self,clm,validate_against):
        return lambda clm, date_format : sf.to_date(sf.col(clm), date_format).isNull()

    def _provide_custom_msg_lmda(self,p_clm,exp_clm_name):

        if self.custom_msg_lmda is None:
            return  lambda p_clm, date_format : self._clm_sticher(exp_clm_name, sf.lit(self.C_DEFAULT_EXCP_MSG.format(p_clm=p_clm,date_format=date_format)))
        else:
            self.logger.info("Using provided Custom msg")
            return self.custom_msg_lmda

