

from pyspark.sql import Column

from script.org.validator.base_validator import Validator
import logging

class EqualValidator(Validator):
    validate_against = None
    default_exp_message_for_list = "{clm} is not matching in given values: {value}"
    default_exp_message_for_single = "{clm} is not matching with expected value:{value}"
    default_exp_message_for_colm = "{clm} is not matching with expected value of:"

    logger = logging.getLogger('example_logger')

    def __init__(self, p_validate_against, custom_msg_lmda=None, excp_msg_clm_provider= None):
        self.validate_against = p_validate_against
        self.logger.info(custom_msg_lmda)
        self.custom_msg_lmda= custom_msg_lmda
        self.excp_msg_clm_provider = excp_msg_clm_provider

    def validate(self, p_df_to_validate, p_clm):
        self.logger.info(f"Validating column {p_clm} against equal value {self.validate_against}")

        l_df = self._add_exeception_msg(p_df_to_validate,p_clm, self.validate_against)

        self.logger.info("Validation of column is done")
        return l_df

    def _compare_function(self,clm,validate_against):
        compare_function = None
        if isinstance(validate_against, list):
            self.logger.info("Using list comparable")
            compare_function = lambda clm, validate_against : ~sf.col(clm).isin(validate_against)| (sf.col(clm).isNull())
        else:
            self.logger.info("eqNullSafe comparable")
            compare_function = lambda clm, validate_against : (~sf.col(clm).eqNullSafe(validate_against))
        return compare_function

    def _provide_custom_msg_lmda(self,p_clm,exp_clm_name):

        if self.custom_msg_lmda is None and (isinstance(self.validate_against, str)|isinstance(self.validate_against, list)):
            msg = EqualValidator.default_exp_message_for_single
            if isinstance(self.validate_against, list):
                msg = EqualValidator.default_exp_message_for_list
            self.logger.info("Using default message for EqualValidator")
            return  lambda p_clm, validate_against : self._clm_sticher(exp_clm_name, sf.lit(msg.format(clm=p_clm, value=validate_against)))

        elif self.custom_msg_lmda is None and isinstance(self.validate_against, Column):
            return  lambda p_clm, validate_against : self._clm_sticher(exp_clm_name, sf.lit(self.default_exp_message_for_colm.format(clm=p_clm)), validate_against)

        else:
            self.logger.info("Using provided Custom msg")
            return self.custom_msg_lmda