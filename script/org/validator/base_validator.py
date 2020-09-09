
from pyspark.sql import functions as sf
import logging

from pyspark.sql import DataFrame
from abc import ABC, abstractmethod

from script.org.validator.config import Configuration


class IDataValidator(ABC):

    @abstractmethod
    def validate(self, df_to_validate, ) -> DataFrame:
        pass


class Validator(ABC):
    confg = Configuration()
    sep = confg.exception_msg_seperator
    excep_desc_clm_name = confg.excep_clm_name
    excp_msg_clm_provider = None

    @abstractmethod
    def validate(self, df_to_validate, clm, exception_context) -> None:
        pass

    def set_excp_clm_provider(self, excp_clm_provider):
        self.excp_clm_provider = excp_clm_provider

    @abstractmethod
    def _compare_function(self, clm, validate_against) -> None:
        pass

    @abstractmethod
    def _provide_custom_msg_lmda(self, p_clm, exp_clm_name) -> None:
        pass

    def _add_exeception_msg(self, p_df_to_validate, p_clm, validate_against):
        try:

            exp_clm_name = self._excp_msg_clm_name_provider(p_clm)

            custom_msg_lmda = self._provide_custom_msg_lmda(p_clm, exp_clm_name)

            compare_function = self._compare_function(p_clm, validate_against)

            default_val_for_valid_rec = self._default_val_for_valida_rec(exp_clm_name)
            generate_msg = lambda p_clm, validate_against, exp_clm_name: self._clm_sticher(exp_clm_name, custom_msg_lmda(p_clm, validate_against), sf.lit(self.sep))
            return p_df_to_validate.withColumn(exp_clm_name, sf.when(compare_function(p_clm, validate_against), generate_msg(p_clm, validate_against, exp_clm_name))
                                                                                                                                .otherwise( default_val_for_valid_rec))
        except Exception as e:
            self.logger.error("Fail to execute validator with error: " + str(e))
            raise ValidationError("Fail to execute validator", e)

    def _clm_sticher(self, excp_clm_name, *col):

        if self.confg.excp_msg_agg:
            return sf.concat(sf.col(excp_clm_name), *col)
        else:
            return sf.concat(*col)

    def _default_val_for_valida_rec(self, excp_clm_name):

        if self.confg.excp_msg_agg:
            return sf.col(excp_clm_name)
        else:
            return sf.lit(None)

    def _excp_msg_clm_name_provider(self, clm_under_validation):
        excp_clm_name = None
        if self.excp_msg_clm_provider is None:
            excp_clm_name = self.confg.excep_clm_name
            self.logger.info("Using default_excp_clm_name {}".format(excp_clm_name))
        else:

            excp_clm_name = self.excp_msg_clm_provider(clm_under_validation)
            self.logger.info("Using passed column provider {}".format(excp_clm_name))
        return excp_clm_name


class IExceptionRecordHandler(ABC):

    @abstractmethod
    def handle(self, exception_rec_df) -> DataFrame:
        pass


class ValidationError(Exception):
    def __init__(self, message, errors):
        # Call the base class constructor with the parameters it needs
        super().__init__(message)

        # Now for your custom code...
        self.errors = errors
