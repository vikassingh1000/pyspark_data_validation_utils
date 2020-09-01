
def auto_str(cls):
    def __str__(self):
        return '%s(%s)' % (type(self).__name__, ', '.join('%s=%s' % item for item in vars(self).items()))

    cls.__str__ = __str__
    return cls


class Configuration(object):

    exception_msg_seperator = "|"
    dummy_excep_msg_clm_name= "not_null_excep_msg_{}"
    empty_mean_null = True
    excep_clm_name = "exception_desc"
    excp_msg_agg = True