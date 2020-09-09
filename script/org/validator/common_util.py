from pyspark.sql import functions as sf

def is_clm_null(clm,confg):

    if  isinstance(clm, str):
        clm = sf.col(clm)

    if self.confg.empty_mean_null:

        return clm.isNull() | ( clm==sf.lit(""))

    return clm.isNull()

def auto_str(cls):
    def __str__(self):
        return '%s(%s)' % (type(self).__name__, ', '.join('%s=%s' % item for item in vars(self).items()))

    cls.__str__ = __str__
    return cls
