def compare_df(df1,df2):
    return  sorted(df1.collect())== sorted(df2.collect())