
from pyspark.sql.functions import isnan, when, count, col,lit,concat,round, monotonically_increasing_id

def get_number_of_nulls_in_df (df):

    """This function is returning a dataframe of percentage of null
        values (%) for each column 
        pram: input df
        return: df of resulting % nulls for each col 
    """
    
    n_records = df.count()
    df_res  = df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df.columns])
    df_res = df_res.select(*(col(c).cast("integer").alias(c) for c in df_res.columns))

    ##  divide by total number of records 
    dividing_cols = [x for x in df_res.columns]

    for column in dividing_cols:
        df_res = df_res.withColumn(column, col(column) / lit(n_records))
        df_res = df_res.withColumn(column, round(col(column) * lit(100),2))
        df_res = df_res.withColumn(column ,concat(col(column), lit(" %")))
    
    return df_res


def drop_columns_from_df(df,cols=[]):

    """This function is returning a new dataframe after dropping colmmns
        pram: input df
        pram: cols list
        return: new df 
    """
    df = df.drop(*cols)
    return df


def check_duplicate_of_column(df,col_name):
    """ this function is returning a new dataframe after dropping duplicates base on colmmn
        pram: input df
        pram: input column name
        return: new df 
        return: flag of had duplicates or not 
        return: number of dropped columns
        return: total number of records before drop
        return: total number of records after drop

    
    """
    had_duplicates_flag = bool
    counts_before_drop = df.count()
    df = df.dropDuplicates([col_name])
    counts_after_drop = df.count()
    if counts_after_drop < counts_before_drop :
        had_duplicates_flag = True
    else:
        had_duplicates_flag = False
    num_of_dropped_cols = counts_before_drop - counts_after_drop
    return df,had_duplicates_flag,num_of_dropped_cols,counts_before_drop,counts_after_drop


def add_surrogate_key_to_df(df,col_name):
    """ this function is returning a new dataframe with surrogate key with new column
        pram: input df
        pram: surrgate key col name
        return: new df with key
    """

    df_with_surr_key = df.withColumn(col_name, monotonically_increasing_id())
    cols = df_with_surr_key.columns
    cols_new_for_select = cols[-1:] + cols[:-1]
    df_with_surr_key = df_with_surr_key.select(cols_new_for_select)

    return df_with_surr_key

