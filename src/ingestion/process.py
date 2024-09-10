from pyspark.sql.functions import monotonically_increasing_id, row_number
from pyspark.sql.window import Window

def remove_specific_rows(df, start_row, end_row):
    """
    Remove specific rows from a DataFrame based on row numbers.
    
    :param df: Input DataFrame
    :param start_row: Start of the row range to remove (inclusive)
    :param end_row: End of the row range to remove (inclusive)
    :return: DataFrame with specified rows removed
    """
    # Add row numbers
    df_with_row_num = df.withColumn("row_num", row_number().over(Window.orderBy(monotonically_increasing_id())))
    
    # Filter out specified rows
    df_cleaned = df_with_row_num.filter((df_with_row_num.row_num < start_row) | (df_with_row_num.row_num > end_row)).drop("row_num")
    
    return df_cleaned


