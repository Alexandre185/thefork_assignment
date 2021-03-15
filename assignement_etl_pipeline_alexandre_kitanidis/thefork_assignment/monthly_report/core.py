import pandas as pd
from monthly_report import decorators

"""
########################################################################################################################
Pre Processing functions
########################################################################################################################
"""


def convert_column_to_datetime(df, column):
    """
    Converts a given column of a dataframe to a column with datetime type

    :param df: the dataframe in which converting the column type
    :param column: name of the column which type is to be converted

    """
    df[column] = pd.to_datetime(df[column], dayfirst=True)


def create_month_column(df):
    """
    Creates the 'month' column from the date 'column' in the provided data in 3 steps:
        - converts the 'date' column with the 'convert_column_to_datetime' function
        - creates the 'month' column by keeping only the month and year from the full date
        - changes the 'month' column format from '%Y-%m' to '%Y_%m'

    :param df: the dataframe in which creating the month column

    """
    convert_column_to_datetime(df, 'date')

    df['month'] = df['date'].dt.to_period('M')
    df['month'] = df['month'].dt.strftime('%Y_%m')


def preprocess_amount_column(df):
    """
    Applies the following changes to the 'amount' column:
    - the ',' are replaced by '.'
    - all the non numeric characters are removed and the data type is converted to float

    :param df: the dataframe with the 'amount' column to modify
    """
    df['amount'] = df['amount'].replace(',', '.', regex=True)
    df['amount'] = df['amount'].replace('[^.0-9]', '', regex=True)
    df['amount'] = df['amount'].astype(float)


@decorators.timed
def preprocess_bookings_df(df):
    """
    Creates a 'month' column with the 'create_month_column' function
    Preprocesses the 'amount' column with the 'preprocess_amount_column' function.

    :param df: the dataframe on which the preprocessing operations are realized
    """
    create_month_column(df)
    preprocess_amount_column(df)


"""
########################################################################################################################
Monthly report generation functions
########################################################################################################################
"""


@decorators.timed
def generate_monthly_restaurant_report_df(df):
    """
    Generates the monthly restaurant report by applying a group by on the columns:
    'restaurant_id', 'restaurant_name', 'country' and 'month'.
    Uses the aggregations dictionary to generate the aggregated columns:
    'number_of_bookings', 'number_of_guests' and 'amount'.

    :param df: the preprocessed dataframe with the bookings data
    :return: the generated monthly report as a dataframe
    """
    group = ['restaurant_id',
             'restaurant_name',
             'country',
             'month'
             ]
    aggregations = {'booking_id': 'count',
                    'guests': 'sum',
                    'amount': 'sum'
                    }
    df = df.groupby(group, as_index=False).agg(aggregations)
    df.rename(columns={'booking_id': 'number_of_bookings', 'guests': 'number_of_guests'}, inplace=True)

    return df


"""
########################################################################################################################
Post Processing functions
########################################################################################################################
"""


def postprocess_amount_column(df):
    """
    Converts the 'amount' column to a string type.
    Uses the 'country' column to add the currency signs to the 'amount'.

    :param df: dataframe on which modifying the 'amount' column
    """
    df['amount'] = df['amount'].round(2).astype(str)
    df.loc[df['country'] != 'United Kingdom', 'amount'] = df['amount'].replace(r'\.', ',', regex=True) + ' €'
    df.loc[df['country'] == 'United Kingdom', 'amount'] = '£' + df['amount']


@decorators.timed
def postprocess_monthly_restaurant_report_df(df):
    """
    Applies the changes defined in the 'postprocess_amount_column' to the 'amount' column.

    :param df: the dataframe on which to perform the modifications
    """
    postprocess_amount_column(df)
