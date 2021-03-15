import decorators


def check_columns(df, columns):
    """
    Checks if a dataframe has exactly the columns specified in the columns list given as parameter

    :param df: dataframe which columns are checked to match the column list
    :param columns: list of the column names we want to check that constitute the dataframe
    :return: boolean, True if the dataframe is made of the list of columns False otherwise
    """
    return set(df.columns.values.tolist()) == set(columns)


def check_is_empty(df):
    """
    Checks if the dataframe contains data

    :param df: the dataframe which emptiness is checked
    :return: boolean, True if the dataframe is empty, False otherwise
    """
    return len(df.index) == 0


@decorators.timed
def check_bookings_df(df):
    """
    Checks the validity of the bookings dataframe created from the booking.csv file

    :param df: bookings dataframe
    :return: boolean, True if the bookings dataframe is valid, False otherwise
    """
    expected_columns = ['booking_id', 'restaurant_id', 'restaurant_name', 'client_id',
                        'client_name', 'amount', 'guests', 'date', 'country']

    if df is None:
        return False
    if check_is_empty(df):
        print('bookings.csv is empty')
        return False
    if not check_columns(df, expected_columns):
        print('bookings.csv does not have the proper structure')
        return False

    return True
