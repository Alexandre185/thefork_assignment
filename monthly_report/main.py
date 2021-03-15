import inputs
import io_data
import checks
import core
import decorators
import logging


@decorators.timed
def main():
    """
    Main function that implements the data processing pipeline:

    1) loads the csv file from its path as a dataframe
    2) pre process the dataframe (month column creation and amount column preparation)
    3) creates the monthly report with a group by and a custom aggregation for the different columns
    4) Post process the monthly report (amount column and type of aggregated columns)
    5) Sends the dataframe to a postgresSQL table (creates the table if it doesn't exist, appends the data otherwise)
    6) [Optional] Saves the monthly report to a csv on the specified path
    """

    logging.basicConfig(filename='monthly_report.log', format='%(asctime)s [%(levelname)s] %(message)s',
                        level=logging.DEBUG)

    params = inputs.get_inputs()

    bookings_df = io_data.load_csv_to_df(params['path_to_bookings_csv'])
    if checks.check_bookings_df(bookings_df) is False:
        return

    core.preprocess_bookings_df(bookings_df)

    monthly_restaurant_report_df = core.generate_monthly_restaurant_report_df(bookings_df)

    core.postprocess_monthly_restaurant_report_df(monthly_restaurant_report_df)

    io_data.send_df_to_postgres_db(monthly_restaurant_report_df, params['postgres_username'],
                                   params['postgres_password'], params['postgres_database'],
                                   params['postgres_host'], params['postgres_port'])

    if 'path_to_save_monthly_restaurants_report_csv' in params:
        io_data.save_df_as_csv(monthly_restaurant_report_df,
                               params['path_to_save_monthly_restaurants_report_csv'])


if __name__ == '__main__':
    main()
