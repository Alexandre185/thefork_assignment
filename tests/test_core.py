from monthly_report import core
import pandas as pd


"""
########################################################################################################################
Pre Processing functions
########################################################################################################################
"""


def test_convert_column_to_datetime():
    test_data = {'date': ['01/01/2021', '02/01/2021', '03-01-2021', '04/01/2021']}
    test_df = pd.DataFrame(test_data)
    ref_data = {'date': pd.date_range('2021-01-01', periods=4, freq='D')}
    ref_df = pd.DataFrame(ref_data)

    core.convert_column_to_datetime(test_df, 'date')

    assert test_df.equals(ref_df)


def test_create_month_column():
    test_data = {'date': ['01/01/2021', '02/02/2021', '03-03-2021', '04/04/2021']}
    test_df = pd.DataFrame(test_data)
    ref_data = {'month': ['2021_01', '2021_02', '2021_03', '2021_04']}
    ref_df = pd.DataFrame(ref_data)

    core.create_month_column(test_df)

    test_df.drop(columns='date', inplace=True)

    assert test_df.equals(ref_df)


def test_preprocess_amount_column():
    test_data = {'amount': ['11,95 €', '£128.35', '76 €', '29,33 €']}
    test_df = pd.DataFrame(test_data)
    ref_data = {'amount': [11.95, 128.35, 76.00, 29.33]}
    ref_df = pd.DataFrame(ref_data)

    core.preprocess_amount_column(test_df)

    assert test_df.equals(ref_df)


def test_preprocess_bookings_df():
    test_data = {'date': ['01/01/2021', '02/02/2021', '03-03-2021', '04/04/2021'],
                 'amount': ['11,95 €', '£128.35', '76 €', '29,33 €']
                 }
    test_df = pd.DataFrame(test_data)
    ref_data = {'date': ['2021-01-01', '2021-02-02', '2021-03-03', '2021-04-04'],
                'amount': [11.95, 128.35, 76.00, 29.33],
                'month': ['2021_01', '2021_02', '2021_03', '2021_04']
                }
    ref_df = pd.DataFrame(ref_data)
    ref_df['date'] = pd.to_datetime(ref_df['date'])

    core.preprocess_bookings_df(test_df)

    assert test_df.equals(ref_df)


"""
########################################################################################################################
Monthly report generation functions
########################################################################################################################
"""


def test_generate_monthly_restaurant_report_df():
    test_data = {'booking_id': ['1', '2', '3', '4'],
                 'restaurant_id': ['81b15746-2dcb-4b3b-92ac-49cf8865e26b', '47bce3e7-ff17-4d66-8aa8-44afdfbc6eac',
                                   '81b15746-2dcb-4b3b-92ac-49cf8865e26b', '47bce3e7-ff17-4d66-8aa8-44afdfbc6eac'],
                 'restaurant_name': ['Guerciotti', 'Adixen Vacuum Products', 'Guerciotti', 'Adixen Vacuum Products'],
                 'country': ['Italia', 'France', 'Italia', 'France'],
                 'month': ['2021_01', '2021_01', '2021_01', '2021_02'],
                 'amount': [11.95, 128.35, 76, 29.33],
                 'guests': [1, 6, 3, 2]
                 }
    test_df = pd.DataFrame(test_data)
    ref_data = {'restaurant_id': ['47bce3e7-ff17-4d66-8aa8-44afdfbc6eac', '47bce3e7-ff17-4d66-8aa8-44afdfbc6eac',
                                  '81b15746-2dcb-4b3b-92ac-49cf8865e26b'],
                'restaurant_name': ['Adixen Vacuum Products', 'Adixen Vacuum Products', 'Guerciotti'],
                'country': ['France', 'France', 'Italia'],
                'month': ['2021_01', '2021_02', '2021_01'],
                'number_of_bookings': [1, 1, 2],
                'number_of_guests': [6, 2, 4],
                'amount': [128.35, 29.33, 87.95]
                }
    ref_df = pd.DataFrame(ref_data)

    assert core.generate_monthly_restaurant_report_df(test_df).equals(ref_df)


"""
########################################################################################################################
Post Processing functions
########################################################################################################################
"""


def test_postprocess_amount_column():
    test_data = {'amount': [11.95, 128.35, 76.00, 29.33],
                 'country': ['España', 'France', 'Italy', 'United Kingdom']
                 }
    test_df = pd.DataFrame(test_data)
    ref_data = {'amount': ['11,95 €', '128,35 €', '76,0 €', '£29.33'],
                'country': ['España', 'France', 'Italy', 'United Kingdom']
                }
    ref_df = pd.DataFrame(ref_data)

    core.postprocess_amount_column(test_df)

    assert test_df.equals(ref_df)


def test_postprocess_monthly_restaurant_report_df():
    test_data = {'amount': [11.95, 128.35, 76.00, 29.33],
                 'country': ['España', 'France', 'Italy', 'United Kingdom']
                 }
    test_df = pd.DataFrame(test_data)
    ref_data = {'amount': ['11,95 €', '128,35 €', '76,0 €', '£29.33'],
                'country': ['España', 'France', 'Italy', 'United Kingdom']
                }
    ref_df = pd.DataFrame(ref_data)

    core.postprocess_monthly_restaurant_report_df(test_df)

    assert test_df.equals(ref_df)
