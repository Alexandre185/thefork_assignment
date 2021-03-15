def safe_input(prompt, type_=None, min_=None, max_=None, range_=None, default=None):
    """
    Checks that the inputs manually typed are of the correct type and in the correct range.
    Asks for new input to the user until it gets an acceptable input.

    :param prompt: text displayed to the user
    :param type_: type of the input
    :param min_: minimum value of the input
    :param max_: maximum value of the input
    :param range_: range in which the input must be
    :param default: default value if no input is provided
    :return: the input typed by the user
    """
    if min_ is not None and max_ is not None and max_ < min_:
        raise ValueError("min_ must be less than or equal to max_.")
    while True:
        ui = input(prompt)
        if ui == "":
            if default is not None:
                return default
            continue
        if type_ is not None:
            try:
                ui = type_(ui)
            except ValueError:
                print("Input type must be {0}.".format(type_.__name__))
                continue
        if max_ is not None and ui > max_:
            print("Input must be less than or equal to {0}.".format(max_))
        elif min_ is not None and ui < min_:
            print("Input must be greater than or equal to {0}.".format(min_))
        elif range_ is not None and ui not in range_:
            if isinstance(range_, range):
                template = "Input must be between {0.start} and {0.stop}."
                print(template.format(range_))
            else:
                template = "Input must be {0}."
                if len(range_) == 1:
                    print(template.format(*range_))
                else:
                    expected = " or ".join((
                        ", ".join(str(x) for x in range_[:-1]),
                        str(range_[-1])
                    ))
                    print(template.format(expected))
        else:
            return ui


def get_inputs():
    """
    Creates a dictionary of all the inputs asked to the user necessary to the main script.

    :return: dictionary containing all the user inputs
    """
    inputs_dict = {
        'path_to_bookings_csv': safe_input('Path to the bookings.csv file:', type_=str),
        'postgres_username': safe_input('postgres username:', type_=str),
        'postgres_password': safe_input('postgres password:', type_=str),
        'postgres_database': safe_input('postgres database:', type_=str),
        'postgres_host': safe_input('postgres host:', default='localhost'),
        'postgres_port': safe_input('postgres port:', type_=int, range_=range(1024, 49152), default=5432)
    }

    if safe_input('Do you wish to save the report as a csv file? [y/n] ', type_=str, range_=['y', 'n']) == 'y':
        inputs_dict['path_to_save_monthly_restaurants_report_csv'] = safe_input('Path where to save the '
                                                                                'monthly_restaurants_report.csv '
                                                                                'file:', type_=str)

    return inputs_dict
