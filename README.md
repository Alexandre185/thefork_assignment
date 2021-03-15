Assignment: ETL pipeline
========================

Given the attached dataset (bookings.csv), we want to generate a report with monthly statistics by restaurants.

input dataset : bookings.csv

* booking_id
* restaurant_id
* restaurant_name
* client_id
* client_name
* amount
* Guests (number of people for the given booking)
* date
* country

Expected output dataset  : monthly_restaurants_report.csv

* restaurant_id
* restaurant_name
* country
* month (in following format : YYYY_MM)
* number_of_bookings
* number_of_guests
* amount

The goal of this assignment is to implement this transformation as a proper data engineering pipeline.

Constraints : 

* The final dataset must be dumped in a postgresql table
* The postgresql will be hosted in a docker container

Languages:

 * Python (with any library/framework you want)
 * SQL


It’s simple and relatively unguided on purpose, our criterias are the following : 

* We can make it work
* The output dataset is clean
* The pipeline is cut in well-structured steps, easy to re-run independently easy to maintain and evolve
* The code is clean and well-structured (naming, functions structuration, ...) : imagine you submit this code to your colleagues for review before release
* Optional : the code is production-ready (ie. all side aspects needed to industrialize a code : unit tests, exception management, logging, ...)
* Discussion in the README.md : you can write down explanations on how to make the pipeline run arbitrations you took 
* Limitations or things you didn’t have time to implement (we know doing a fully prod-ready script may take quite some time).
* Any extras you think are relevant

========================

Proposed solution

========================

For this assignment, I worked with Python 3.8.8 (and Docker Desktop 3.2.1 with the Postgres image for the database part).

I used the following packages: pandas, sqlalchemy (with psycopg2), pytest, numpy, time, logging, functools, csv, io

The project is strctured as follow: projet main folder (thefork_assignment) has 2 directories: monthly_report and tests.

monthly_report contains:
 - __init__.py, empty file
 - main.py, the main file with the data pipeline to be executed
 - core.py, core functions that acts on the loaded data to make the monthly report
 - io_data.py, functions to load data from the csv, to save data to a csv, and to send data to the postgresSQL table
 - inputs.py, functions that handle the inputs asked to the user when the code is executed
 - checks.py, functions that perform validity checks on the data to verify it is as expected/ not corrupted
 - decorators.py, contains the decorators (actually only one used to log the execution time of a function to the 'monthly_report.log' file in the directory)
 
tests contains:
 - __init__.py, empty file
 - run_tests.py, file that runs all the tests when executed
 - test_core.py, unit test functions for all the core functions

When executing the main.py file, the following will be asked to the user:
 - the path to the 'bookings.csv' file, must be full path including the bookings.csv part
 - the username of the postgresSQL instance where to send the data to
 - the username of the postgresSQL instance where to send the data to
 - the postgresSQL database where to send the data to
 - the host of the postgresSQL instance where to send the data to (if left blank will default to localhost)
 - the port on which access the postgresSQL instance (if left blank will default to 5432)
 - if the user wants to save locally the montly report as a csv file, if yes the user will be asked the full path where to save it

By default, the data will be sent to a table named 'monthly_reports_restaurant' in the specified database. If the table doesn't exist it will be created, if it exists the data will be append to it. This behavior can be changed by modifying the parameters if_exists from 'append' to 'replace' if we want to recreate the table if it exists. I assumed the append option because if it is an operation we want to run every month and add the result to the monthly report table on the postgres database.
To change the table name, it will have to be passed as a paramater to the function 'io_data.send_df_to_postgres_db' called in the main function in the main.py file.

Regarding the actual data pipeline to generate the monthly report:

The generation of the monthly report from the bookings data is mainly a group by on the restaurant id and the month with 3 aggregation: a count on booking id, and sums on guests and amout.
So I broke down the pipeline in the following steps:
 - data loading from the bookings.csv file to a pandas dataframe
 - preprocessing needed on the dataframe before doing the groupby
 - the actual groupby operation
 - the post processing needed on the generated report to get the output in the required format
 - the transmission to the data: to the postgresSQL table and eventually to be saved as a csv file 

For the preprocessing part, only 2 columns needed operations:
 - the 'date' column in order to generate the 'month' column, I first converted the 'date' column to datetime and kept only the years and month information to    create the 'month' column
 - the 'amount' column to get rid of the currency signs to convert it to float for sum during the aggregation (and changing the , to . in case of €)

The group by part, I wanted to do the equivalent of:
#
(SELECT count(bookings_id) AS number_of_bookings, sum(guests) AS number_of_guests, sum(amount) AS amount
FROM bookings
GROUP BY restaurant_id, restaurant_name, country, month) AS monthly_restaurants_reports
#
At first, I used the groupby pandas function with the apply function with a custom aggregation function that I had defined. However using the groupby function with the aggregate function and a dictionnary of the aggregation I wanted to apply made the operation go from over 4s to less than 0.05s.

For the postprocessing, I only had to process back the amount column to get back the currency signs into it and turning back its type to string.

Possible ameliorations:
 - more checks:
   * more often, such as the ones realized on the loaded dataframe, but at more steps (to check we are not corrupting the data along the way)
   * more extensive, I only checked the structure and the presence of data, but a check sould be implemented to detect empty values and then to decide which behavior to have in this situation (ex: a record doesn't have the number of           guests, do we discard it or keep the rest of its information, etc)
 - more logging, I only logged the execution time of the high level functions and the main one to have an idea of the time bottlenecks and what to focuse on to optimize. However a lot of other things should be logged such as the errors    and exception encountered or the number of records processed and metrics such as CPU and RAM.
 - code optimization:
   * the part that takes the more time is sending the data to the postgresSQL table (over half of the total execution time). I already improved the speed by setting the parameter method to 'multi' that allows to pass multiple values in        a single INSERT clause. And I improved it further by passing 'psql_insert_copy' as method which is defined in the pandas documentation. To be improved further either parallelization with multiples concurrent inserts or I need to          learn more about SQLalchemy and how to optimize it.
   * I could use parallelization to make it run faster, for the different tasks on the different columns in the pre and post processing. And maybe also by sharding the data (by country for instance) and running the operations on each          shard in parallel (given that the actual sharding does not take more time than the parallelization saves)
 - more tests: I only did the unit tests for the core functions, all the functions should be tested. A functionnal test should also be implemented on the whole pipeline. 
 - memory optimization: if we want to optimize memory space during the data processing, we can write the report dataframe on the previous bookings dataframe to not store the 2 of them during the code execution. 

Things I would do differently:
 - instead of converting the 'date' column to datetime type when preprocessing, I could directly get only the year and month part by selecting only the end of the string.
 - instead of going back to the amount column with a string type with the currency signs, I would keep the amount column as a float to make any further operation easier, and either use the country column to get the currency information      or create a currency column with only the currency sign 

Additional comments:
I initially went with the definition of a class but as it had only one attribute that was actually the dataframe I felt it did not have much benefit so I chose to do the full assignment with functions.
Disclaimer:
 - I found the 'safe_input' function online, I just adapted it to include a default value
 - I found the 'timed' decorator online
 - The 'psql_insert_copy' function is from the Pandas documentation
