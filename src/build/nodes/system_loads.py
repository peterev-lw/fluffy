import pandas as pd
import numpy as np
import datetime
from dateutil.relativedelta import relativedelta
from connectors import execute_sql_file_and_load_into_dataframe

def calculate_date_for_loan_request_load(end_cutoff, 
                                         system_checks_num_weeks_to_calculate_mean,
                                         mean_calculation_offset):
    """
    Given the config parameter, returns the date used to load the loan_request table
    in order to calculate the mean number of requests, submissions, etc.

    Args:
    end_cutoff
    system_checks_num_weeks_to_calculate_mean: int

    Returns:
    string of form YYYY-MM-DD
    """

    calculate_mean_from = datetime.datetime.strptime(end_cutoff, '%Y-%m-%d %H:%M:%S') \
                          - relativedelta(weeks=system_checks_num_weeks_to_calculate_mean) \
                          - relativedelta(days=mean_calculation_offset)

    return calculate_mean_from.strftime("%Y-%m-%d")


def system_return_loan_requests(date_to_load_loan_requests_from, end_cutoff):
    """
    Load all loan requests after a given date.

    Args: 
    date_to_load_loan_requests_from
    """

    loan_requests = execute_sql_file_and_load_into_dataframe('src/build/sql_files/system_load_loan_requests.sql',
                                                             params = {'mean_start_cutoff':date_to_load_loan_requests_from,
                                                                       'end_cutoff':end_cutoff})
    return loan_requests

def loan_request_checks(loan_request_table, num_days_offset_mean_calc, num_weeks_for_mean, start_cutoff, end_cutoff):
    """
    Checks the loan request table for unusual numbers of requests, deduped requests,
    quote offers, submissions, and paid loans. 
    Unusual activity defined by numbers going above or below a st. dev. threshold from the mean,
    defined in config.

    Args:
    loan_request_table
    num_days_offset_mean_calc:
    num_weeks_for_mean: mean values are calculated using X number of weeks data
    start_cutoff
    end_cutoff
    """
    start_cutoff = datetime.datetime.strptime(start_cutoff, '%Y-%m-%d %H:%M:%S')
    end_cutoff = datetime.datetime.strptime(end_cutoff, '%Y-%m-%d %H:%M:%S')
    mean_calc_end_cutoff = end_cutoff - relativedelta(days=num_days_offset_mean_calc)

    lr_table_for_mean_calc = loan_request_table.loc[(loan_request_table.created_at < mean_calc_end_cutoff)]
    lr_table_in_period = loan_request_table.loc[(loan_request_table.created_at > start_cutoff) &
                                                (loan_request_table.created_at < end_cutoff)]
    
    lr_deduped_for_mean_calc = lr_table_for_mean_calc.drop_duplicates()
    lr_deduped_in_period = lr_table_in_period.drop_duplicates()

    lr_quote_offered_for_mean_calc = lr_table_for_mean_calc.loc[lr_table_for_mean_calc['decision'] == 'approved']
    lr_quote_offered_in_period = lr_table_in_period.loc[lr_table_in_period['decision'] == 'approved']

    lr_submissions_for_mean_calc = lr_table_for_mean_calc.loc[lr_table_for_mean_calc['applied'] == True]
    lr_submissions_in_period = lr_table_in_period.loc[lr_table_in_period['applied'] == True]

    lr_paid_for_mean_calc = lr_table_for_mean_calc.loc[lr_table_for_mean_calc['status'] == 'paid']
    lr_paid_in_period = lr_table_in_period.loc[lr_table_in_period['status'] == 'paid']

    len_period = end_cutoff - start_cutoff 
    len_period_in_seconds = int(len_period.total_seconds())
    #last period is removed from the Series so that all counts are full counts 
    counts_over_periods_all = lr_table_for_mean_calc.groupby(pd.Grouper(key = 'created_at', freq= f"{len_period_in_seconds}s")).size()[:-1]
    counts_over_periods_deduped = lr_deduped_for_mean_calc.groupby(pd.Grouper(key = 'created_at', freq= f"{len_period_in_seconds}s")).size()[:-1]
    counts_over_periods_quote_offered = lr_quote_offered_for_mean_calc.groupby(pd.Grouper(key = 'created_at', freq= f"{len_period_in_seconds}s")).size()[:-1]
    counts_over_periods_submissions = lr_submissions_for_mean_calc.groupby(pd.Grouper(key = 'created_at', freq= f"{len_period_in_seconds}s")).size()[:-1]
    counts_over_periods_paid = lr_paid_for_mean_calc.groupby(pd.Grouper(key = 'created_at', freq= f"{len_period_in_seconds}s")).size()[:-1]

    df = pd.DataFrame({'check':['Requests',
                                'Deduped Requests',
                                'Quote Offers',
                                'Submissions',
                                'Paid Loans'
                        ],
                       'mean_value':[counts_over_periods_all.mean(),
                                     counts_over_periods_deduped.mean(),
                                     counts_over_periods_quote_offered.mean(),
                                     counts_over_periods_submissions.mean(),
                                     counts_over_periods_paid.mean()
                        ],
                       'std':[counts_over_periods_all.std(),
                             counts_over_periods_deduped.std(),
                            counts_over_periods_quote_offered.std(),
                            counts_over_periods_submissions.std(),
                            counts_over_periods_paid.std()
                        ],
                        'actual_value':[len(lr_table_in_period),
                                        len(lr_deduped_in_period),
                                        len(lr_quote_offered_in_period),
                                        len(lr_submissions_in_period),
                                        len(lr_paid_in_period)
                        ]
                       })         



    
    return df

    

    