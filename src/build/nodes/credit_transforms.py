import pandas as pd
import datetime

def flagged_decision_codes_to_standard_form(sf_table, flagged_loans):
    """
    Transforms the DataFrame of paid out loans with an erroneous decision code
    determined by the credit_loads.get_loans_with_flag_decision_codes function
    to the standard ACS table format, and appends it to the given ACS table.

    Args:
    sf_table: the standard ACS form table to append to
    flagged_loans: DataFrame with cols loan_id, loan_request_id and decision_code

    Returns:
    DataFrame

    """

    #pivot to get all loans/loan_request for each decision code together
    pivot = pd.pivot_table(flagged_loans, 
                           index = 'decision_code',
                           values = ['loan_id', 'loan_request_id'],
                           aggfunc = lambda x:list(x)
                           ).reset_index()

    df = pd.DataFrame(columns = sf_table.columns)
    df['loan_ids'] = pivot['loan_id']
    df['loan_request_ids'] = pivot['loan_request_id']
    df['type_of_error'] = 'Credit decisioning'
    df['validation_name'] = pivot['decision_code'].map(
        lambda x: f'Paid out loan with failed lending policy - {x}')
    df['threshold_hit'] = 'Any record'
    df['rag_status'] = 'Red'
    df['created_at'] = datetime.datetime.now()
    df['num_instances'] = pivot['loan_id'].map(len)
    
    return  sf_table.append(df)


def over_threshold_decision_codes_to_standard_form(sf_table, flagged_codes, dc_dict):

    """
    Transforms the DataFrame of threshold-identified decision codes to the 
    standard ACS table format, and append it to the given ACS table.

    Args:
    sf_table: the standard ACS form table to append to
    flagged_loans: DataFrame with columns decision_code, loan_id, loan_request_id,
                                          num_instances, percentage, rag_status

    Returns:
    DataFrame
    """

    df = pd.DataFrame(columns = sf_table.columns)
    df['loan_ids'] = flagged_codes['loan_id']
    df['loan_request_ids'] = flagged_codes['loan_request_id']
    df['type_of_error'] = 'Credit decisioning'
    df['validation_name'] = flagged_codes['decision_code'].map(
        lambda x: f'Paid out loan with decision code {x} threshold reached')
    df['rag_status'] = flagged_codes['rag_status']
    df['percentage'] = flagged_codes['percentage']
    df['created_at'] = datetime.datetime.now()
    df['num_instances'] = flagged_codes['num_instances']
    df['threshold_hit'] = flagged_codes['threshold_hit']

    return sf_table.append(df)

