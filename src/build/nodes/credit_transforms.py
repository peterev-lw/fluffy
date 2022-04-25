import pandas as pd
import datetime

def flagged_decision_codes_to_standard_form(sf_table, flagged_loans):
    """
    Transforms the DataFrame of paid out loans with an erroneous decision code
    determined by the credit_loads.get_loans_with_flag_decision_codes function
    to the standard ACS table format, and appends it to the given ACS table.

    Args:
    sf_table: the standard ACS form table to append to
    flagged_loans: DataFrame with cols decision_code, loan_id, loan_request_id, 
                                       num_instances, percentage, threshold_hit,
                                       rag_status

    Returns:
    DataFrame

    """

    df = pd.DataFrame(columns = sf_table.columns)
    df['loan_ids'] = flagged_loans['loan_id']
    df['loan_request_ids'] = flagged_loans['loan_request_id']
    df['type_of_error'] = 'Credit decisioning'
    df['validation_name'] = flagged_loans['decision_code'].map(
                            lambda x: f'Paid out loan with failed lending policy - {x}'
                            )
    df['threshold_hit'] = flagged_loans['threshold_hit']
    df['rag_status'] = flagged_loans['rag_status']
    df['created_at'] = datetime.datetime.now()
    df['num_instances'] = flagged_loans['num_instances']
    df['percentage'] = flagged_loans['percentage']
    
    return sf_table.append(df)


