from connectors import execute_sql_and_load_into_dataframe

def get_loans_with_flag_decision_codes(decision_codes_to_flag, start_cutoff, end_cutoff):
    """
    Given a list of decision codes for flagging, queries SOMIC to find 
    all paid loans that have been assigned them within the given period.

    Args:
    decision_codes_to_flag: list of strings
    
    Returns:
    DataFrame of loan_ids
    """

    decision_codes_to_flag = tuple(decision_codes_to_flag)
    query = f"""
    select id from loans
    where loanrequest_id in 
    (
        select loan_request_id from loan_request_decision_codes
        where updated_at > '{start_cutoff}' and updated_at < '{end_cutoff}'
        and decision_code_id in 
        (
            select id from decision_codes
            where name in {decision_codes_to_flag}
        )
    )
    """



    flagged_loans = execute_sql_and_load_into_dataframe(query)

    return flagged_loans
