from connectors import execute_sql_and_load_into_dataframe

def get_loans_with_flag_decision_codes(decision_codes_to_flag, start_cutoff, end_cutoff):
    """
    Given a list of decision codes for flagging, queries SOMIC to find 
    all paid loans that have been assigned them within the given period.

    Args:
    decision_codes_to_flag: list of strings
    
    Returns:
    DataFrame of loan_id, loan_request_id and decision_code
    """

    decision_codes_to_flag = tuple(decision_codes_to_flag)

    query = f"""
    select l.id as loan_id,
           lrdc.loan_request_id as loan_request_id,
           dc.name as decision_code
    from loans as l

    inner join loan_request_decision_codes as lrdc
    on l.loanrequest_id = lrdc.loan_request_id

    inner join decision_codes as dc
    on dc.id = lrdc.decision_code_id

    where lrdc.updated_at > '{start_cutoff}' 
    and lrdc.updated_at < '{end_cutoff}'
    and dc.name in {decision_codes_to_flag}
    """

    flagged_loans = execute_sql_and_load_into_dataframe(query)
    return flagged_loans
