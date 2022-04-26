import pandas as pd

def get_loans_with_flag_decision_codes(all_loans, dc_dict):
    """
    Given a dictionary of decision codes, along with their respective thresholds, log an error if said
    thresholds have been crossed, and if so identify which
    paid loans have contributed to this error.

    Args:
    dc_dict: dictionary with decision codes as the keys, and the values either 'Any' for those codes that
             are to be flagged for any occurrence on a paid loan, or a length-2 list of the green-amber and amber- 
             red boundaries for those codes that have an allowed threshold.
    start_cutoff
    end_cutoff

    Returns:
    DataFrame of loan_id, loan_request_id and decision_code
    """

    total_number = len(all_loans)

    decision_codes_to_flag = list(dc_dict.keys())
    flagged_loans = all_loans.loc[all_loans.decision_code.isin(decision_codes_to_flag)]
    df = pd.pivot_table(flagged_loans, 
                        index = 'decision_code',
                        values = ['loan_id', 'loan_request_id'],
                        aggfunc = lambda x:list(x)
                        ).reset_index()

    df['num_instances'] = df['loan_id'].map(len)
    df['percentage'] = df['num_instances'] / total_number * 100
    df['threshold_hit'] = df['decision_code'].map(dc_dict.get)
    df['rag_status'] = df.apply(lambda x: 'Red' if x['threshold_hit'] == 'Any' else 
                                           ('Green' if x['percentage'] <= dc_dict[x['decision_code']][0] else
                                           ('Amber' if x['percentage'] <= dc_dict[x['decision_code']][1] else 'Red'
                                           )), axis = 1)
    df['threshold_hit'] = df.apply(lambda x: 'Any record' if x['threshold_hit'] == 'Any' else
                                            ('None' if x['rag_status'] == 'Green' else
                                            (f"{dc_dict[x['decision_code']][0]}-{dc_dict[x['decision_code']][1]}%" if x['rag_status'] == 'Amber'
                                             else f"> {dc_dict[x['decision_code']][1]}%"
                                          )), axis = 1)

    return df
                                           