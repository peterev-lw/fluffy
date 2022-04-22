import pandas as pd

def initialise_standard_form_table():
    """
    Returns an empty dataframe, with the columns of the ACS standard form table.

    Args:
    None

    Returns:
    DataFrame
    """

    df = pd.DataFrame(
        columns = [
            'type_of_error',
            'validation_name',
            'threshold_hit',
            'rag_status',
            'num_instances',
            'created_at',
            'loan_ids',
            'loan_request_ids',
            'search_ids',
            'payment_ids',
            'transaction_ids'
        ]
    )

    return df
