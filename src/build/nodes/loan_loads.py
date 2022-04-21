from connectors import execute_sql_and_load_into_dataframe

def get_mismatched_payment_transactions_balances(start_cutoff, end_cutoff):
    #settled = True for payments received

    #transactions, sum by loan id

    query = f"""
    select p.loan_id, p.payments_sum, t.transactions_sum
    from (
        select loan_id, sum(value) as payments_sum
        from payments_rev p

        where settled = TRUE
        and updated_at > '{start_cutoff}' 
        and updated_at < '{end_cutoff}'

        group by loan_id
    ) as p

    inner join (
        select loan_id, sum(amount) as transactions_sum
        from transactions

        where type_id in (7)
        and updated_at > '{start_cutoff}' 
        and updated_at < '{end_cutoff}'

        group by loan_id
    ) as t
    on p.loan_id = t.loan_id
    """

    df = execute_sql_and_load_into_dataframe(query)
    df['tables_equal'] = df['payments_sum'].eq(df['transactions_sum'])

    return df