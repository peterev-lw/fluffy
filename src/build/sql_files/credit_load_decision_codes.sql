    select l.id as loan_id,
           lrdc.loan_request_id as loan_request_id,
           dc.name as decision_code
    from loans as l

    inner join loan_request_decision_codes as lrdc
    on l.loanrequest_id = lrdc.loan_request_id

    inner join decision_codes as dc
    on dc.id = lrdc.decision_code_id

    where lrdc.updated_at > %(start_cutoff)s 
    and lrdc.updated_at < %(end_cutoff)s;