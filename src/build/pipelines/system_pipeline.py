from faber import node
from src.build.nodes.system_loads import *

system_base_pipeline = [
    node(calculate_date_for_loan_request_load,
         inputs = ['end_cutoff', 
                   'system_checks_num_weeks_to_calculate_mean',
                   'system_checks_num_days_to_offset_mean_calc'],
         outputs = ['date_to_load_loan_requests_from'],
         tags = ['get_loan_request_load_date']
    ),
    node(system_return_loan_requests,
         inputs = ['date_to_load_loan_requests_from', 'end_cutoff'],
         outputs = ['loan_request_table'],
         tags = ['get_all_loan_requests']
    ),
    node(loan_request_checks,
         inputs = ['loan_request_table',
                   'system_checks_num_weeks_to_calculate_mean',
                   'system_checks_num_days_to_offset_mean_calc',
                   'system_num_standard_deviations_from_mean_to_flag',
                   'start_cutoff', 
                   'end_cutoff'],
         outputs = ['unusual_loan_request_summary'],
         tags = ['check_for_unusual_lr_numbers'])
]