from faber import node
from src.build.nodes.credit_loads import *
from src.build.nodes.credit_transforms import *
from src.build.nodes.aggregation import initialise_standard_form_table

credit_base_pipeline = [
    node(initialise_standard_form_table,
         inputs = [],
         outputs = ['init_credit_sf_table'],
         tags = ['init_credit_sf_table']
    ),
    node(get_loans_with_flag_decision_codes,
         inputs = ['decision_codes_to_flag', 'start_cutoff', 'end_cutoff'],
         outputs = ['flagged_decision_code_loans'],
         tags = ['get_loans_with_decision_flags']
    ),
    node(flagged_decision_codes_to_standard_form,
         inputs = ['init_credit_sf_table', 'flagged_decision_code_loans'],
         outputs = ['credit_sf_table'],
         tags = ['transform_loans_with_flagged_decision']
    )
]