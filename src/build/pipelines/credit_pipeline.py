from faber import node
from src.build.nodes.credit_loads import *
from src.build.nodes.credit_transforms import *
from src.build.nodes.common.standard_form import initialise_standard_form_table
from src.build.nodes.common.return_raw import return_raw

credit_base_pipeline = [
    node(initialise_standard_form_table,
         inputs = [],
         outputs = ['init_credit_sf_table'],
         tags = ['init_credit_sf_table']
    ),
    node(return_raw,
         inputs = ['decision_codes_table_catalog'],
         outputs = ['decision_codes_table'],
         tags = ['load_full_decision_code_table']
    ),
    node(get_loans_with_flag_decision_codes,
         inputs = ['decision_codes_table', 'decision_codes_to_flag_dict'],
         outputs = ['loans_with_flagged_decision_codes'],
         tags = ['get_loans_with_flag_decision_codes']
    ),
    node(flagged_decision_codes_to_standard_form,
         inputs = ['init_credit_sf_table', 'loans_with_flagged_decision_codes'],
         outputs = ['credit_sf_table'],
         tags = ['transform_loans_with_flagged_decision']
    ),
]