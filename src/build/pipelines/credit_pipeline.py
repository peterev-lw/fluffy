from faber import node
from src.build.nodes.credit_loads import *
from src.build.nodes.credit_transforms import *
from src.build.nodes.standard_form import initialise_standard_form_table

credit_base_pipeline = [
    node(initialise_standard_form_table,
         inputs = [],
         outputs = ['init_credit_sf_table'],
         tags = ['init_credit_sf_table']
    ),
    # node(get_loans_with_flag_decision_codes,
    #      inputs = ['decision_codes_to_flag', 'start_cutoff', 'end_cutoff'],
    #      outputs = ['flagged_decision_code_loans'],
    #      tags = ['get_loans_with_decision_flags']
    # ),

    # node(get_loans_with_flag_decision_codes_over_threshold,
    #      inputs = ['flag_decision_code_threshold_dictionary', 'start_cutoff', 'end_cutoff'],
    #      outputs = ['threshold_flagged_decision_codes'],
    #      tags = ['get_over_threshold_decision_codes']
    # ),
    # node(over_threshold_decision_codes_to_standard_form,
    #      inputs = ['credit_sf_table', 'threshold_flagged_decision_codes', 'flag_decision_code_threshold_dictionary'],
    #      outputs = ['credit_sf_table'],
    #      tags = ['over_threshold_decision_codes_to_sf']
    # )

    node(get_loans_with_flag_decision_codes,
         inputs = ['decision_codes_to_flag_dict', 'start_cutoff', 'end_cutoff'],
         outputs = ['loans_with_flagged_decision_codes'],
         tags = ['get_loans_with_flag_decision_codes']
    ),
    node(flagged_decision_codes_to_standard_form,
         inputs = ['init_credit_sf_table', 'loans_with_flagged_decision_codes'],
         outputs = ['credit_sf_table'],
         tags = ['transform_loans_with_flagged_decision']
    ),
]