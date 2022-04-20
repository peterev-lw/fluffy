from faber import node
from src.build.nodes.credit_loads import *

credit_base_pipeline = [
    node(get_loans_with_flag_decision_codes,
         inputs = ['decision_codes_to_flag', 'start_cutoff', 'end_cutoff'],
         outputs = ['failed_lending_policies'],
         tags = ['get_loans_with_decision_flags'])
]