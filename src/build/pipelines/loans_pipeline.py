from faber import node
from src.build.nodes.loan_loads import *

loans_base_pipeline = [
    node(get_mismatched_payment_transactions_outstanding_balances,
         inputs = ['start_cutoff', 'end_cutoff'],
         outputs = ['mismatched_payments_transactions_oustanding_balances'],
         tags = ['get_mismatched_transactions_payments'])
]

