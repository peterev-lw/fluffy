from faber import node
from src.build.nodes.loan_loads import *

loans_base_pipeline = [
    node(get_mismatched_payment_transactions_balances,
         inputs = ['start_cutoff', 'end_cutoff'],
         outputs = ['payment_sums'],
         tags = ['get_mismatched_transactions_payments'])
]

