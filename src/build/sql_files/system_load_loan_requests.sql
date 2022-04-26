select id, 
       decision,
       applied,
       status,
       updated_at,
       created_at
from loan_requests
where created_at > %(mean_start_cutoff)s
and created_at < %(end_cutoff)s;
