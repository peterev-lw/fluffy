from connectors import *
from faber import *
from src.common import environments_config
from src.build.pipelines.system_pipeline import *
from src.build.pipelines.credit_pipeline import *
from src.build.pipelines.loans_pipeline import *

def main_build(target_env='sandbox', start_cutoff='2022-04-25 09:00:00', end_cutoff='2022-04-26 10:00:00'):

    set_boto_session(profile_name=target_env, region_name='eu-west-1')
    state=environments_config[target_env].copy()

    state['start_cutoff'] = start_cutoff
    state['end_cutoff'] = end_cutoff

    state, catalog = build_catalog(state, path = 'conf/build/')

    fb=Faber(catalog)
    fb.set_state(state)
    fb.set_io(unpack_io())
    fb.create_pipeline(system_base_pipeline, 'system')
    fb.create_pipeline(credit_base_pipeline, 'credit')
    fb.create_pipeline(loans_base_pipeline, 'loans')

    fb.run()

    print(fb.state['unusual_loan_request_summary'])
    # print(fb.state['credit_sf_table'].info())
    # print(fb.state['credit_sf_table'].head(20))
    # fb.state['credit_sf_table'].to_csv('./sf_table_example.csv')
  
    return fb
