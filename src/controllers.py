from connectors import *
from faber import *
from src.common import environments_config
from src.build.pipelines.credit_pipeline import *

def main_build(target_env='sandbox', start_cutoff='2021-01-01', end_cutoff='2022-01-01'):

    set_boto_session(profile_name=target_env, region_name='eu-west-1')
    state=environments_config[target_env].copy()

    state['start_cutoff'] = start_cutoff
    state['end_cutoff'] = end_cutoff

    state, catalog = build_catalog(state, path = 'conf/credit/')

    fb=Faber(catalog)
    fb.set_state(state)
    fb.set_io(unpack_io())
    fb.create_pipeline(credit_base_pipeline, 'credit')

    fb.run()

    print(fb.state['loan_ids'].head(5))
    print(fb.state['loan_ids'].info())


    return fb
