import pandas as pd

def return_raw(df):
#   df['yearweek'] = df['lr_created_at'].dt.year * 100 + df['lr_created_at'].dt.isocalendar().week
    return df