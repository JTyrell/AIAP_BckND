import pandas as pd
import os

raw_dir = r"c:\Users\JT\Downloads\Capstone\BckND\AIAP_BckND\data\raw"

def test_aggregation():
    # Attempt to aggregate raw files into daily metrics format
    transactions = pd.read_csv(os.path.join(raw_dir, 'transactions.csv'), parse_dates=['transaction_time'])
    cash = pd.read_csv(os.path.join(raw_dir, 'cash_status.csv'), parse_dates=['timestamp'])
    maint = pd.read_csv(os.path.join(raw_dir, 'maintenance_records.csv'), parse_dates=['maintenance_date'])
    logs = pd.read_csv(os.path.join(raw_dir, 'operational_logs.csv'), parse_dates=['timestamp'])
    
    # 1. Transactions per day
    transactions['record_date'] = transactions['transaction_time'].dt.date
    tx_daily = transactions.groupby(['atm_id', 'record_date']).size().reset_index(name='transaction_count')
    
    # 2. Errors per day
    logs['record_date'] = logs['timestamp'].dt.date
    errors_daily = logs[logs['uptime_status'] == 0].groupby(['atm_id', 'record_date']).size().reset_index(name='error_count')
    
    # Downtime per day
    downtime_daily = logs.groupby(['atm_id', 'record_date'])['downtime_duration'].sum().reset_index(name='downtime_duration')
    
    # 3. Cash Status
    cash['record_date'] = cash['timestamp'].dt.date
    cash = cash.rename(columns={'remaining_cash': 'ending_cash_balance'})
    
    # Base date range per ATM
    atms = cash['atm_id'].unique()
    dfs = []
    import numpy as np
    
    for atm in atms:
        c = cash[cash['atm_id'] == atm].sort_values('record_date')
        # approx starting cash is previous ending cash
        c['starting_cash_balance'] = c['ending_cash_balance'].shift(1).fillna(c['ending_cash_balance'])  
        dfs.append(c)
        
    daily = pd.concat(dfs)
    
    # Merge all
    daily = pd.merge(daily, tx_daily, on=['atm_id', 'record_date'], how='left')
    daily = pd.merge(daily, errors_daily, on=['atm_id', 'record_date'], how='left')
    daily = pd.merge(daily, downtime_daily, on=['atm_id', 'record_date'], how='left')
    
    daily['transaction_count'] = daily['transaction_count'].fillna(0)
    daily['error_count'] = daily['error_count'].fillna(0)
    daily['downtime_duration'] = daily['downtime_duration'].fillna(0)
    daily['uptime_minutes'] = 1440 - daily['downtime_duration']
    daily['uptime_percentage'] = (daily['uptime_minutes'] / 1440) * 100
    
    # Merge Maintenance
    maint['record_date'] = maint['maintenance_date'].dt.date
    m_latest = maint.sort_values('record_date').drop_duplicates('atm_id', keep='last')
    m_latest = m_latest[['atm_id', 'record_date']].rename(columns={'record_date': 'service_date'})
    
    daily = pd.merge(daily, m_latest, on='atm_id', how='left')
    daily['service_date'] = pd.to_datetime(daily['service_date'])
    
    print(daily.head())
    print(daily.columns)
    
test_aggregation()
