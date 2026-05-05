"""
AIAP ML Engine — Data Provider Interface
"""
import os
import pandas as pd
from typing import Optional
from config import Config

# Global cache for data provider results
_data_cache = {}

def clear_provider_cache():
    """Invalidate the global data provider cache."""
    global _data_cache
    _data_cache = {}

class DataProvider:
    """Abstract interface for data ingestion routines."""
    def get_data(self, days: Optional[int] = None) -> pd.DataFrame:
        raise NotImplementedError("Subclasses must implement get_data()")

class FileDataProvider(DataProvider):
    """
    Ingests standard raw files (transactions, operational_logs, cash_status, maintenance_records)
    from data/raw/ and aggregates them into the daily metric format expected by feature_engineering.py.
    """
    def __init__(self, raw_dir: str = Config.RAW_DATA_DIR):
        self.raw_dir = raw_dir

    def get_data(self, days: Optional[int] = None) -> pd.DataFrame:
        cache_key = (self.raw_dir, days)
        if cache_key in _data_cache:
            return _data_cache[cache_key].copy()

        trans_path = os.path.join(self.raw_dir, 'transactions.csv')
        logs_path = os.path.join(self.raw_dir, 'operational_logs.csv')
        cash_path = os.path.join(self.raw_dir, 'cash_status.csv')
        maint_path = os.path.join(self.raw_dir, 'maintenance_records.csv')

        # Load raw data
        transactions = pd.read_csv(trans_path, parse_dates=['transaction_time'])
        logs = pd.read_csv(logs_path, parse_dates=['timestamp'])
        cash = pd.read_csv(cash_path, parse_dates=['timestamp'])
        maint = pd.read_csv(maint_path, parse_dates=['maintenance_date'])

        # 1. Transactions per day
        transactions['record_date'] = transactions['transaction_time'].dt.normalize()
        
        if 'transaction_type' in transactions.columns:
            w_mask = transactions['transaction_type'].str.lower().isin(['withdrawal', 'w'])
            d_mask = transactions['transaction_type'].str.lower().isin(['deposit', 'd'])
            amt_col = 'amount' if 'amount' in transactions.columns else 'withdrawal_amount'

            tx_w = transactions[w_mask]
            tx_d = transactions[d_mask]

            w_daily = tx_w.groupby(['atm_id', 'record_date']).agg(
                transaction_count=('atm_id', 'size'),
                daily_withdrawal_amount=(amt_col, 'sum'),
                avg_transaction_amount=(amt_col, 'mean')
            ).reset_index()

            d_daily = tx_d.groupby(['atm_id', 'record_date']).agg(
                daily_deposit_count=('atm_id', 'size'),
                daily_deposit_amount=(amt_col, 'sum')
            ).reset_index()

            tx_daily = pd.merge(w_daily, d_daily, on=['atm_id', 'record_date'], how='outer').fillna(0)
        else:
            tx_daily = transactions.groupby(['atm_id', 'record_date']).agg(
                transaction_count=('atm_id', 'size'),
                daily_withdrawal_amount=('withdrawal_amount', 'sum'),
                avg_transaction_amount=('withdrawal_amount', 'mean')
            ).reset_index()
            tx_daily['daily_deposit_count'] = 0
            tx_daily['daily_deposit_amount'] = 0.0

        # Output explicit float/int types
        tx_daily['transaction_count'] = tx_daily['transaction_count'].astype(int)
        tx_daily['daily_deposit_count'] = tx_daily['daily_deposit_count'].astype(int)

        # 2. Errors & Uptime per day
        logs['record_date'] = logs['timestamp'].dt.normalize()
        # Count only rows where uptime_status is 0 (failure) as an error
        errors_daily = logs[logs['uptime_status'] == 0].groupby(['atm_id', 'record_date']).size().reset_index(name='error_count')
        downtime_daily = logs.groupby(['atm_id', 'record_date'])['downtime_duration'].sum().reset_index(name='downtime_duration')

        # 3. Cash Status (Starting/Ending array)
        cash['record_date'] = cash['timestamp'].dt.normalize()
        cash = cash.rename(columns={'remaining_cash': 'ending_cash_balance'}).drop(columns=['cash_id', 'timestamp'])
        
        atms = cash['atm_id'].unique()
        dfs = []
        for atm in atms:
            c = cash[cash['atm_id'] == atm].sort_values('record_date').copy()
            # Approximation: starting cash is yesterday's ending cash
            c['starting_cash_balance'] = c['ending_cash_balance'].shift(1).fillna(c['ending_cash_balance'])
            dfs.append(c)
        cash_daily = pd.concat(dfs) if dfs else pd.DataFrame()

        # Merge base table
        daily = cash_daily.copy()
        if not daily.empty:
            daily = pd.merge(daily, tx_daily, on=['atm_id', 'record_date'], how='left')
            daily = pd.merge(daily, errors_daily, on=['atm_id', 'record_date'], how='left')
            daily = pd.merge(daily, downtime_daily, on=['atm_id', 'record_date'], how='left')

            daily['transaction_count'] = daily['transaction_count'].fillna(0).astype(int)
            daily['daily_deposit_count'] = daily['daily_deposit_count'].fillna(0).astype(int)
            daily['daily_withdrawal_amount'] = daily['daily_withdrawal_amount'].fillna(0)
            daily['daily_deposit_amount'] = daily['daily_deposit_amount'].fillna(0)
            daily['avg_transaction_amount'] = daily['avg_transaction_amount'].fillna(0)
            daily['error_count'] = daily['error_count'].fillna(0).astype(int)
            daily['downtime_duration'] = daily['downtime_duration'].fillna(0).astype(int)
            
            # Uptime calculation
            daily['uptime_minutes'] = 1440 - daily['downtime_duration'].clip(0, 1440)
            daily['uptime_percentage'] = (daily['uptime_minutes'] / 1440) * 100

            # 4. Integrate service_date from maintenance logs
            maint['record_date'] = maint['maintenance_date'].dt.normalize()
            m_latest = maint.sort_values('record_date').drop_duplicates('atm_id', keep='last')
            m_latest = m_latest[['atm_id', 'record_date']].rename(columns={'record_date': 'service_date'})
            daily = pd.merge(daily, m_latest, on='atm_id', how='left')
            
            # Apply 'days' filter
            if days is not None:
                max_date = daily['record_date'].max()
                cutoff = max_date - pd.Timedelta(days=days)
                daily = daily[daily['record_date'] >= cutoff]
        
        daily = daily.reset_index(drop=True)
        _data_cache[cache_key] = daily
        return daily.copy()


class DPDataProvider(DataProvider):
    """
    Placeholder stub for future Data Provider connection logic.
    Replace implementation with actual DP integration.
    """
    def __init__(self, endpoint: str = ""):
        self.endpoint = endpoint

    def get_data(self, days: Optional[int] = None) -> pd.DataFrame:
        print("DPDataProvider: Fetching data from External DP...")
        # TODO: Implement connection to DP
        # return external_api.fetch_daily_metrics()
        
        # Fallback to local files for now to avoid breaking the pipeline testing
        file_provider = FileDataProvider()
        return file_provider.get_data(days=days)

# Factory helper
def get_data_provider() -> DataProvider:
    source = getattr(Config, 'DATA_SOURCE', 'file').lower()
    if source == 'dp':
        return DPDataProvider()
    else:
        return FileDataProvider()
