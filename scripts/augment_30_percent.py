import pandas as pd
import numpy as np
import os
import shutil
from datetime import timedelta

# Configuration
ORIG_DIR = r"c:\Users\JT\Downloads\Capstone\BckND\AIAP_BckND\training data to be extended"
RAW_DIR = r"c:\Users\JT\Downloads\Capstone\BckND\AIAP_BckND\data\raw"
BACKUP_DIR = r"c:\Users\JT\Downloads\Capstone\BckND\AIAP_BckND\data\raw_backup"
RANDOM_SEED = 42

np.random.seed(RANDOM_SEED)

def backup_raw_data():
    if not os.path.exists(BACKUP_DIR):
        os.makedirs(BACKUP_DIR)
    for f in os.listdir(RAW_DIR):
        if f.endswith('.csv') or f.endswith('.xlsx'):
            shutil.copy2(os.path.join(RAW_DIR, f), os.path.join(BACKUP_DIR, f))
    print(f"Backed up raw data to {BACKUP_DIR}")

def load_data():
    files = {
        'atm': pd.read_csv(os.path.join(ORIG_DIR, 'atm_metadata.csv')),
        'cash': pd.read_csv(os.path.join(ORIG_DIR, 'cash_status.csv')),
        'maint': pd.read_csv(os.path.join(ORIG_DIR, 'maintenance_records.csv')),
        'logs': pd.read_csv(os.path.join(ORIG_DIR, 'operational_logs.csv')),
        'trans': pd.read_csv(os.path.join(ORIG_DIR, 'transactions.csv'))
    }
    # Parse dates
    files['cash']['timestamp'] = pd.to_datetime(files['cash']['timestamp'])
    files['maint']['maintenance_date'] = pd.to_datetime(files['maint']['maintenance_date'])
    files['logs']['timestamp'] = pd.to_datetime(files['logs']['timestamp'])
    files['trans']['transaction_time'] = pd.to_datetime(files['trans']['transaction_time'])
    return files

def augment_30_percent():
    backup_raw_data()
    
    data = load_data()
    atms = data['atm']['atm_id'].unique()
    
    original_trans_count = len(data['trans'])
    target_new_trans = int(original_trans_count * 0.30)
    print(f"Original transactions: {original_trans_count}. Target new transactions (+30%): {target_new_trans}")
    
    new_cash_list = []
    new_maint_list = []
    new_logs_list = []
    new_trans_list = []
    
    # Global Max IDs
    max_cash_id = data['cash']['cash_id'].max()
    max_maint_id = data['maint']['maintenance_id'].max()
    max_log_id = data['logs']['log_id'].max()
    max_trans_id = data['trans']['transaction_id'].max()
    
    # State tracking per ATM
    atm_states = {}
    for atm_id in atms:
        atm_trans = data['trans'][data['trans']['atm_id'] == atm_id]
        atm_cash = data['cash'][data['cash']['atm_id'] == atm_id]
        if atm_trans.empty or atm_cash.empty:
            continue
            
        last_date = atm_cash['timestamp'].max()
        last_cash = atm_cash[atm_cash['timestamp'] == last_date]['remaining_cash'].values[0]
        daily_vol = len(atm_trans) / (atm_trans['transaction_time'].max() - atm_trans['transaction_time'].min()).days
        avg_withdraw = atm_trans['withdrawal_amount'].mean()
        std_withdraw = atm_trans['withdrawal_amount'].std()
        
        atm_states[atm_id] = {
            'last_date': last_date,
            'current_cash': last_cash,
            'daily_vol': daily_vol,
            'avg_withdraw': avg_withdraw,
            'std_withdraw': std_withdraw
        }
    
    generated_trans = 0
    # Day-by-day generation across all ATMs until target is reached
    d = 1
    while generated_trans < target_new_trans:
        for atm_id, state in atm_states.items():
            current_date = state['last_date'] + timedelta(days=d)
            
            # 1. Transactions
            num_trans = np.random.poisson(state['daily_vol'])
            day_withdraw_total = 0
            
            for _ in range(num_trans):
                if generated_trans >= target_new_trans:
                    break
                max_trans_id += 1
                generated_trans += 1
                
                seconds = np.random.randint(0, 86400)
                trans_time = current_date + timedelta(seconds=seconds)
                amount = max(500, int(np.random.normal(state['avg_withdraw'], state['std_withdraw'] / 2)))
                amount = (amount // 100) * 100 
                status = 1 if np.random.random() < 0.96 else 0
                
                new_trans_list.append({
                    'transaction_id': max_trans_id,
                    'atm_id': atm_id,
                    'transaction_time': trans_time,
                    'withdrawal_amount': amount,
                    'transaction_status': status
                })
                
                if status == 1:
                    day_withdraw_total += amount
            
            # 2. Maintenance
            refill_amount = 0
            if np.random.random() < 0.2: 
                max_maint_id += 1
                maint_type = 'Preventive' if np.random.random() < 0.75 else 'Corrective'
                refill_amount = int(np.random.choice(data['maint']['amount_added']))
                maint_time = current_date + timedelta(hours=np.random.randint(1, 23))
                new_maint_list.append({
                    'maintenance_id': max_maint_id,
                    'atm_id': atm_id,
                    'maintenance_date': maint_time,
                    'maintenance_type': maint_type,
                    'amount_added': refill_amount
                })
            
            # 3. Logs
            if np.random.random() < 0.15: 
                max_log_id += 1
                uptime = 0 if np.random.random() < 0.1 else 1 
                error = None
                duration = 0
                if uptime == 0:
                    error = np.random.choice(data['logs']['error_code'].dropna().unique())
                    duration = np.random.randint(10, 300)
                
                log_time = current_date + timedelta(seconds=np.random.randint(0, 86400))
                new_logs_list.append({
                    'log_id': max_log_id,
                    'atm_id': atm_id,
                    'timestamp': log_time,
                    'uptime_status': uptime,
                    'error_code': error,
                    'downtime_duration': duration
                })
            
            # 4. End of day cash
            state['current_cash'] = max(0, state['current_cash'] - day_withdraw_total + refill_amount)
            max_cash_id += 1
            new_cash_list.append({
                'cash_id': max_cash_id,
                'atm_id': atm_id,
                'timestamp': current_date,
                'remaining_cash': int(state['current_cash'])
            })
            
            if generated_trans >= target_new_trans:
                break
        d += 1

    # Combine and Save to data/raw/
    files_to_save = {
        'cash_status': (data['cash'], pd.DataFrame(new_cash_list)),
        'maintenance_records': (data['maint'], pd.DataFrame(new_maint_list)),
        'operational_logs': (data['logs'], pd.DataFrame(new_logs_list)),
        'transactions': (data['trans'], pd.DataFrame(new_trans_list))
    }
    
    data['atm'].to_csv(os.path.join(RAW_DIR, 'atm_metadata.csv'), index=False)

    for name, (orig, new) in files_to_save.items():
        if not new.empty:
            time_cols = {'cash_status': 'timestamp', 'maintenance_records': 'maintenance_date', 
                         'operational_logs': 'timestamp', 'transactions': 'transaction_time'}
            new = new.sort_values(by=['atm_id', time_cols[name]])
            combined = pd.concat([orig, new], ignore_index=True)
            
            # Convert to appropriate string format for consistency
            if name == 'cash_status':
                 combined['timestamp'] = pd.to_datetime(combined['timestamp']).dt.strftime('%Y-%m-%d')
            else:
                 combined[time_cols[name]] = pd.to_datetime(combined[time_cols[name]]).dt.strftime('%Y-%m-%d %H:%M:%S')
            
            csv_path = os.path.join(RAW_DIR, f"{name}.csv")
            combined.to_csv(csv_path, index=False)
            print(f"Saved {name} with exactly {len(new)} new rows (Total: {len(combined)}).")

if __name__ == "__main__":
    augment_30_percent()
