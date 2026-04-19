import pandas as pd
import numpy as np
import os
from datetime import timedelta

# Configuration
DATA_DIR = r"c:\Users\JT\Downloads\Capstone\BckND\AIAP_BckND\training data to be extended"
OUTPUT_DIR = r"c:\Users\JT\Downloads\Capstone\BckND\AIAP_BckND\augmented_data"
EXTENSION_DAYS = 30
RANDOM_SEED = 42

np.random.seed(RANDOM_SEED)

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

def load_data():
    files = {
        'atm': pd.read_csv(os.path.join(DATA_DIR, 'atm_metadata.csv')),
        'cash': pd.read_csv(os.path.join(DATA_DIR, 'cash_status.csv')),
        'maint': pd.read_csv(os.path.join(DATA_DIR, 'maintenance_records.csv')),
        'logs': pd.read_csv(os.path.join(DATA_DIR, 'operational_logs.csv')),
        'trans': pd.read_csv(os.path.join(DATA_DIR, 'transactions.csv'))
    }
    # Parse dates
    files['cash']['timestamp'] = pd.to_datetime(files['cash']['timestamp'])
    files['maint']['maintenance_date'] = pd.to_datetime(files['maint']['maintenance_date'])
    files['logs']['timestamp'] = pd.to_datetime(files['logs']['timestamp'])
    files['trans']['transaction_time'] = pd.to_datetime(files['trans']['transaction_time'])
    return files

def augment_all():
    data = load_data()
    atms = data['atm']['atm_id'].unique()
    
    new_cash_list = []
    new_maint_list = []
    new_logs_list = []
    new_trans_list = []
    
    # Global Max IDs to avoid collisions
    max_cash_id = data['cash']['cash_id'].max()
    max_maint_id = data['maint']['maintenance_id'].max()
    max_log_id = data['logs']['log_id'].max()
    max_trans_id = data['trans']['transaction_id'].max()
    
    for atm_id in atms:
        # Get historical stats for this ATM
        atm_trans = data['trans'][data['trans']['atm_id'] == atm_id]
        atm_cash = data['cash'][data['cash']['atm_id'] == atm_id]
        
        if len(atm_trans) == 0 or len(atm_cash) == 0:
            continue
            
        last_date = atm_cash['timestamp'].max()
        last_cash = atm_cash[atm_cash['timestamp'] == last_date]['remaining_cash'].values[0]
        
        # Calculate daily transaction volume
        daily_vol = len(atm_trans) / (atm_trans['transaction_time'].max() - atm_trans['transaction_time'].min()).days
        avg_withdraw = atm_trans['withdrawal_amount'].mean()
        std_withdraw = atm_trans['withdrawal_amount'].std()
        
        current_cash = last_cash
        
        for d in range(1, EXTENSION_DAYS + 1):
            current_date = last_date + timedelta(days=d)
            
            # 1. Transactions for the day
            num_trans = np.random.poisson(daily_vol)
            day_withdraw_total = 0
            
            for _ in range(num_trans):
                max_trans_id += 1
                # Spread transactions across the day
                seconds = np.random.randint(0, 86400)
                trans_time = current_date + timedelta(seconds=seconds)
                
                amount = max(500, int(np.random.normal(avg_withdraw, std_withdraw / 2)))
                amount = (amount // 100) * 100 # Round to nearest 100
                
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
            
            # 2. Maintenance Events (Refills) - Every 3 days approx
            refill_amount = 0
            if np.random.random() < 0.2: # 20% chance refill
                max_maint_id += 1
                maint_type = 'Preventive' if np.random.random() < 0.75 else 'Corrective'
                refill_amount = int(np.random.choice(data['maint']['amount_added']))
                
                # Maintenance time
                maint_time = current_date + timedelta(hours=np.random.randint(1, 23))
                
                new_maint_list.append({
                    'maintenance_id': max_maint_id,
                    'atm_id': atm_id,
                    'maintenance_date': maint_time,
                    'maintenance_type': maint_type,
                    'amount_added': refill_amount
                })
            
            # 3. Operational Logs (Errors) - ~4 errors per month avg
            if np.random.random() < 0.15: # 15% chance of log entry
                max_log_id += 1
                uptime = 0 if np.random.random() < 0.1 else 1 # 10% chance error
                error = None
                duration = 0
                if uptime == 0:
                    error = np.random.choice(['CARD_JAM', 'NETWORK_FAIL', 'OUT_OF_SERVICE', 'CASH_LOW', 'E001'])
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
            
            # 4. Update Cash Status for the day end
            current_cash = max(0, current_cash - day_withdraw_total + refill_amount)
            max_cash_id += 1
            new_cash_list.append({
                'cash_id': max_cash_id,
                'atm_id': atm_id,
                'timestamp': current_date.date(),
                'remaining_cash': int(current_cash)
            })

    # Combine and Save
    files_to_save = {
        'cash_status': (data['cash'], pd.DataFrame(new_cash_list)),
        'maintenance_records': (data['maint'], pd.DataFrame(new_maint_list)),
        'operational_logs': (data['logs'], pd.DataFrame(new_logs_list)),
        'transactions': (data['trans'], pd.DataFrame(new_trans_list))
    }
    
    # Maintain Metadata
    data['atm'].to_csv(os.path.join(OUTPUT_DIR, 'atm_metadata.csv'), index=False)
    data['atm'].to_excel(os.path.join(OUTPUT_DIR, 'atm_metadata.xlsx'), index=False)

    for name, (orig, new) in files_to_save.items():
        # Ensure new column orders
        if not new.empty:
            # Sort new data by ATM then Time
            time_cols = {'cash_status': 'timestamp', 'maintenance_records': 'maintenance_date', 
                         'operational_logs': 'timestamp', 'transactions': 'transaction_time'}
            new = new.sort_values(by=['atm_id', time_cols[name]])
            
            # Concatenate
            combined = pd.concat([orig, new], ignore_index=True)
            
            # Save Excel (Preserve datetime objects)
            xlsx_path = os.path.join(OUTPUT_DIR, f"{name}.xlsx")
            combined.to_excel(xlsx_path, index=False)
            
            # Save CSV (Format dates back to string)
            csv_combined = combined.copy()
            csv_combined[time_cols[name]] = pd.to_datetime(csv_combined[time_cols[name]]).dt.strftime('%Y-%m-%d %H:%M:%S')
            if name == 'cash_status':
                 csv_combined['timestamp'] = pd.to_datetime(csv_combined['timestamp']).dt.strftime('%Y-%m-%d')
            
            csv_path = os.path.join(OUTPUT_DIR, f"{name}.csv")
            csv_combined.to_csv(csv_path, index=False)
            
            print(f"Saved {name} with {len(new)} new rows.")

if __name__ == "__main__":
    augment_all()
