#!/usr/bin/env python3

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random


np.random.seed(42)
random.seed(42)

START_DATE = datetime(2025, 1, 1, 0, 0, 0)
END_DATE = datetime(2025, 7, 1, 0, 0, 0)   # 6 months
TOTAL_DAYS = (END_DATE - START_DATE).days   # 181 days

atm_data = [
    {'atm_id': 'NCB0001', 'atm_bank': 'National Commercial Bank', 'location': 'NCB UWI Branch',        'atm_model': 'NCR_SelfServ_22'},
    {'atm_id': 'NCB0002', 'atm_bank': 'National Commercial Bank', 'location': 'NCB UWI Branch',        'atm_model': 'NCR_SelfServ_22'},
    {'atm_id': 'NCB0003', 'atm_bank': 'National Commercial Bank', 'location': 'NCB UWI Branch',        'atm_model': 'NCR_SelfServ_22'},
    {'atm_id': 'NCB0004', 'atm_bank': 'National Commercial Bank', 'location': 'NCB UWI Branch',        'atm_model': 'DN_Series_100D'},
    {'atm_id': 'NCB0005', 'atm_bank': 'National Commercial Bank', 'location': 'FSS Parking Lot',       'atm_model': 'NCR_SelfServ_22'},
    {'atm_id': 'JNB0006', 'atm_bank': 'Jamaica National Bank',    'location': 'JN Bank UWI Branch',    'atm_model': 'DN_Series_100D'},
    {'atm_id': 'BNS0007', 'atm_bank': 'Bank of Nova Scotia',      'location': 'Scotiabank UWI Branch', 'atm_model': 'NCR_SelfServ_22'},
    {'atm_id': 'BNS0008', 'atm_bank': 'Bank of Nova Scotia',      'location': 'MSBM Parking Lot',      'atm_model': 'DN_Series_100D'},
    {'atm_id': 'BNS0009', 'atm_bank': 'Bank of Nova Scotia',      'location': 'MSBM Parking Lot',      'atm_model': 'DN_Series_100D'},
    {'atm_id': 'SAG0010', 'atm_bank': 'Sagicor Bank',             'location': 'Student Union',         'atm_model': 'NCR_SelfServ_22'}
]

atm_df = pd.DataFrame(atm_data)
atm_ids = atm_df['atm_id'].tolist()

traffic_profile = {
    'NCB0001': 2.8, 'NCB0002': 2.6, 'NCB0003': 2.5, 'NCB0004': 2.4,
    'NCB0005': 1.8, 'BNS0007': 2.7,
    'JNB0006': 1.4, 'BNS0008': 1.3, 'BNS0009': 1.3, 'SAG0010': 1.2
}

total_weight = sum(traffic_profile.values())
tx_target = 100_000

print(f"Generating dataset for {len(atm_df)} ATMs over {TOTAL_DAYS} days...")

transactions = []
tx_id = 1

for atm in atm_data:
    atm_id = atm['atm_id']
    weight = traffic_profile[atm_id]
    atm_tx_target = int((weight / total_weight) * tx_target)

    all_days = [START_DATE + timedelta(days=d) for d in range(TOTAL_DAYS)]
    tx_per_day = atm_tx_target // TOTAL_DAYS
    remainder = atm_tx_target % TOTAL_DAYS

    for i, day in enumerate(all_days):
        is_weekend = day.weekday() >= 5
        daily_count = tx_per_day + (1 if i < remainder else 0)
        if is_weekend:
            daily_count = int(daily_count * 1.2)

        for _ in range(daily_count):
            hour = random.choices(
                range(24),
                weights=[1.3 if h < 8 or h >= 20 else 4.5 for h in range(24)]
            )[0]
            tx_time = day.replace(
                hour=hour,
                minute=random.randint(0, 59),
                second=random.randint(0, 59)
            )
            if tx_time >= END_DATE:
                tx_time = END_DATE - timedelta(seconds=1)

            is_student = random.random() < 0.65

            if is_student:
                bill = random.choices(
                    [500, 1000, 2000, 5000],
                    weights=[0.40, 0.30, 0.20, 0.10]
                )[0]
                num_bills = random.choices(
                    range(1, 11),
                    weights=[30, 25, 18, 12, 7, 4, 2, 1, 0.5, 0.5]
                )[0]
            else:
                bill = random.choices(
                    [500, 1000, 2000, 5000],
                    weights=[0.15, 0.25, 0.30, 0.30]
                )[0]
                num_bills = random.choices(
                    range(1, 21),
                    weights=[10, 12, 14, 13, 11, 9, 7, 6, 5, 4, 3, 2, 1.5, 1, 1, 0.5, 0.5, 0.3, 0.2, 0.2]
                )[0]

            withdrawal_amount = int(bill * num_bills)
            transaction_status = 1 if random.random() < 0.96 else 0

            transactions.append({
                'transaction_id': tx_id,
                'atm_id': atm_id,
                'transaction_time': tx_time,
                'withdrawal_amount': withdrawal_amount,
                'transaction_status': transaction_status
            })
            tx_id += 1

tx_df = pd.DataFrame(transactions)
print(f"Generated {len(tx_df):,} transactions")


# OPERATIONAL LOGS (3–5 per ATM per day)
logs = []
log_id = 1

for atm in atm_data:
    atm_id = atm['atm_id']
    is_problematic = (atm_id == 'NCB0005')

    for d in range(TOTAL_DAYS):
        day = START_DATE + timedelta(days=d)
        num_logs = random.randint(3, 5)
        used_hours = random.sample(range(24), min(num_logs, 24))

        for hour in used_hours:
            ts = day.replace(
                hour=hour,
                minute=random.randint(0, 59),
                second=random.randint(0, 59)
            )

            if is_problematic and random.random() < 0.68:
                uptime_status = 0
                error_code = random.choice(['OUT_OF_SERVICE', 'CARD_JAM', 'CASH_LOW', 'NETWORK_FAIL', 'E001'])
                downtime_duration = random.randint(45, 720)
            else:
                uptime_status = 1 if random.random() < 0.965 else 0
                error_code = None if uptime_status == 1 else random.choice(['CARD_JAM', 'NETWORK_FAIL', 'PRINTER_ERROR', 'E005'])
                downtime_duration = random.randint(5, 180) if uptime_status == 0 else 0

            logs.append({
                'log_id': log_id,
                'atm_id': atm_id,
                'timestamp': ts,
                'uptime_status': uptime_status,
                'error_code': error_code,
                'downtime_duration': downtime_duration
            })
            log_id += 1

logs_df = pd.DataFrame(logs)
print(f"✓ Generated {len(logs_df):,} operational logs (3–5 per ATM per day)")


cash_records = []
maint_records = []
cash_id = 1
maint_id = 1

tx_by_atm = {atm_id: [] for atm_id in atm_ids}
for t in transactions:
    tx_by_atm[t['atm_id']].append(t)
for atm_id in atm_ids:
    tx_by_atm[atm_id].sort(key=lambda x: x['transaction_time'])

for atm in atm_data:
    atm_id = atm['atm_id']
    current_cash = random.choice(range(700_000, 1_000_001, 1000))
    low_threshold = 150_000
    atm_txs = tx_by_atm[atm_id]
    tx_idx = 0

    for d in range(TOTAL_DAYS + 1):
        snapshot_time = START_DATE + timedelta(days=d)
        if snapshot_time > END_DATE:
            snapshot_time = END_DATE

        while tx_idx < len(atm_txs) and atm_txs[tx_idx]['transaction_time'] <= snapshot_time:
            tx = atm_txs[tx_idx]
            if tx['transaction_status'] == 1:
                current_cash -= tx['withdrawal_amount']
            tx_idx += 1

        current_cash = max(0, current_cash)

        # Refill BEFORE recording snapshot so cash never shows as 0
        if current_cash < low_threshold:
            refill_amount = random.choice(range(700_000, 1_000_001, 1000))
            current_cash += refill_amount

        cash_records.append({
            'cash_id': cash_id,
            'atm_id': atm_id,
            'timestamp': snapshot_time,
            'remaining_cash': int(current_cash)
        })
        cash_id += 1

cash_df = pd.DataFrame(cash_records)
print(f"Generated {len(cash_df):,} cash status records (1 per ATM per day)")


#MAINTENANCE (every 2 days per ATM based on transaction volume)

for atm in atm_data:
    atm_id = atm['atm_id']
    atm_txs = tx_by_atm[atm_id]

    tx_by_day = {}
    for t in atm_txs:
        day_key = t['transaction_time'].date()
        tx_by_day[day_key] = tx_by_day.get(day_key, 0) + 1

    avg_daily = len(atm_txs) / TOTAL_DAYS

    d = 0
    while d < TOTAL_DAYS:
        maint_date = START_DATE + timedelta(days=d, hours=random.randint(1, 23))
        day_key = maint_date.date()
        daily_vol = tx_by_day.get(day_key, 0)

        if daily_vol > avg_daily * 1.2:
            maint_type = random.choices(['Corrective', 'Preventive'], weights=[0.65, 0.35])[0]
        else:
            maint_type = random.choices(['Corrective', 'Preventive'], weights=[0.25, 0.75])[0]

        amount_added = random.choice(range(700_000, 1_000_001, 1000)) if maint_type == 'Corrective' else random.choice(range(300_000, 700_001, 1000))

        maint_records.append({
            'maintenance_id': maint_id,
            'atm_id': atm_id,
            'maintenance_date': maint_date,
            'maintenance_type': maint_type,
            'amount_added': amount_added
        })
        maint_id += 1
        d += 2

maint_df = pd.DataFrame(maint_records)
print(f"Generated {len(maint_df):,} maintenance records (every 2 days per ATM)")


#SORT & EXPORT CSV
tx_df = tx_df.sort_values('transaction_id').reset_index(drop=True)
logs_df = logs_df.sort_values('log_id').reset_index(drop=True)
cash_df = cash_df.sort_values(['atm_id', 'timestamp']).reset_index(drop=True)
maint_df = maint_df.sort_values('maintenance_id').reset_index(drop=True)

atm_df.to_csv('atm_metadata.csv', index=False)
tx_df.to_csv('transactions.csv', index=False)
logs_df.to_csv('operational_logs.csv', index=False)
cash_df.to_csv('cash_status.csv', index=False)
maint_df.to_csv('maintenance_records.csv', index=False)

total_records = len(atm_df) + len(tx_df) + len(logs_df) + len(cash_df) + len(maint_df)

print("\n Dataset generation completed successfully!")
print(f"\nRecord counts:")
print(f"   • atm_metadata       : {len(atm_df):,}")
print(f"   • transactions       : {len(tx_df):,}")
print(f"   • operational_logs   : {len(logs_df):,}")
print(f"   • cash_status        : {len(cash_df):,}")
print(f"   • maintenance_records: {len(maint_df):,}")
print(f"\n   Total records        : {total_records:,}")
