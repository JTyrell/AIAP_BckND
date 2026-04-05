"""
Generate fixture CSV files from the static ATM_DATA in Prototype_V2.jsx.
Creates:
  data/raw/atm_fixture_master.csv
  data/raw/atm_fixture_daily_metrics.csv
  data/raw/atm_fixture_maintenance_logs.csv

Run:  python scripts/generate_fixtures.py
"""
import os, sys, csv
from datetime import datetime, timedelta
import random
import math

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR = os.path.join(BASE_DIR, 'data', 'raw')
os.makedirs(RAW_DIR, exist_ok=True)

# ── Static ATM data (mirrored from Prototype_V2.jsx) ────────────────────────
ATMS = [
    dict(
        atm_id="ATM-001", name="Main Library Entrance", short="Library",
        manufacturer="NCR", model="SelfServ 87",
        lat=17.3985, lng=-76.5492, street="Ring Road",
        building="Faculty of Engineering",
        services="Cash Withdrawal;Balance Inquiry;Fund Transfer;Mini Statement",
        card_types="Visa;Mastercard;Maestro;LINX",
        currency="JMD", status="in_service",
        uptime=98.6, error_count=2, txns=147, cash_level=82,
        cash_stress=0.18, days_depl=6, avg_daily_wd=312000,
        velocity=12.3, avg_amt=4200, fail_prob=0.04,
        error_accel=0.12, health=94, health_trend=2.1,
        last_maint="2026-02-14", days_since_maint=19,
        maint_count_30d=1, corrective=False,
        weekly_txns=[112,119,128,138,152,184,98],
    ),
    dict(
        atm_id="ATM-002", name="Student Union Ground Floor",
        short="Student Union", manufacturer="Diebold",
        model="Nixdorf DN Series", lat=17.3991, lng=-76.5478,
        street="University Avenue", building="Student Union Complex",
        services="Cash Withdrawal;Cash Deposit;Balance Inquiry;Fund Transfer",
        card_types="Visa;Mastercard;Maestro;LINX;JCB",
        currency="JMD", status="in_service",
        uptime=91.2, error_count=11, txns=312, cash_level=23,
        cash_stress=0.77, days_depl=2, avg_daily_wd=598000,
        velocity=26.0, avg_amt=3800, fail_prob=0.38,
        error_accel=0.71, health=58, health_trend=-4.3,
        last_maint="2026-01-22", days_since_maint=42,
        maint_count_30d=0, corrective=False,
        weekly_txns=[278,295,304,322,340,398,198],
    ),
    dict(
        atm_id="ATM-003", name="Faculty of Medical Sciences",
        short="Med Sciences", manufacturer="Hyosung",
        model="MoniMax 7600", lat=17.3978, lng=-76.5501,
        street="Mona Road", building="Medical Sciences Block",
        services="Cash Withdrawal;Balance Inquiry",
        card_types="Visa;Mastercard;LINX",
        currency="JMD", status="in_service",
        uptime=96.4, error_count=4, txns=89, cash_level=67,
        cash_stress=0.33, days_depl=9, avg_daily_wd=228000,
        velocity=7.4, avg_amt=5100, fail_prob=0.09,
        error_accel=0.22, health=79, health_trend=0.5,
        last_maint="2026-02-20", days_since_maint=13,
        maint_count_30d=1, corrective=False,
        weekly_txns=[71,76,79,83,88,107,52],
    ),
    dict(
        atm_id="ATM-004", name="Administration Building",
        short="Admin Block", manufacturer="NCR", model="SelfServ 84",
        lat=17.3995, lng=-76.5488, street="Chancellor Drive",
        building="Administration Complex",
        services="Cash Withdrawal;Fund Transfer;Balance Inquiry",
        card_types="Visa;Mastercard;Maestro;LINX",
        currency="JMD", status="out_of_service",
        uptime=42.1, error_count=28, txns=0, cash_level=91,
        cash_stress=0.09, days_depl=17, avg_daily_wd=0,
        velocity=0, avg_amt=0, fail_prob=0.89,
        error_accel=2.14, health=19, health_trend=-11.2,
        last_maint="2025-12-18", days_since_maint=77,
        maint_count_30d=0, corrective=True,
        weekly_txns=[134,102,68,29,8,0,0],
    ),
    dict(
        atm_id="ATM-005", name="Sports Complex", short="Sports Complex",
        manufacturer="Wincor", model="ProCash 2050",
        lat=17.3972, lng=-76.5469, street="Stadium Road",
        building="UWI Sports & Cultural Centre",
        services="Cash Withdrawal;Balance Inquiry",
        card_types="Visa;Mastercard;LINX",
        currency="JMD", status="in_service",
        uptime=94.8, error_count=6, txns=198, cash_level=28,
        cash_stress=0.72, days_depl=3, avg_daily_wd=398000,
        velocity=16.5, avg_amt=3200, fail_prob=0.21,
        error_accel=0.34, health=71, health_trend=1.8,
        last_maint="2026-02-10", days_since_maint=23,
        maint_count_30d=1, corrective=False,
        weekly_txns=[167,178,184,196,210,252,124],
    ),
    dict(
        atm_id="ATM-006", name="Canteen & Dining Area", short="Canteen",
        manufacturer="Diebold", model="Nixdorf CS 300",
        lat=17.3988, lng=-76.5462, street="Union Road",
        building="Campus Dining Block",
        services="Cash Withdrawal;Cash Deposit;Balance Inquiry",
        card_types="Visa;Mastercard;Maestro;LINX",
        currency="JMD", status="in_service",
        uptime=95.9, error_count=5, txns=241, cash_level=61,
        cash_stress=0.39, days_depl=8, avg_daily_wd=488000,
        velocity=20.1, avg_amt=2900, fail_prob=0.13,
        error_accel=0.28, health=83, health_trend=-1.2,
        last_maint="2026-02-05", days_since_maint=28,
        maint_count_30d=1, corrective=False,
        weekly_txns=[198,210,216,228,244,294,148],
    ),
]

# Weekly multipliers (Mon=0..Sun=6) from JSX
WEEKLY_MULT = [0.88, 0.92, 0.95, 0.97, 1.05, 1.22, 0.80]
DAYS = 30
TODAY = datetime(2026, 3, 5)


def _jitter(val, pct=0.03):
    """Add ±pct random jitter."""
    return val * (1 + random.uniform(-pct, pct))


def generate():
    random.seed(42)

    # ── 1. Master CSV ────────────────────────────────────────────────────
    master_path = os.path.join(RAW_DIR, 'atm_fixture_master.csv')
    master_cols = [
        'atm_id', 'name', 'short', 'manufacturer', 'model',
        'lat', 'lng', 'street', 'building',
        'services', 'card_types', 'currency',
    ]
    with open(master_path, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=master_cols)
        w.writeheader()
        for a in ATMS:
            w.writerow({k: a[k] for k in master_cols})
    print(f"[OK] {master_path}  ({len(ATMS)} rows)")

    # ── 2. Daily Metrics CSV ─────────────────────────────────────────────
    metrics_path = os.path.join(RAW_DIR, 'atm_fixture_daily_metrics.csv')
    metrics_cols = [
        'atm_id', 'record_date', 'uptime_percentage', 'uptime_minutes',
        'error_count', 'transaction_count',
        'starting_cash_balance', 'ending_cash_balance',
        'operational_status',
    ]
    rows = []
    for a in ATMS:
        base_cash = int(a['avg_daily_wd'] * a['days_depl'] + a['avg_daily_wd'])
        if base_cash == 0:
            base_cash = 1_000_000

        for d in range(DAYS):
            date = TODAY - timedelta(days=DAYS - 1 - d)
            dow = date.weekday()
            mult = WEEKLY_MULT[dow]

            # Uptime degrades slightly for critical ATMs in recent days
            if a['atm_id'] == 'ATM-004' and d > 20:
                up = max(30, _jitter(a['uptime'] - (d - 20) * 2.5, 0.02))
            else:
                up = min(100, max(0, _jitter(a['uptime'], 0.015)))

            uptime_mins = round(up / 100 * 1440)
            errs = max(0, round(_jitter(a['error_count'], 0.25)))
            txns = max(0, round(_jitter(a['txns'] * mult, 0.08)))

            # Cash: start high, deplete daily
            daily_wd = max(0, _jitter(a['avg_daily_wd'], 0.1)) if txns > 0 else 0
            start_cash = round(base_cash - daily_wd * max(0, d - 15))
            start_cash = max(100_000, start_cash)
            end_cash = max(0, round(start_cash - daily_wd))

            status = a['status']
            if a['atm_id'] == 'ATM-004' and d >= 23:
                status = 'out_of_service'

            rows.append({
                'atm_id': a['atm_id'],
                'record_date': date.strftime('%Y-%m-%d'),
                'uptime_percentage': round(up, 1),
                'uptime_minutes': uptime_mins,
                'error_count': errs,
                'transaction_count': txns,
                'starting_cash_balance': start_cash,
                'ending_cash_balance': end_cash,
                'operational_status': status,
            })

    with open(metrics_path, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=metrics_cols)
        w.writeheader()
        w.writerows(rows)
    print(f"[OK] {metrics_path}  ({len(rows)} rows)")

    # ── 3. Maintenance Logs CSV ──────────────────────────────────────────
    maint_path = os.path.join(RAW_DIR, 'atm_fixture_maintenance_logs.csv')
    maint_cols = ['atm_id', 'service_date', 'service_type', 'description']
    maint_rows = []
    for a in ATMS:
        if a['last_maint']:
            stype = 'corrective' if a['corrective'] else 'preventive'
            maint_rows.append({
                'atm_id': a['atm_id'],
                'service_date': a['last_maint'],
                'service_type': stype,
                'description': f"{'Corrective' if a['corrective'] else 'Preventive'} maintenance",
            })
    with open(maint_path, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=maint_cols)
        w.writeheader()
        w.writerows(maint_rows)
    print(f"[OK] {maint_path}  ({len(maint_rows)} rows)")

    print("\nAll fixture CSVs generated successfully.")


if __name__ == '__main__':
    generate()
