import os
import sys
import pandas as pd
from sqlalchemy import create_engine, text

# Add the root directory to sys.path so we can import config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import Config

def init_db():
    if not Config.DATABASE_URL:
        print("DATABASE_URL is not set in the environment or config.")
        return

    engine = create_engine(Config.DATABASE_URL)
    
    db_script_path = os.path.join(os.path.dirname(__file__), 'db', 'database_tables.session.sql')
    if not os.path.exists(db_script_path):
        # Fallback if running before restructuring
        db_script_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'database_tables.session.sql')

    print("Initializing tables...")
    try:
        with engine.connect() as conn:
            with open(db_script_path, 'r') as f:
                sql = f.read()
                # Basic split by semicolon to execute commands
                commands = sql.split(';')
                for cmd in commands:
                    if cmd.strip():
                        conn.execute(text(cmd))
            conn.commit()
    except Exception as e:
        print("\nERROR: Could not connect to the PostgreSQL database.")
        print("Please ensure that PostgreSQL is installed, running, and the credentials in your .env file are correct.")
        print(f"Current DATABASE_URL: {Config.DATABASE_URL}\n")
        print(f"Technical details: {e}")
        return

    print("Loading data from CSVs...")
    datasets_dir = Config.DATASETS_DIR
    if not os.path.exists(datasets_dir):
        # Fallback to current raw directory if running before restructure
        datasets_dir = os.path.join(Config.DATA_DIR, 'raw')

    # Transactions
    tx_file = os.path.join(datasets_dir, 'transactions.csv')
    if os.path.exists(tx_file):
        print("Loading transactions...")
        df = pd.read_csv(tx_file)
        # Check column names: transaction_time, withdrawal_amount, etc.
        if 'amount' in df.columns:
            df.rename(columns={'amount': 'withdrawal_amount'}, inplace=True)
        # Only keep columns in schema if they match
        schema_cols = ['atm_id', 'transaction_time', 'withdrawal_amount', 'transaction_status']
        cols_to_insert = [c for c in schema_cols if c in df.columns]
        df[cols_to_insert].to_sql('transactions', engine, if_exists='append', index=False)

    # Operational Logs
    logs_file = os.path.join(datasets_dir, 'operational_logs.csv')
    if os.path.exists(logs_file):
        print("Loading operational logs...")
        df = pd.read_csv(logs_file)
        schema_cols = ['atm_id', 'timestamp', 'uptime_status', 'error_code', 'downtime_duration']
        cols_to_insert = [c for c in schema_cols if c in df.columns]
        df[cols_to_insert].to_sql('operational_logs', engine, if_exists='append', index=False)

    # Cash Status
    cash_file = os.path.join(datasets_dir, 'cash_status.csv')
    if os.path.exists(cash_file):
        print("Loading cash status...")
        df = pd.read_csv(cash_file)
        schema_cols = ['atm_id', 'timestamp', 'remaining_cash']
        cols_to_insert = [c for c in schema_cols if c in df.columns]
        df[cols_to_insert].to_sql('cash_status', engine, if_exists='append', index=False)

    # Maintenance Records
    maint_file = os.path.join(datasets_dir, 'maintenance_records.csv')
    if os.path.exists(maint_file):
        print("Loading maintenance records...")
        df = pd.read_csv(maint_file)
        schema_cols = ['atm_id', 'maintenance_date', 'maintenance_type', 'amount_added']
        cols_to_insert = [c for c in schema_cols if c in df.columns]
        df[cols_to_insert].to_sql('maintenance_records', engine, if_exists='append', index=False)

    print("Database initialization complete.")

if __name__ == "__main__":
    init_db()
