"""
Validation Script for AIAP Intelligent Imputation
Generates a messy dataset and verifies the IterativeImputer output.
"""
import sys
import os
import pandas as pd
import numpy as np

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.data_processor import check_and_impute_metrics

def main():
    print("--- STARTING IMPUTATION VALIDATION ---")
    
    # 1. Generate Synthetic Messy Data
    data = {
        'atm_id': ['ATM-001', 'ATM-001', 'ATM-002', 'ATM-002', 'ATM-003'],
        'record_date': ['2026-04-01', '2026-04-02', '2026-04-01', '2026-04-02', '2026-04-01'],
        'uptime_percentage': [98.5, np.nan, 90.0, np.nan, 45.0],
        'error_count': [2, np.nan, 15, 20, np.nan],
        'transaction_count': [150, 160, np.nan, 310, 10],
        'starting_cash_balance': [1000000, 1000000, 500000, 500000, 200000],
        'ending_cash_balance': [800000, np.nan, 100000, np.nan, np.nan]
    }
    
    df_messy = pd.DataFrame(data)
    print("\nOriginal Messy Data:")
    print(df_messy)
    
    # 2. Run Imputation
    df_clean = check_and_impute_metrics(df_messy)
    
    print("\nCleaned/Imputed Data:")
    print(df_clean)
    
    # 3. Validations
    print("\n--- RUNNING CHECKS ---")
    
    # Check for NaNs
    nan_count = df_clean.isna().sum().sum()
    print(f"Total NaNs remaining: {nan_count}")
    assert nan_count == 0, "ERROR: NaNs still present in cleaned data"
    
    # Check Uptime Range
    assert df_clean['uptime_percentage'].max() <= 100, "ERROR: Uptime > 100"
    assert df_clean['uptime_percentage'].min() >= 0, "ERROR: Uptime < 0"
    print("Check: Uptime bounds [0, 100] PASSED")
    
    # Check Cash Logic
    cash_violations = (df_clean['ending_cash_balance'] > df_clean['starting_cash_balance']).sum()
    print(f"Cash balance violations: {cash_violations}")
    assert cash_violations == 0, "ERROR: ending_cash > starting_cash"
    print("Check: ending_cash <= starting_cash PASSED")
    
    # Check Integer Types
    assert pd.api.types.is_integer_dtype(df_clean['transaction_count']), "ERROR: transaction_count is not integer"
    assert pd.api.types.is_integer_dtype(df_clean['error_count']), "ERROR: error_count is not integer"
    print("Check: Integer types for counts PASSED")

    print("\n--- VALIDATION SUCCESSFUL ---")

if __name__ == "__main__":
    main()
