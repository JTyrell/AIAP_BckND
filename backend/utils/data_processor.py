"""
AIAP — Data Processing & Intelligent Imputation
Uses Scikit-Learn IterativeImputer for missing value handling with ATM constraints.
"""
import logging
import numpy as np
import pandas as pd
from sklearn.experimental import enable_iterative_imputer  # noqa
from sklearn.impute import IterativeImputer
from sklearn.linear_model import BayesianRidge
from sklearn.preprocessing import LabelEncoder

logger = logging.getLogger(__name__)

def check_and_impute_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans incoming ATM metrics and uses IterativeImputer to fill gaps.
    Enforces domain-specific constraints (e.g. uptime <= 100).
    """
    if df.empty:
        return df

    df = df.copy()
    
    # 1. Pre-processing: Type conversion & Feature Extraction
    numeric_cols = [
        'uptime_percentage', 'error_count', 'transaction_count',
        'starting_cash_balance', 'ending_cash_balance'
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    if 'record_date' in df.columns:
        df['record_date'] = pd.to_datetime(df['record_date'], errors='coerce')
        df['day_of_week'] = df['record_date'].dt.dayofweek.fillna(0).astype(int)
    else:
        df['day_of_week'] = 0

    # Label encode atm_id to give the imputer context for specific ATM behaviors
    le = LabelEncoder()
    if 'atm_id' in df.columns:
        df['atm_id_encoded'] = le.fit_transform(df['atm_id'].fillna('UNKNOWN'))
    else:
        df['atm_id_encoded'] = 0

    # 2. Imputation Setup
    feats_to_impute = [
        'atm_id_encoded', 'day_of_week', 'uptime_percentage',
        'error_count', 'transaction_count', 'starting_cash_balance', 'ending_cash_balance'
    ]
    
    # Ensure all columns exist in df before slice
    for f in feats_to_impute:
        if f not in df.columns:
            df[f] = np.nan

    impute_data = df[feats_to_impute]

    # Define constraints (min_value, max_value) for each column
    min_vals = [-np.inf, 0, 0.0, 0.0, 0.0, 0.0, 0.0]
    max_vals = [np.inf, 6, 100.0, 1000.0, 5000.0, 10000000.0, 10000000.0]

    imputer = IterativeImputer(
        estimator=BayesianRidge(),
        max_iter=10,
        random_state=42,
        min_value=min_vals,
        max_value=max_vals
    )

    # 3. Perform Imputation
    try:
        imputed_array = imputer.fit_transform(impute_data)
        imputed_df = pd.DataFrame(imputed_array, columns=feats_to_impute, index=df.index)
        
        # Update original dataframe
        for col in numeric_cols:
            df[col] = imputed_df[col]
            
    except Exception as e:
        logger.error(f"IterativeImputer failed: {e}. Falling back to basic fillna(0).")
        df[numeric_cols] = df[numeric_cols].fillna(0)

    # 4. Post-processing: Domain Constraints & Rounding
    df['ending_cash_balance'] = df[['ending_cash_balance', 'starting_cash_balance']].min(axis=1)
    
    df['error_count'] = df['error_count'].round(0).astype(int)
    df['transaction_count'] = df['transaction_count'].round(0).astype(int)
    df['starting_cash_balance'] = df['starting_cash_balance'].round(0).astype(int)
    df['ending_cash_balance'] = df['ending_cash_balance'].round(0).astype(int)

    df.drop(columns=['atm_id_encoded', 'day_of_week'], inplace=True, errors='ignore')

    logger.info(f"Imputation complete for {len(df)} rows.")
    return df
