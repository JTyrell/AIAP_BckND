"""Quick diagnostic: check how many rows the repository returns."""
import logging
logging.basicConfig(level=logging.WARNING)

from utils.repository import get_repository

r = get_repository()

# Raw daily metrics (no day filter)
df_raw = r.get_daily_metrics(days=None)
print(f"Raw daily metrics: {len(df_raw)} rows")
print(f"  Date range: {df_raw['record_date'].min()} to {df_raw['record_date'].max()}")

# With default 30-day filter
df_30 = r.get_daily_metrics(days=30)
print(f"30-day filtered:   {len(df_30)} rows")
print(f"  Date range: {df_30['record_date'].min()} to {df_30['record_date'].max()}")

# Combined with maintenance
df_full = r.get_metrics_with_maintenance(days=None)
print(f"With maintenance:  {len(df_full)} rows")

# Single ATM
df_one = r.get_metrics_with_maintenance(atm_id="ATM-001", days=30)
print(f"ATM-001 (30d):     {len(df_one)} rows")
