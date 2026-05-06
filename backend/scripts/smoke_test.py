"""Quick smoke test for the AIAP ML engine pipeline."""
import logging
logging.basicConfig(level=logging.INFO)

from utils.repository import get_repository
from ml_engine.feature_engineering import engineer_features
from ml_engine.pipeline import train_all_models, predict_for_atm, generate_alerts

# 1. Load fixture data via Smart Stub
repo = get_repository()
df = repo.get_metrics_with_maintenance()
print(f"\n[1] Metrics loaded: {len(df)} rows, {len(df.columns)} cols")

# 2. Feature engineering
df_feat = engineer_features(df)
print(f"[2] Features engineered: {len(df_feat)} rows, {len(df_feat.columns)} cols")

health_by_atm = df_feat.groupby('atm_id')['composite_health_score'].last()
print(f"    Health scores (latest): {health_by_atm.to_dict()}")

# 3. Train all models
result = train_all_models(repository=repo)
print(f"\n[3] Training result: {result}")

# 4. Predict for each ATM
print("\n[4] Per-ATM predictions:")
for atm_id in df_feat['atm_id'].unique():
    pred = predict_for_atm(atm_id, repository=repo)
    alerts = generate_alerts(pred)
    print(f"    {atm_id}: health={pred['health_score']}, "
          f"fail={pred['failure_probability']}, "
          f"activity={pred['activity_level']}, "
          f"depletion={pred['days_to_depletion']}d, "
          f"alerts={alerts}")

print("\n[OK] ALL SMOKE TESTS PASSED")
