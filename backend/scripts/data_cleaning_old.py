import pandas as pd
from sklearn.feature_selection import SelectKBest, f_classif


# 1. LOAD DATA
logs = pd.read_csv("operational_logs.csv", parse_dates=["timestamp"])
cash = pd.read_csv("cash_status.csv", parse_dates=["timestamp"])
atm = pd.read_csv("atm_metadata.csv")


# 2. CLEAN + SORT
logs = logs.drop_duplicates()
logs = logs.sort_values(["atm_id", "timestamp"]).reset_index(drop=True)


# 3. MERGE ATM METADATA
df = logs.merge(atm, on="atm_id", how="left")


# 4. MERGE CASH STATUS
df = df.sort_values(["timestamp", "atm_id"]).reset_index(drop=True)
cash = cash.sort_values(["timestamp", "atm_id"]).reset_index(drop=True)

df = pd.merge_asof(
    df,
    cash,
    on="timestamp",
    by="atm_id",
    direction="backward"
)


# 5. CREATE FUTURE FAILURE TARGET
df = df.sort_values(["atm_id", "timestamp"]).reset_index(drop=True)

prediction_window = pd.Timedelta(hours=1)
df["future_failure"] = 0

for atm_id, group in df.groupby("atm_id"):
    group = group.sort_values("timestamp")

    for i in group.index:
        current_time = df.loc[i, "timestamp"]

        future_rows = group[
            (group["timestamp"] > current_time)
            & (group["timestamp"] <= current_time + prediction_window)
        ]

        if (future_rows["uptime_status"] == 0).any():
            df.loc[i, "future_failure"] = 1


# 6. FEATURE ENGINEERING
df["hour"] = df["timestamp"].dt.hour
df["day_of_week"] = df["timestamp"].dt.dayofweek
df["is_weekend"] = (df["day_of_week"] >= 5).astype(int)

df["low_cash_flag"] = (df["remaining_cash"] < 200000).astype(int)

df["cash_change"] = (
    df.groupby("atm_id")["remaining_cash"]
    .diff()
    .fillna(0)
)

# Time since last actual failure
df["last_failure_time"] = df["timestamp"].where(df["uptime_status"] == 0)
df["last_failure_time"] = df.groupby("atm_id")["last_failure_time"].ffill()

df["time_since_last_failure"] = (
    df["timestamp"] - df["last_failure_time"]
).dt.total_seconds()

df["time_since_last_failure"] = df["time_since_last_failure"].fillna(999999999)

# Recent actual failures from previous logs only, within each ATM
df["past_failure"] = (
    df.groupby("atm_id")["uptime_status"]
    .shift(1)
    .fillna(1)
)

df["past_failure_flag"] = (df["past_failure"] == 0).astype(int)

df["recent_failures"] = (
    df.groupby("atm_id")["past_failure_flag"]
    .rolling(window=5)
    .sum()
    .reset_index(level=0, drop=True)
    .fillna(0)
)


# 7. FEATURE SELECTION USING FISHER SCORE
features = [
    "remaining_cash",
    "cash_change",
    "hour",
    "day_of_week",
    "is_weekend",
    "low_cash_flag",
    "time_since_last_failure",
    "recent_failures"
]

X = df[features]
y = df["future_failure"]

selector = SelectKBest(score_func=f_classif, k="all")
selector.fit(X, y)

feature_scores = list(zip(X.columns, selector.scores_))
feature_scores = sorted(feature_scores, key=lambda x: x[1], reverse=True)

print("\nFISHER SCORES:")
for feature, score in feature_scores:
    print(f"{feature}: {score:.4f}")


# 8. SELECT TOP FEATURES
top_k = 5

selector_top = SelectKBest(score_func=f_classif, k=top_k)
selector_top.fit(X, y)

selected_features = X.columns[selector_top.get_support()]

print("\nSELECTED FEATURES:")
print(selected_features)


# 9. SAVE CLEANED DATASET FOR MODELING TEAM
final_columns = [
    "atm_id",
    "timestamp",
    "future_failure"
] + features

final_df = df[final_columns]

final_df.to_csv("final_atm_feature_dataset.csv", index=False)

print("\nSaved final dataset as final_atm_feature_dataset.csv")
print(final_df.head())