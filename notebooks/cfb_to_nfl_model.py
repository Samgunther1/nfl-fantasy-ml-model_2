import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.impute import SimpleImputer
from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.model_selection import cross_val_score
import pickle
import os

# ── Config ────────────────────────────────────────────────────────────────────

DATA_DIR   = "data/processed"
OUTPUT_DIR = "data/processed"
MODEL_DIR  = "models"

os.makedirs(MODEL_DIR, exist_ok=True)

# Columns that are metadata/identifiers — never used as features
META_COLS = [
    "player_key", "athlete_name", "season", "position",
    "athlete_id", "nfl_player_id", "nfl_name",
    "nfl_rookie_season", "avg_weekly_ppr", "n_weeks"
]

# interceptions columns are all-NA across every position — drop them
DROP_COLS = [
    "interceptions_td_pg",
    "interceptions_yds_pg",
    "interceptions_int_pg"
]

TARGET = "avg_weekly_ppr"

# ── Helpers ───────────────────────────────────────────────────────────────────

def get_feature_cols(df):
    """Return columns that are actual model features (numeric, not meta/drop)."""
    exclude = set(META_COLS + DROP_COLS + [TARGET])
    return [c for c in df.columns if c not in exclude and pd.api.types.is_numeric_dtype(df[c])]


def train_position_model(pos: str):
    print(f"\n{'='*50}")
    print(f"  Training CFB → NFL model: {pos.upper()}")
    print(f"{'='*50}")

    # ── Load data ──────────────────────────────────────────────────────────────
    path = os.path.join(DATA_DIR, f"cfb_to_nfl_{pos}_modeling.csv")
    df = pd.read_csv(path)
    print(f"  Loaded {len(df)} rows, {len(df.columns)} columns")

    feature_cols = get_feature_cols(df)
    print(f"  Features ({len(feature_cols)}): {feature_cols}")

    X = df[feature_cols].copy()
    y = df[TARGET].copy()

    # ── Impute NAs with median (robust to outliers, works well with RF) ────────
    imputer = SimpleImputer(strategy="median")
    X_imputed = imputer.fit_transform(X)
    X_imputed = pd.DataFrame(X_imputed, columns=feature_cols)

    # ── Train random forest ────────────────────────────────────────────────────
    rf = RandomForestRegressor(
        n_estimators=500,
        max_features="sqrt",
        min_samples_leaf=5,
        random_state=42,
        n_jobs=-1
    )

    # Cross-validated MAE to sanity check before final fit
    cv_mae = -cross_val_score(rf, X_imputed, y, cv=5, scoring="neg_mean_absolute_error")
    print(f"  5-fold CV MAE: {cv_mae.mean():.4f} ± {cv_mae.std():.4f}")

    cv_r2 = cross_val_score(rf, X_imputed, y, cv=5, scoring="r2")
    print(f"  5-fold CV R²:  {cv_r2.mean():.4f} ± {cv_r2.std():.4f}")

    # Final fit on full training data
    rf.fit(X_imputed, y)

    # In-sample metrics (for reference only)
    y_pred_train = rf.predict(X_imputed)
    print(f"  Train MAE: {mean_absolute_error(y, y_pred_train):.4f}")
    print(f"  Train R²:  {r2_score(y, y_pred_train):.4f}")

    # ── Feature importances ────────────────────────────────────────────────────
    importances = pd.Series(rf.feature_importances_, index=feature_cols).sort_values(ascending=False)
    print(f"\n  Top 10 feature importances:")
    print(importances.head(10).to_string())

    # ── Generate cfb_projected_ppr for every row in training data ──────────────
    # This is what will be joined into the NFL model's training set
    df["cfb_projected_ppr"] = rf.predict(X_imputed)

    projection_cols = ["player_key", "nfl_player_id", "nfl_name", "nfl_rookie_season", "cfb_projected_ppr", TARGET, "n_weeks"]
    # QB has extra meta cols; use only what's present
    projection_cols = [c for c in projection_cols if c in df.columns]
    projections = df[projection_cols].copy()

    proj_path = os.path.join(OUTPUT_DIR, f"cfb_to_nfl_{pos}_projections.csv")
    projections.to_csv(proj_path, index=False)
    print(f"\n  Projections saved → {proj_path}")

    # ── Save model + imputer together ─────────────────────────────────────────
    artifact = {
        "model": rf,
        "imputer": imputer,
        "feature_cols": feature_cols,
        "position": pos
    }
    model_path = os.path.join(MODEL_DIR, f"cfb_to_nfl_{pos}_rf.pkl")
    with open(model_path, "wb") as f:
        pickle.dump(artifact, f)
    print(f"  Model saved    → {model_path}")

    return artifact, projections


def predict_new_players(pos: str, new_df: pd.DataFrame) -> pd.DataFrame:
    """
    Score a new set of CFB players (e.g. 2025 draft class) using a saved model.
    Returns new_df with a cfb_projected_ppr column appended.

    Usage:
        draft_class = pd.read_csv("data/raw/cfb_2025_draft_class_wr.csv")
        scored = predict_new_players("wr", draft_class)
    """
    model_path = os.path.join(MODEL_DIR, f"cfb_to_nfl_{pos}_rf.pkl")
    with open(model_path, "rb") as f:
        artifact = pickle.load(f)

    rf         = artifact["model"]
    imputer    = artifact["imputer"]
    feature_cols = artifact["feature_cols"]

    X = new_df[feature_cols].copy()
    X_imputed = imputer.transform(X)
    new_df = new_df.copy()
    new_df["cfb_projected_ppr"] = rf.predict(X_imputed)
    return new_df


# ── Main ──────────────────────────────────────────────────────────────────────

def append_rookies_to_nfl_predict(pos: str, artifact: dict):
    """
    Score the 2025 CFB draft class using the trained CFB model, then append
    those rookies into the NFL model's existing predict file for that position
    so Cell 2 of the NFL notebook picks them up automatically.

    Rookies are appended with their CFB stat columns set to NaN (the NFL model
    has no NFL stats for them) and cfb_projected_ppr populated. The NFL model's
    median imputer will handle the NaN stat columns at prediction time, leaning
    on cfb_projected_ppr as the informative signal.
    """
    cfb_predict_path = os.path.join(DATA_DIR, f"cfb_to_nfl_{pos}_predict_2026.csv")
    nfl_predict_path = os.path.join(OUTPUT_DIR, f"nfl_to_nfl_{pos}_predict_2026.csv")

    if not os.path.exists(cfb_predict_path):
        print(f"  [SKIP] No CFB predict file found for {pos.upper()} at {cfb_predict_path}")
        return

    if not os.path.exists(nfl_predict_path):
        print(f"  [SKIP] No NFL predict file found for {pos.upper()} at {nfl_predict_path}")
        return

    rf           = artifact["model"]
    imputer      = artifact["imputer"]
    feature_cols = artifact["feature_cols"]

    # Score the draft class — pass named DataFrame to avoid feature name warning
    cfb_df = pd.read_csv(cfb_predict_path)
    X = cfb_df.reindex(columns=feature_cols)
    X_imputed = pd.DataFrame(imputer.transform(X), columns=feature_cols)
    cfb_df["cfb_projected_ppr"] = rf.predict(X_imputed)

    # Load the existing NFL predict file to get its schema
    nfl_df = pd.read_csv(nfl_predict_path)

    # Build rookie rows: start with NFL predict file columns, then always
    # add cfb_projected_ppr and is_rookie regardless of whether they exist in nfl_df
    all_cols = list(nfl_df.columns)
    if "cfb_projected_ppr" not in all_cols:
        all_cols.append("cfb_projected_ppr")
    if "is_rookie" not in all_cols:
        all_cols.append("is_rookie")

    rookie_rows = pd.DataFrame(index=cfb_df.index, columns=all_cols, dtype=float)

    if "player_id" in rookie_rows.columns and "nfl_player_id" in cfb_df.columns:
        rookie_rows["player_id"] = cfb_df["nfl_player_id"].values

    # Always write cfb_projected_ppr — this is the whole point
    rookie_rows["cfb_projected_ppr"] = cfb_df["cfb_projected_ppr"].values
    rookie_rows["is_rookie"] = 1

    # Ensure nfl_df also has both columns before concat
    nfl_df["is_rookie"] = 0
    if "cfb_projected_ppr" not in nfl_df.columns:
        nfl_df["cfb_projected_ppr"] = np.nan

    combined = pd.concat([nfl_df, rookie_rows], ignore_index=True)
    combined.to_csv(nfl_predict_path, index=False)

    print(f"  Appended {len(cfb_df)} rookies → {nfl_predict_path} (total rows: {len(combined)})")


if __name__ == "__main__":
    results = {}
    for pos in ["qb", "rb", "wr", "te"]:
        artifact, projections = train_position_model(pos)
        results[pos] = projections

        # Append 2025 draft class into the NFL model's predict file for this position
        print(f"\n  Appending 2025 draft class to NFL predict file: {pos.upper()}")
        append_rookies_to_nfl_predict(pos, artifact)

    # ── Combined projections file (all positions, easy to join into NFL data) ──
    combined = pd.concat(results.values(), ignore_index=True)
    combined_path = os.path.join(OUTPUT_DIR, "cfb_to_nfl_all_projections.csv")
    combined.to_csv(combined_path, index=False)
    print(f"\nCombined projections saved → {combined_path}")
    print(f"Total players projected: {len(combined)}")
