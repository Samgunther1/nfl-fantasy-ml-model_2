import pandas as pd
import numpy as np
import streamlit as st
from pathlib import Path

PRED_PATH = "data/processed/nfl_to_nfl_all_predictions_2026_named.csv"
SCORE_COL = "pred_fp_ppr_2026"
VALID_POS = ["QB", "RB", "WR", "TE"]

st.set_page_config(page_title="Draft Pick Assistant", layout="centered")

@st.cache_data
def load_preds(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    needed = ["player_id", "position", SCORE_COL]
    for c in needed:
        if c not in df.columns:
            raise ValueError(f"Missing required column: {c}")

    if "player_name" not in df.columns:
        df["player_name"] = ""

    df = df[["player_id", "player_name", "position", SCORE_COL]].copy()
    df[SCORE_COL] = pd.to_numeric(df[SCORE_COL], errors="coerce")
    df = df.dropna(subset=[SCORE_COL])
    df["position"] = df["position"].astype(str).str.upper()

    df = df.sort_values(["position", SCORE_COL], ascending=[True, False]).reset_index(drop=True)
    return df

def build_pools(preds: pd.DataFrame):
    pools = {
        pos: preds.loc[preds["position"] == pos, "player_id"].tolist()
        for pos in VALID_POS
    }
    picked = {pos: [] for pos in VALID_POS}
    return pools, picked

def recommend_one(pos: str, pools, preds_idx):
    ids = pools.get(pos, [])
    if not ids:
        return None
    pid = ids[0]
    row = preds_idx.loc[pid]
    return {
        "player_id": pid,
        "player_name": row.get("player_name", ""),
        "position": row["position"],
        "pred": float(row[SCORE_COL]),
    }

def remove_from_pool(pos: str, player_id: str, pools) -> bool:
    try:
        pools[pos].remove(player_id)
        return True
    except ValueError:
        return False

def get_top_n(pos: str, pools, preds_idx, n=10) -> pd.DataFrame:
    ids = pools.get(pos, [])[:n]
    if not ids:
        return pd.DataFrame(columns=["rank", "player_id", "player_name", "pred_fp_ppr_2026"])
    rows = preds_idx.loc[ids].copy()
    if isinstance(rows, pd.Series):
        rows = rows.to_frame().T
    out = rows.reset_index().rename(columns={"index": "player_id"})
    out = out[["player_id", "player_name", SCORE_COL]].rename(columns={SCORE_COL: "pred_fp_ppr_2026"})
    out.insert(0, "rank", range(1, len(out) + 1))
    return out

# ---------- App ----------
st.title("🏈 Draft Pick Assistant (Prototype)")

preds = load_preds(PRED_PATH)
preds_idx = preds.set_index("player_id")

# session state init
if "pools" not in st.session_state or "picked" not in st.session_state:
    st.session_state.pools, st.session_state.picked = build_pools(preds)

# top bar controls
col1, col2 = st.columns([2, 1])
with col1:
    pos = st.selectbox("Choose a position", VALID_POS, index=1)
with col2:
    if st.button("Reset draft"):
        st.session_state.pools, st.session_state.picked = build_pools(preds)
        st.success("Draft reset.")

# status
remaining = {p: len(st.session_state.pools.get(p, [])) for p in VALID_POS}
st.caption("Remaining: " + " | ".join([f"{p}: {remaining[p]}" for p in VALID_POS]))

rec = recommend_one(pos, st.session_state.pools, preds_idx)

if rec is None:
    st.warning(f"No players left in pool for {pos}.")
    st.stop()

st.subheader(f"Recommendation: {rec['player_name'] or '(name unavailable)'}")
st.write(f"**Predicted PPR:** {rec['pred']:.2f}")
st.code(f"player_id: {rec['player_id']}")

# accept / deny controls
c1, c2 = st.columns(2)
with c1:
    if st.button("✅ Accept pick"):
        remove_from_pool(pos, rec["player_id"], st.session_state.pools)
        st.session_state.picked[pos].append(rec["player_id"])
        st.success("Pick accepted and removed from pool. Get next recommendation above.")
        st.rerun()

with c2:
    deny = st.button("❌ Deny recommendation")

if deny:
    st.session_state["deny_mode"] = True

# deny flow
if st.session_state.get("deny_mode", False):
    st.markdown("### Why deny?")
    reason = st.radio(
        "Select one:",
        ["Player already picked", "I disagree with this pick"],
        index=0
    )

    if reason == "Player already picked":
        if st.button("Confirm: remove from pool and recommend next"):
            remove_from_pool(pos, rec["player_id"], st.session_state.pools)
            st.session_state.picked[pos].append(rec["player_id"])
            st.session_state["deny_mode"] = False
            st.success("Removed from pool. Showing next recommendation.")
            st.rerun()

    else:
        st.info("Player stays in the pool. Here are the next 10 suggestions.")
        top10 = get_top_n(pos, st.session_state.pools, preds_idx, n=10)
        st.dataframe(top10, use_container_width=True, hide_index=True)

        chosen = st.text_input("To accept one of these, paste a player_id here:")
        if st.button("Accept chosen player_id"):
            chosen = chosen.strip()
            if chosen == "":
                st.warning("Enter a player_id.")
            elif chosen not in st.session_state.pools.get(pos, []):
                st.error("That player_id is not currently available in this position pool.")
            else:
                remove_from_pool(pos, chosen, st.session_state.pools)
                st.session_state.picked[pos].append(chosen)
                st.session_state["deny_mode"] = False
                st.success("Chosen player accepted and removed from pool.")
                st.rerun()

        if st.button("Back"):
            st.session_state["deny_mode"] = False
            st.rerun()

# optional: show picked list
with st.expander("Show picked players (IDs)"):
    st.json(st.session_state.picked)