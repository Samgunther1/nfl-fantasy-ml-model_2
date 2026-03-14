import pandas as pd
import streamlit as st

PRED_PATH = "..data/processed/all_predictions_2026.csv"
SCORE_COL = "pred_fp_ppr_2026"
VALID_POS = ["QB", "RB", "WR", "TE"]

st.set_page_config(page_title="Fantasy Draft Assistant", layout="centered")

@st.cache_data
def load_preds(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    needed = ["player_id", "player_display_name", "position", SCORE_COL, "college_flag"]
    for c in needed:
        if c not in df.columns:
            raise ValueError(f"Missing required column: {c}")

    df = df[needed].copy()
    df[SCORE_COL] = pd.to_numeric(df[SCORE_COL], errors="coerce")
    df = df.dropna(subset=[SCORE_COL])
    df["position"] = df["position"].astype(str).str.upper()
    df["player_display_name"] = df["player_display_name"].fillna("Unknown")
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
        "player_name": row.get("player_display_name", "Unknown"),
        "position": row["position"],
        "college_flag": int(row["college_flag"]),
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
        return pd.DataFrame(columns=["rank", "player_id", "player_display_name", "college_flag", SCORE_COL])
    rows = preds_idx.loc[ids].copy()
    if isinstance(rows, pd.Series):
        rows = rows.to_frame().T
    out = rows.reset_index().rename(columns={"index": "player_id"})
    out = out[["player_id", "player_display_name", "college_flag", SCORE_COL]]
    out.insert(0, "rank", range(1, len(out) + 1))
    return out

# ── App ───────────────────────────────────────────────────────────────────────
st.title("🏈 Fantasy Draft Assistant")

preds = load_preds(PRED_PATH)
preds_idx = preds.set_index("player_id")

# Session state init
if "pools" not in st.session_state or "picked" not in st.session_state:
    st.session_state.pools, st.session_state.picked = build_pools(preds)

# Top bar controls
col1, col2 = st.columns([2, 1])
with col1:
    pos = st.selectbox("Choose a position", VALID_POS, index=1)
with col2:
    if st.button("Reset draft"):
        st.session_state.pools, st.session_state.picked = build_pools(preds)
        st.session_state["deny_mode"] = False
        st.success("Draft reset.")

# Remaining counts
remaining = {p: len(st.session_state.pools.get(p, [])) for p in VALID_POS}
st.caption("Remaining: " + " | ".join([f"{p}: {remaining[p]}" for p in VALID_POS]))

rec = recommend_one(pos, st.session_state.pools, preds_idx)

if rec is None:
    st.warning(f"No players left in pool for {pos}.")
    st.stop()

# Recommendation card
st.subheader(f"⭐ Recommended: {rec['player_name']}")
col_a, col_b, col_c = st.columns(3)
col_a.metric("Predicted Weekly PPR", f"{rec['pred']:.2f}")
col_b.metric("Position", rec["position"])
col_c.metric("Player Type", "🎓 College" if rec["college_flag"] == 1 else "🏈 NFL")
st.caption(f"player_id: {rec['player_id']}")

# Accept / Deny controls
c1, c2 = st.columns(2)
with c1:
    if st.button("✅ Accept pick"):
        remove_from_pool(pos, rec["player_id"], st.session_state.pools)
        st.session_state.picked[pos].append(rec["player_id"])
        st.session_state["deny_mode"] = False
        st.success(f"Picked {rec['player_name']}!")
        st.rerun()

with c2:
    if st.button("❌ Deny recommendation"):
        st.session_state["deny_mode"] = True

# Deny flow
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
            st.success(f"Removed {rec['player_name']} from pool.")
            st.rerun()

    else:
        st.info("Player stays in pool. Here are the next 10 suggestions.")
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
                name = preds_idx.loc[chosen, "player_display_name"] if chosen in preds_idx.index else chosen
                st.success(f"Picked {name}!")
                st.rerun()

        if st.button("Back"):
            st.session_state["deny_mode"] = False
            st.rerun()

# Picked players expander
with st.expander("📋 Show my draft picks"):
    any_picked = False
    for p in VALID_POS:
        picked_ids = st.session_state.picked[p]
        if picked_ids:
            any_picked = True
            st.markdown(f"**{p}**")
            picked_rows = preds_idx.loc[
                [pid for pid in picked_ids if pid in preds_idx.index],
                ["player_display_name", SCORE_COL]
            ].reset_index()
            picked_rows.columns = ["player_id", "player_display_name", "pred_fp_ppr_2026"]
            st.dataframe(picked_rows, use_container_width=True, hide_index=True)
    if not any_picked:
        st.info("No picks made yet.")