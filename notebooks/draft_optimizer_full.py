"""
Fantasy Football Draft Optimization — FULL SCALE Prototype
===========================================================
Scaled-up version of the toy prototype.

Changes from toy (draft_optimizer.py):
  - 12-team snake draft, 15 picks per team (standard ESPN/Yahoo)
  - Full player pool: top 36 QB / 85 RB / 95 WR / 36 TE + 32 K + 32 D/ST
  - FLEX slot (best remaining RB/WR/TE) and 6 bench slots
  - Roster score = top-N per slot + 0.2 x bench
  - Oracle = Monte-Carlo rollouts (exhaustive search infeasible now)
  - Policy = XGBoost multi-class classifier over position actions

Pipeline mirrors the VRP/CatGPT structure from class:
  generate_instance  ->  MC-rollout oracle  ->  dataset  ->
      XGBoost imitation policy  ->  evaluate vs baselines
"""

import os
import time
import random
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from collections import Counter, defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed

import xgboost as xgb
from sklearn.preprocessing import LabelEncoder

# ------------------------------------------------------------------
# CONFIG
# ------------------------------------------------------------------
RNG_SEED         = 42
PREDICTIONS_CSV  = "data/processed/all_predictions_2026.csv"

N_TEAMS          = 12
N_ROUNDS         = 15
N_PICKS_TOTAL    = N_TEAMS * N_ROUNDS            # 180

POSITIONS        = ["QB", "RB", "WR", "TE", "K", "DST"]
POS_IDX          = {p: i for i, p in enumerate(POSITIONS)}

# Per-position pool sizing (keeps a realistic, draftable universe)
POS_POOL_CAPS    = {"QB": 36, "RB": 85, "WR": 95, "TE": 36, "K": 32, "DST": 32}

# Roster starter slots
STARTER_SLOTS    = {"QB": 1, "RB": 2, "WR": 2, "TE": 1, "K": 1, "DST": 1}
N_FLEX           = 2
FLEX_ELIGIBLE    = {"RB", "WR", "TE"}
N_STARTERS       = sum(STARTER_SLOTS.values()) + N_FLEX          # 9
N_BENCH          = N_ROUNDS - N_STARTERS                          # 6
BENCH_WEIGHT     = 0.2

# Soft positional caps for opponent model
MAX_CAPS         = {"QB": 3, "RB": 7, "WR": 8, "TE": 3, "K": 2, "DST": 2}
KDST_EARLIEST_RD = 10

# Synthetic projection ranges for K and DST
K_RANGE          = (9.0, 12.0)
DST_RANGE        = (6.0, 12.0)

# Per-instance projection noise
PROJ_NOISE_STD   = 1.5

# Oracle settings
N_ROLLOUTS       = 20          # rollouts per candidate position per decision
N_INSTANCES      = 300         # total draft instances to solve
TRAIN_FRAC       = 0.70        # fraction used for imitation-learning training

# Opponent / future-self policy stochasticity
OPP_TOPK         = 3           # sample among top-K by projection

random.seed(RNG_SEED)
np.random.seed(RNG_SEED)


# ==================================================================
# 1) POOL & INSTANCE GENERATION
# ==================================================================

def load_full_pool():
    """Top-K NFL players per position + synthetic K and DST."""
    df  = pd.read_csv(PREDICTIONS_CSV)
    nfl = df[df.college_flag == 0].copy()

    parts = []
    for pos in ("QB", "RB", "WR", "TE"):
        k  = POS_POOL_CAPS[pos]
        tp = nfl[nfl.position == pos].nlargest(k, "pred_fp_ppr_2026")
        parts.append(tp[["player_id", "player_display_name",
                         "position", "pred_fp_ppr_2026"]])

    rng = np.random.default_rng(RNG_SEED)
    kickers = pd.DataFrame({
        "player_id":           [f"K_{i:02d}"   for i in range(POS_POOL_CAPS["K"])],
        "player_display_name": [f"Kicker_{i:02d}"  for i in range(POS_POOL_CAPS["K"])],
        "position":            "K",
        "pred_fp_ppr_2026":    rng.uniform(*K_RANGE, POS_POOL_CAPS["K"]),
    })
    dsts = pd.DataFrame({
        "player_id":           [f"D_{i:02d}"   for i in range(POS_POOL_CAPS["DST"])],
        "player_display_name": [f"Defense_{i:02d}" for i in range(POS_POOL_CAPS["DST"])],
        "position":            "DST",
        "pred_fp_ppr_2026":    rng.uniform(*DST_RANGE, POS_POOL_CAPS["DST"]),
    })
    parts.extend([kickers, dsts])

    pool = pd.concat(parts, ignore_index=True).rename(
        columns={"pred_fp_ppr_2026": "base_proj"})
    return pool.reset_index(drop=True)


def generate_instance(pool, seed):
    """One draft instance: Gaussian-noised projections + random draft slot."""
    rng   = np.random.default_rng(seed)
    noise = rng.normal(0.0, PROJ_NOISE_STD, size=len(pool))
    projs = np.clip(pool["base_proj"].values + noise, 0.0, None)
    pos_idx = np.array([POS_IDX[p] for p in pool["position"].values])
    names = pool["player_display_name"].values
    my_slot = int(rng.integers(0, N_TEAMS))
    return {
        "projs":   projs.astype(np.float32),      # (N,)
        "pos_idx": pos_idx.astype(np.int8),       # (N,)
        "names":   names,
        "my_slot": my_slot,
        "seed":    seed,
    }


def snake_pick_order(n_teams=N_TEAMS, n_rounds=N_ROUNDS):
    out = []
    for r in range(n_rounds):
        out.extend(list(range(n_teams)) if r % 2 == 0
                   else list(range(n_teams - 1, -1, -1)))
    return np.array(out, dtype=np.int8)


PICK_ORDER = snake_pick_order()


# ==================================================================
# 2) ROSTER SCORING  (top-N per slot + 0.2 * bench)
# ==================================================================

def score_roster_from_picks(pick_indices, projs, pos_idx):
    """Compute Top-N-per-slot + bench-weighted score for a roster."""
    by_pos = [[] for _ in POSITIONS]
    for idx in pick_indices:
        by_pos[int(pos_idx[idx])].append(float(projs[idx]))
    for lst in by_pos:
        lst.sort(reverse=True)

    starter = 0.0
    used = [0] * len(POSITIONS)

    # Fixed slots
    for pos_name, n in STARTER_SLOTS.items():
        p = POS_IDX[pos_name]
        for _ in range(n):
            if used[p] < len(by_pos[p]):
                starter += by_pos[p][used[p]]
                used[p] += 1

    # Flex: best remaining RB/WR/TE
    best_val, best_p = -1.0, None
    for pos_name in FLEX_ELIGIBLE:
        p = POS_IDX[pos_name]
        if used[p] < len(by_pos[p]) and by_pos[p][used[p]] > best_val:
            best_val = by_pos[p][used[p]]
            best_p   = p
    if best_p is not None:
        starter += best_val
        used[best_p] += 1

    # Bench = everything else
    bench = 0.0
    for p in range(len(POSITIONS)):
        bench += sum(by_pos[p][used[p]:])

    return starter + BENCH_WEIGHT * bench


# ==================================================================
# 3) PICK POLICIES  (opponent / future-self)
# ==================================================================

def _pick_weighted_topk(scores, topk=OPP_TOPK, rng=None):
    """Sample one index from the top-k of `scores`, weighted by score."""
    if rng is None:
        rng = np.random
    k = min(topk, len(scores))
    top_ids = np.argpartition(-scores, k - 1)[:k]
    top_ids = top_ids[np.argsort(-scores[top_ids])]
    w = scores[top_ids] - scores[top_ids].min() + 0.1
    p = w / w.sum()
    return int(rng.choice(top_ids, p=p))


def needs_aware_pick(available_mask, team_counts, round_idx, projs, pos_idx, rng=None):
    """Realistic opponent / future-self pick.

    Rules:
      - Enforce soft positional caps (MAX_CAPS).
      - No K or DST before KDST_EARLIEST_RD unless the team has no other valid pick.
      - Among eligible players, sample weighted from the top-K by projection.
    """
    if rng is None:
        rng = np.random

    # Identify capped and early-round-banned positions
    banned = np.zeros(len(POSITIONS), dtype=bool)
    for pos_name, cap in MAX_CAPS.items():
        if team_counts[pos_name] >= cap:
            banned[POS_IDX[pos_name]] = True
    if round_idx < KDST_EARLIEST_RD:
        banned[POS_IDX["K"]] = True
        banned[POS_IDX["DST"]] = True

    eligible = available_mask.copy()
    for p in np.where(banned)[0]:
        eligible &= (pos_idx != p)

    # Fallback if no eligible (rare): relax K/DST early-round ban
    if not eligible.any():
        eligible = available_mask.copy()
        for pos_name, cap in MAX_CAPS.items():
            if team_counts[pos_name] >= cap:
                eligible &= (pos_idx != POS_IDX[pos_name])
        if not eligible.any():
            eligible = available_mask.copy()

    valid_ids = np.where(eligible)[0]
    scores    = projs[valid_ids]
    choice    = _pick_weighted_topk(scores, OPP_TOPK, rng)
    return int(valid_ids[choice])


def best_at_position(available_mask, pos_idx, projs, target_pos):
    """Deterministic: return index of highest-projected available player at target_pos."""
    mask = available_mask & (pos_idx == target_pos)
    if not mask.any():
        return -1
    ids = np.where(mask)[0]
    return int(ids[np.argmax(projs[ids])])


# ==================================================================
# 4) FULL DRAFT SIMULATION
# ==================================================================

def simulate_from(inst, my_action_fn, start_pick=0, init_state=None, rng=None):
    """
    Simulate a draft starting from pick `start_pick`.
      my_action_fn(state) -> position_name (string) for each of my turns.
      init_state: optional dict with pre-computed state (for resuming from mid-draft).

    Returns:
      team_rosters: {team_idx -> list of picked player indices}
      my_actions:   list of (pick_idx_global, position_chosen) that *I* made.
    """
    if rng is None:
        rng = np.random.default_rng()

    projs   = inst["projs"]
    pos_idx = inst["pos_idx"]
    my_slot = inst["my_slot"]

    if init_state is None:
        N = len(projs)
        available_mask = np.ones(N, dtype=bool)
        team_rosters   = {t: [] for t in range(N_TEAMS)}
        team_counts    = {t: {p: 0 for p in POSITIONS} for t in range(N_TEAMS)}
    else:
        available_mask = init_state["available"].copy()
        team_rosters   = {t: list(r) for t, r in init_state["rosters"].items()}
        team_counts    = {t: dict(c) for t, c in init_state["counts"].items()}

    my_actions = []

    for pick_i in range(start_pick, N_PICKS_TOTAL):
        team      = int(PICK_ORDER[pick_i])
        round_idx = pick_i // N_TEAMS

        if team == my_slot:
            # My turn: use provided action function
            state = {
                "inst":          inst,
                "available":     available_mask,
                "rosters":       team_rosters,
                "counts":        team_counts,
                "pick_i":        pick_i,
                "round_idx":     round_idx,
                "my_roster":     team_rosters[my_slot],
                "my_counts":     team_counts[my_slot],
            }
            chosen_pos = my_action_fn(state)
            pick_idx   = best_at_position(available_mask,
                                          pos_idx, projs,
                                          POS_IDX[chosen_pos])
            if pick_idx < 0:                              # fallback: any available
                pick_idx = needs_aware_pick(available_mask, team_counts[team],
                                            round_idx, projs, pos_idx, rng=rng)
            my_actions.append((pick_i, chosen_pos))
        else:
            pick_idx = needs_aware_pick(available_mask, team_counts[team],
                                        round_idx, projs, pos_idx, rng=rng)

        available_mask[pick_idx] = False
        team_rosters[team].append(pick_idx)
        team_counts[team][POSITIONS[int(pos_idx[pick_idx])]] += 1

    return team_rosters, my_actions


def _future_self_random(rng):
    """
    Action function for 'my' future turns during rollouts:
    use the same needs-aware logic as opponents.
    """
    def _fn(state):
        pick_idx = needs_aware_pick(state["available"], state["my_counts"],
                                    state["round_idx"], state["inst"]["projs"],
                                    state["inst"]["pos_idx"], rng=rng)
        return POSITIONS[int(state["inst"]["pos_idx"][pick_idx])]
    return _fn


# ==================================================================
# 5) MC-ROLLOUT ORACLE
# ==================================================================

def mc_oracle_action(inst, available_mask, team_rosters, team_counts,
                     pick_i, n_rollouts=N_ROLLOUTS, rng=None):
    """
    At a given decision state (my turn), evaluate each position candidate
    by running `n_rollouts` simulations forward and averaging my final score.

    Returns the best position (string) and its avg score.
    """
    if rng is None:
        rng = np.random.default_rng()
    projs   = inst["projs"]
    pos_idx = inst["pos_idx"]
    my_slot = inst["my_slot"]
    round_idx = pick_i // N_TEAMS

    # Candidate positions: those with at least one available player
    # and that aren't silly for current round (don't consider K/DST early)
    candidates = []
    for pos in POSITIONS:
        p = POS_IDX[pos]
        if not (available_mask & (pos_idx == p)).any():
            continue
        # Skip K/DST extremely early (rounds 0-7); they'll always be suboptimal
        if pos in ("K", "DST") and round_idx < 8:
            continue
        candidates.append(pos)

    if not candidates:
        return "RB", 0.0   # should not happen

    best_pos, best_avg = None, -np.inf
    for cand_pos in candidates:
        p = POS_IDX[cand_pos]
        # Deterministic immediate pick at this candidate position
        hypothetical_pick = best_at_position(available_mask, pos_idx, projs, p)
        if hypothetical_pick < 0:
            continue

        # Base state AFTER taking this candidate
        base_available = available_mask.copy()
        base_available[hypothetical_pick] = False
        base_rosters   = {t: list(r) for t, r in team_rosters.items()}
        base_counts    = {t: dict(c) for t, c in team_counts.items()}
        base_rosters[my_slot].append(hypothetical_pick)
        base_counts[my_slot][cand_pos] += 1

        rollout_scores = []
        for _ in range(n_rollouts):
            init_state = {"available": base_available,
                          "rosters":   base_rosters,
                          "counts":    base_counts}
            rosters, _ = simulate_from(
                inst,
                my_action_fn=_future_self_random(rng),
                start_pick=pick_i + 1,
                init_state=init_state,
                rng=rng,
            )
            s = score_roster_from_picks(rosters[my_slot], projs, pos_idx)
            rollout_scores.append(s)

        avg = float(np.mean(rollout_scores))
        if avg > best_avg:
            best_avg = avg
            best_pos = cand_pos

    return best_pos, best_avg


# ==================================================================
# 6) FEATURE ENGINEERING
# ==================================================================

def build_features(inst, available_mask, my_counts, opp_counts_list, pick_i):
    """Feature vector describing a decision state."""
    projs   = inst["projs"]
    pos_idx = inst["pos_idx"]
    round_idx = pick_i // N_TEAMS
    slot_in_round = pick_i % N_TEAMS

    # Distance to my next pick in a snake draft
    picks_until_next = 0
    for j in range(pick_i + 1, min(pick_i + 2 * N_TEAMS + 1, N_PICKS_TOTAL)):
        picks_until_next += 1
        if PICK_ORDER[j] == inst["my_slot"]:
            break

    feats = [float(round_idx), float(slot_in_round), float(picks_until_next)]

    # Position counts on my roster
    feats.extend([float(my_counts[p]) for p in POSITIONS])

    # Unmet starter needs
    for p in POSITIONS:
        req = STARTER_SLOTS.get(p, 0)
        feats.append(float(max(0, req - my_counts[p])))

    # Best available projection by position (and 2nd-best for drop-off signal)
    for p in POSITIONS:
        mask = available_mask & (pos_idx == POS_IDX[p])
        vals = projs[mask]
        if len(vals) == 0:
            feats.extend([0.0, 0.0])
        elif len(vals) == 1:
            feats.extend([float(vals.max()), 0.0])
        else:
            top2 = np.partition(vals, -2)[-2:]
            feats.extend([float(top2.max()), float(top2.min())])

    # Opponents' mean roster composition
    for p in POSITIONS:
        vals = [c[p] for c in opp_counts_list]
        feats.append(float(np.mean(vals)) if vals else 0.0)

    return np.array(feats, dtype=np.float32)


FEATURE_NAMES = (
    ["round", "slot_in_round", "picks_until_next"]
    + [f"my_cnt_{p}"   for p in POSITIONS]
    + [f"need_{p}"     for p in POSITIONS]
    + [x for p in POSITIONS for x in (f"best_{p}", f"second_{p}")]
    + [f"opp_avg_{p}"  for p in POSITIONS]
)


# ==================================================================
# 7) SOLVE ONE INSTANCE WITH THE ORACLE  (parallel-friendly)
# ==================================================================

def draft_rng_for(inst):
    """Deterministic 'real draft' RNG seeded per instance.
    Used by both the oracle's realized draft AND all baseline policies,
    so opponent randomness is held constant across policy comparisons."""
    return np.random.default_rng(inst["seed"] * 7919 + 1)


def solve_instance(inst, n_rollouts=N_ROLLOUTS):
    """
    Drive an instance to completion, using the MC oracle at each of my turns.
    Collect (features, oracle_action) pairs for training.

    Uses two independent RNG streams:
      - draft_rng: realized opponent picks (same stream used by baselines)
      - rollout_rng: counterfactual rollouts inside the oracle
    """
    draft_rng   = draft_rng_for(inst)
    rollout_rng = np.random.default_rng(inst["seed"] * 1237 + 3)

    projs   = inst["projs"]
    pos_idx = inst["pos_idx"]
    my_slot = inst["my_slot"]

    N = len(projs)
    available_mask = np.ones(N, dtype=bool)
    team_rosters   = {t: [] for t in range(N_TEAMS)}
    team_counts    = {t: {p: 0 for p in POSITIONS} for t in range(N_TEAMS)}

    feats_list, actions_list = [], []
    my_picks = []

    for pick_i in range(N_PICKS_TOTAL):
        team = int(PICK_ORDER[pick_i])
        round_idx = pick_i // N_TEAMS

        if team == my_slot:
            opp_counts_list = [team_counts[t] for t in range(N_TEAMS) if t != my_slot]
            f = build_features(inst, available_mask,
                               team_counts[my_slot], opp_counts_list, pick_i)
            action, _ = mc_oracle_action(inst, available_mask,
                                         team_rosters, team_counts,
                                         pick_i, n_rollouts=n_rollouts,
                                         rng=rollout_rng)
            feats_list.append(f)
            actions_list.append(action)

            pick_idx = best_at_position(available_mask, pos_idx, projs,
                                        POS_IDX[action])
            if pick_idx < 0:
                pick_idx = needs_aware_pick(available_mask, team_counts[team],
                                            round_idx, projs, pos_idx, rng=draft_rng)
            my_picks.append((pick_i, pick_idx, action))
        else:
            pick_idx = needs_aware_pick(available_mask, team_counts[team],
                                        round_idx, projs, pos_idx, rng=draft_rng)

        available_mask[pick_idx] = False
        team_rosters[team].append(pick_idx)
        team_counts[team][POSITIONS[int(pos_idx[pick_idx])]] += 1

    final_score = score_roster_from_picks(team_rosters[my_slot], projs, pos_idx)
    action_seq  = tuple(a for _, _, a in my_picks)

    return {
        "instance":      inst,
        "features":      np.vstack(feats_list),           # (15, n_features)
        "actions":       actions_list,                    # length 15
        "oracle_score":  float(final_score),
        "action_seq":    action_seq,
        "my_picks":      my_picks,
        "final_rosters": team_rosters,
    }


def _solve_instance_worker(args):
    pool_df, seed, n_rollouts = args
    # Rebuild instance inside worker (pools are small)
    inst = generate_instance(pool_df, seed)
    return solve_instance(inst, n_rollouts=n_rollouts)


# ==================================================================
# 8) BASELINES
# ==================================================================

def policy_greedy_bpa(inst, rng=None):
    """Best-Player-Available (respects max caps but otherwise just takes top proj)."""
    rng = rng or np.random.default_rng()

    def _fn(state):
        pi = state["inst"]["pos_idx"]
        pr = state["inst"]["projs"]
        ri = state["round_idx"]
        mask = state["available"].copy()
        # Same caps/K-DST rules as opponents
        for pos_name, cap in MAX_CAPS.items():
            if state["my_counts"][pos_name] >= cap:
                mask &= (pi != POS_IDX[pos_name])
        if ri < KDST_EARLIEST_RD:
            mask &= (pi != POS_IDX["K"]); mask &= (pi != POS_IDX["DST"])
        if not mask.any():
            mask = state["available"]
        ids = np.where(mask)[0]
        best = int(ids[np.argmax(pr[ids])])
        return POSITIONS[int(pi[best])]
    rosters, _ = simulate_from(inst, _fn, rng=rng)
    return score_roster_from_picks(rosters[inst["my_slot"]],
                                   inst["projs"], inst["pos_idx"]), rosters


def policy_fixed_template(template):
    """Try to follow a fixed positional template, fall back to greedy if needed."""
    def _run(inst, rng=None):
        rng = rng or np.random.default_rng()
        template_iter = iter(template)

        def _fn(state):
            try:
                tgt = next(template_iter)
            except StopIteration:
                tgt = POSITIONS[0]
            pi, pr = state["inst"]["pos_idx"], state["inst"]["projs"]
            mask = state["available"] & (pi == POS_IDX[tgt])
            if not mask.any():
                # fallback: greedy needs-aware
                pick_idx = needs_aware_pick(state["available"], state["my_counts"],
                                            state["round_idx"], pr, pi, rng=rng)
                return POSITIONS[int(pi[pick_idx])]
            return tgt

        rosters, _ = simulate_from(inst, _fn, rng=rng)
        return score_roster_from_picks(rosters[inst["my_slot"]],
                                       inst["projs"], inst["pos_idx"]), rosters
    return _run


def policy_xgb(model, le):
    """Rollout the trained XGBoost policy."""
    def _run(inst, rng=None):
        rng = rng or np.random.default_rng()

        def _fn(state):
            opp = [state["counts"][t] for t in range(N_TEAMS) if t != state["inst"]["my_slot"]]
            f   = build_features(state["inst"], state["available"],
                                 state["my_counts"], opp, state["pick_i"])
            probs = model.predict_proba(f.reshape(1, -1))[0]
            # Rank positions by model preference
            order = np.argsort(-probs)
            pi, pr = state["inst"]["pos_idx"], state["inst"]["projs"]
            for cls_idx in order:
                pos_name = le.inverse_transform([cls_idx])[0]
                if (state["available"] & (pi == POS_IDX[pos_name])).any():
                    return pos_name
            # hard fallback
            return POSITIONS[0]

        rosters, _ = simulate_from(inst, _fn, rng=rng)
        return score_roster_from_picks(rosters[inst["my_slot"]],
                                       inst["projs"], inst["pos_idx"]), rosters
    return _run


# ==================================================================
# 9) MAIN
# ==================================================================

def main():
    t0 = time.time()
    out_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                           "plots_full")
    os.makedirs(out_dir, exist_ok=True)

    # --- Load pool ---
    pool = load_full_pool()
    print(f"Pool loaded: {len(pool)} players")
    print(pool.groupby("position").size().to_string())

    # --- Solve instances with MC oracle (parallel) ---
    print(f"\nSolving {N_INSTANCES} instances with MC oracle "
          f"(N_ROLLOUTS={N_ROLLOUTS})...")
    seeds = list(range(1000, 1000 + N_INSTANCES))

    n_workers = min(os.cpu_count() or 2, 6)
    print(f"  using {n_workers} parallel workers")
    args = [(pool, s, N_ROLLOUTS) for s in seeds]

    records = []
    with ProcessPoolExecutor(max_workers=n_workers) as ex:
        futures = [ex.submit(_solve_instance_worker, a) for a in args]
        for i, fut in enumerate(as_completed(futures), 1):
            records.append(fut.result())
            if i % 10 == 0 or i == N_INSTANCES:
                el = time.time() - t0
                rate = i / el
                eta = (N_INSTANCES - i) / rate if rate > 0 else 0
                print(f"  {i}/{N_INSTANCES}  | elapsed {el:6.1f}s  | "
                      f"rate {rate:.2f}/s | eta {eta:5.1f}s")

    # Keep original seed order so splits are reproducible
    records.sort(key=lambda r: r["instance"]["seed"])

    # --- Build imitation-training dataset ---
    n_train = int(TRAIN_FRAC * len(records))
    train_recs, val_recs = records[:n_train], records[n_train:]

    X_train = np.vstack([r["features"] for r in train_recs])
    y_train = np.concatenate([r["actions"] for r in train_recs])
    X_val   = np.vstack([r["features"] for r in val_recs])
    y_val   = np.concatenate([r["actions"] for r in val_recs])
    print(f"\nTraining examples: {len(X_train)}  |  val: {len(X_val)}")

    # --- Train XGBoost multiclass policy ---
    le = LabelEncoder().fit(POSITIONS)
    y_train_enc = le.transform(y_train)
    y_val_enc   = le.transform(y_val)

    clf = xgb.XGBClassifier(
        n_estimators=300, max_depth=5, learning_rate=0.08,
        subsample=0.85, colsample_bytree=0.85,
        objective="multi:softprob", num_class=len(POSITIONS),
        tree_method="hist", n_jobs=4,
        random_state=RNG_SEED,
    )
    clf.fit(X_train, y_train_enc, eval_set=[(X_val, y_val_enc)], verbose=False)
    train_acc = clf.score(X_train, y_train_enc)
    val_acc   = clf.score(X_val,   y_val_enc)
    print(f"XGBoost policy: train acc={train_acc:.3f}  val acc={val_acc:.3f}")

    # --- Evaluate policies on the held-out validation set ---
    # (Re-uses val_recs' oracle scores; no need to re-solve the oracle.)
    eval_records = val_recs
    print(f"\nEvaluating policies on {len(eval_records)} held-out instances "
          f"vs MC-oracle scores:")
    # All policies use the SAME draft_rng per instance, so opponent
    # randomness is held constant for a fair apples-to-apples comparison.
    policies = {
        "XGBoost Policy":   policy_xgb(clf, le),
        "Greedy BPA":       policy_greedy_bpa,
        "Fixed RB-heavy":   policy_fixed_template(
            ("RB","RB","WR","WR","RB","WR","TE","QB","WR","RB","TE","K","DST","QB","RB")),
        "Fixed WR-heavy":   policy_fixed_template(
            ("WR","WR","RB","RB","WR","TE","RB","QB","WR","RB","TE","K","DST","QB","WR")),
        "Fixed Balanced":   policy_fixed_template(
            ("RB","WR","RB","WR","TE","QB","WR","RB","WR","RB","TE","K","DST","QB","RB")),
    }

    results = {"MC Oracle": {"scores": np.array([r["oracle_score"] for r in eval_records]),
                             "gaps":   np.zeros(len(eval_records))}}
    oracle_scores = results["MC Oracle"]["scores"]
    print(f"  {'MC Oracle':<18} avg pts = {oracle_scores.mean():6.2f}  "
          f"(reference)")

    for name, fn in policies.items():
        scores = []
        for rec in eval_records:
            inst = rec["instance"]
            # Fresh draft_rng seeded identically to the oracle's realized draft
            s, _ = fn(inst, rng=draft_rng_for(inst))
            scores.append(s)
        scores = np.array(scores)
        gaps = (oracle_scores - scores) / oracle_scores * 100.0
        # NOTE: gaps can be slightly negative when the policy beats the
        # noisy MC-oracle estimate on a given instance; we keep the raw sign.
        print(f"  {name:<18} avg pts = {scores.mean():6.2f}  "
              f"avg gap = {gaps.mean():+5.2f}%  "
              f"median gap = {np.median(gaps):+5.2f}%  "
              f"max gap = {gaps.max():+5.2f}%")
        results[name] = {"scores": scores, "gaps": gaps}

    # --- Plots ---
    # Position-by-round heatmap from ORACLE decisions (training set)
    plot_position_by_round(records, os.path.join(out_dir, "position_by_round.png"))
    plot_gap_distribution(results, os.path.join(out_dir, "gap_distribution.png"))
    plot_feature_importance(clf, os.path.join(out_dir, "feature_importance.png"))
    plot_policy_comparison(results, os.path.join(out_dir, "policy_comparison.png"))
    plot_example_draft(eval_records[0], os.path.join(out_dir, "example_draft.png"))

    print(f"\nTotal runtime: {time.time() - t0:.1f}s")
    print(f"Plots written to: {out_dir}")
    return clf, le, records, eval_records, results


# ==================================================================
# 10) PLOTS
# ==================================================================

def plot_position_by_round(records, out_path):
    counts = np.zeros((len(POSITIONS), N_ROUNDS))
    for rec in records:
        for r, pos in enumerate(rec["action_seq"]):
            counts[POS_IDX[pos], r] += 1
    probs = counts / counts.sum(axis=0, keepdims=True)

    fig, ax = plt.subplots(figsize=(11, 4.5))
    im = ax.imshow(probs, aspect="auto", cmap="Blues", vmin=0, vmax=1)
    ax.set_xticks(range(N_ROUNDS))
    ax.set_xticklabels([f"R{r+1}" for r in range(N_ROUNDS)])
    ax.set_yticks(range(len(POSITIONS))); ax.set_yticklabels(POSITIONS)
    for i in range(len(POSITIONS)):
        for j in range(N_ROUNDS):
            if probs[i, j] > 0.02:
                ax.text(j, i, f"{probs[i,j]:.2f}", ha="center", va="center",
                        color="white" if probs[i, j] > 0.5 else "black", fontsize=7.5)
    ax.set_title("P(oracle position | round) across instances  (12-team, 15 picks)")
    plt.colorbar(im, ax=ax, label="probability")
    plt.tight_layout(); plt.savefig(out_path, dpi=140); plt.close(fig)


def plot_gap_distribution(results, out_path):
    fig, ax = plt.subplots(figsize=(10, 5))
    items = [(n, d) for n, d in results.items() if n != "MC Oracle"]
    max_g = max(d["gaps"].max() for _, d in items) + 0.5
    bins = np.linspace(0, max_g, 30)
    for name, d in items:
        ax.hist(d["gaps"], bins=bins, alpha=0.55,
                label=f"{name} (mean={d['gaps'].mean():.2f}%)")
    ax.set_xlabel("Optimality gap (%) vs. MC-rollout oracle  -  lower is better")
    ax.set_ylabel("Count of eval instances")
    ax.set_title("Policy quality on fresh eval set")
    ax.legend()
    plt.tight_layout(); plt.savefig(out_path, dpi=140); plt.close(fig)


def plot_feature_importance(clf, out_path, top_n=15):
    imp = clf.feature_importances_
    order = np.argsort(imp)[::-1][:top_n]
    names = [FEATURE_NAMES[i] for i in order]
    vals  = imp[order]
    fig, ax = plt.subplots(figsize=(9, 5))
    ax.barh(range(len(order)), vals, color="#2c7fb8")
    ax.set_yticks(range(len(order))); ax.set_yticklabels(names, fontsize=9)
    ax.invert_yaxis()
    ax.set_xlabel("Feature importance (XGBoost gain)")
    ax.set_title(f"Top {top_n} features driving the policy")
    plt.tight_layout(); plt.savefig(out_path, dpi=140); plt.close(fig)


def plot_policy_comparison(results, out_path):
    names  = [n for n in results.keys() if n != "MC Oracle"]
    means  = [results[n]["gaps"].mean() for n in names]
    stds   = [results[n]["gaps"].std()  for n in names]
    order  = np.argsort(means)
    names  = [names[i]  for i in order]
    means  = [means[i]  for i in order]
    stds   = [stds[i]   for i in order]

    fig, ax = plt.subplots(figsize=(9, 4.5))
    colors = ["#2ca02c" if n == "XGBoost Policy" else "#7f7f7f" for n in names]
    ax.barh(range(len(names)), means, xerr=stds, color=colors,
            error_kw=dict(ecolor="black", capsize=3))
    ax.set_yticks(range(len(names))); ax.set_yticklabels(names)
    ax.invert_yaxis()
    ax.set_xlabel("Avg optimality gap (%)  -  lower is better")
    ax.set_title("Policy comparison  (XGBoost policy vs baselines)")
    for i, m in enumerate(means):
        ax.text(m + 0.05, i, f"{m:.2f}%", va="center", fontsize=9)
    plt.tight_layout(); plt.savefig(out_path, dpi=140); plt.close(fig)


def plot_example_draft(rec, out_path):
    inst = rec["instance"]
    rosters = rec["final_rosters"]
    my_slot = inst["my_slot"]
    projs, pos_idx, names = inst["projs"], inst["pos_idx"], inst["names"]

    fig, ax = plt.subplots(figsize=(13, 5.5))
    colors = {"QB": "#d62728", "RB": "#1f77b4", "WR": "#2ca02c",
              "TE": "#9467bd", "K": "#e377c2", "DST": "#8c564b"}

    for rnd in range(N_ROUNDS):
        for team in range(N_TEAMS):
            if rnd >= len(rosters[team]):
                continue
            idx = rosters[team][rnd]
            pos = POSITIONS[int(pos_idx[idx])]
            is_mine = (team == my_slot)
            ax.barh(team, 1, left=rnd, color=colors[pos],
                    edgecolor="black" if is_mine else "white",
                    linewidth=2.0 if is_mine else 0.3, alpha=0.9)
            nm = str(names[idx])
            short = nm.split()[-1][:8] if " " in nm else nm[:8]
            ax.text(rnd + 0.5, team, f"{short}\n{pos} {projs[idx]:.1f}",
                    ha="center", va="center",
                    fontsize=5.8, fontweight="bold" if is_mine else "normal")

    ax.set_xlim(0, N_ROUNDS); ax.set_xticks(np.arange(0.5, N_ROUNDS, 1))
    ax.set_xticklabels([f"R{r+1}" for r in range(N_ROUNDS)])
    ax.set_yticks(range(N_TEAMS))
    ax.set_yticklabels([f"T{t}{' (me)' if t == my_slot else ''}" for t in range(N_TEAMS)])
    ax.invert_yaxis()
    my_score = rec["oracle_score"]
    ax.set_title(f"Oracle-solved draft  (my slot={my_slot}, my score={my_score:.1f})\n"
                 f"strategy: {' -> '.join(rec['action_seq'])}", fontsize=9)
    handles = [plt.Rectangle((0, 0), 1, 1, color=c) for c in colors.values()]
    ax.legend(handles, colors.keys(), loc="upper right",
              bbox_to_anchor=(1.11, 1.0), fontsize=8)
    plt.tight_layout(); plt.savefig(out_path, dpi=140, bbox_inches="tight")
    plt.close(fig)


if __name__ == "__main__":
    main()
