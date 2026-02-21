nfl_stats <- read.csv('data/raw/nfl_player_stats_2011_2025.csv')

library(dplyr)

# Operation 1: create weekly position tables (QB/RB/WR/TE)
nfl_weekly_pos <- nfl_stats %>%
  mutate(
    pos_raw = coalesce(position_group),
    pos_group = case_when(
      pos_raw %in% c("QB") ~ "QB",
      pos_raw %in% c("RB", "FB") ~ "RB",     # optional: fold FB into RB
      pos_raw %in% c("WR") ~ "WR",
      pos_raw %in% c("TE") ~ "TE",
      TRUE ~ NA_character_
    )
  ) %>%
  filter(!is.na(pos_group))

nfl_qb_weekly <- nfl_weekly_pos %>% filter(pos_group == "QB")
nfl_rb_weekly <- nfl_weekly_pos %>% filter(pos_group == "RB")
nfl_wr_weekly <- nfl_weekly_pos %>% filter(pos_group == "WR")
nfl_te_weekly <- nfl_weekly_pos %>% filter(pos_group == "TE")

# sanity counts
bind_rows(
  nfl_qb_weekly %>% summarise(pos="QB", rows=n()),
  nfl_rb_weekly %>% summarise(pos="RB", rows=n()),
  nfl_wr_weekly %>% summarise(pos="WR", rows=n()),
  nfl_te_weekly %>% summarise(pos="TE", rows=n())
)

qb_stat_cols <- c(
  "completions",
  "attempts",
  "passing_yards",
  "passing_tds",
  "passing_interceptions",
  "sacks_suffered",
  "sack_yards_lost",
  "sack_fumbles",
  "sack_fumbles_lost",
  "passing_air_yards",
  "passing_yards_after_catch",
  "passing_first_downs",
  "passing_epa",
  "passing_cpoe",
  "passing_2pt_conversions",
  "pacr",
  "carries",
  "rushing_yards",
  "rushing_tds",
  "rushing_fumbles",
  "rushing_fumbles_lost",
  "rushing_first_downs",
  "rushing_epa",
  "rushing_2pt_conversions",
  "fumble_recovery_own",
  "fumble_recovery_yards_own",
  "penalties",
  "penalty_yards"
)

qb_season_features <- nfl_qb_weekly %>%
  filter(season >= 2011, season <= 2024) %>%
  group_by(player_id, season) %>%
  summarise(
    games = n_distinct(week),
    across(
      all_of(qb_stat_cols),
      ~ mean(.x, na.rm = TRUE),
      .names = "{.col}_pg"
    ),
    .groups = "drop"
  )

# quick check
qb_season_features %>%
  summarise(min_season = min(season), max_season = max(season), rows = n())


# 1) Target table: season-level avg weekly fantasy points (targets exist 2012–2025)
qb_targets_next <- nfl_qb_weekly %>%
  filter(season >= 2012, season <= 2025) %>%
  group_by(player_id, season) %>%
  summarise(
    target_fp_ppr = mean(fantasy_points_ppr, na.rm = TRUE),
    target_games = n_distinct(week),
    .groups = "drop"
  ) %>%
  rename(target_season = season)

# 2) Add join key to features (feature season t predicts target season t+1)
qb_model_df <- qb_season_features %>%
  mutate(target_season = season + 1L) %>%
  left_join(qb_targets_next, by = c("player_id", "target_season"))

# sanity: should only have targets up through 2025, and some NAs if player not in league next year
qb_model_df %>%
  summarise(
    feature_season_min = min(season),
    feature_season_max = max(season),
    target_season_min = min(target_season),
    target_season_max = max(target_season),
    rows = n(),
    rows_with_target = sum(!is.na(target_fp_ppr))
  )

qb_train_df <- qb_model_df %>%
  filter(!is.na(target_fp_ppr))

# quick sanity
qb_train_df %>% summarise(
  rows = n(),
  feature_season_min = min(season),
  feature_season_max = max(season),
  target_season_min = min(target_season),
  target_season_max = max(target_season)
)


# write to disk (adjust paths to match your repo)
write.csv(qb_train_df, "data/processed/nfl_to_nfl_qb_train.csv", row.names = FALSE)



skill_stat_cols <- c(
  "sacks_suffered",
  "sack_yards_lost",
  "sack_fumbles",
  "sack_fumbles_lost",
  "carries",
  "rushing_yards",
  "rushing_tds",
  "rushing_fumbles",
  "rushing_fumbles_lost",
  "rushing_first_downs",
  "rushing_epa",
  "rushing_2pt_conversions",
  "receptions",
  "targets",
  "receiving_yards",
  "receiving_tds",
  "receiving_fumbles",
  "receiving_fumbles_lost",
  "receiving_air_yards",
  "receiving_yards_after_catch",
  "receiving_first_downs",
  "receiving_epa",
  "receiving_2pt_conversions",
  "racr",
  "target_share",
  "air_yards_share",
  "wopr",
  "fumble_recovery_own",
  "fumble_recovery_yards_own",
  "fumble_recovery_tds",
  "penalties",
  "penalty_yards"
)

build_season_features <- function(weekly_df, stat_cols) {
  weekly_df %>%
    filter(season >= 2011, season <= 2024) %>%
    group_by(player_id, season) %>%
    summarise(
      games = n_distinct(week),
      across(
        all_of(stat_cols),
        ~ mean(.x, na.rm = TRUE),
        .names = "{.col}_pg"
      ),
      .groups = "drop"
    )
}

rb_season_features <- build_season_features(nfl_rb_weekly, skill_stat_cols)
wr_season_features <- build_season_features(nfl_wr_weekly, skill_stat_cols)
te_season_features <- build_season_features(nfl_te_weekly, skill_stat_cols)

# quick sanity
bind_rows(
  rb_season_features %>% summarise(pos="RB", min_season=min(season), max_season=max(season), rows=n()),
  wr_season_features %>% summarise(pos="WR", min_season=min(season), max_season=max(season), rows=n()),
  te_season_features %>% summarise(pos="TE", min_season=min(season), max_season=max(season), rows=n())
)

build_targets <- function(weekly_df) {
  weekly_df %>%
    filter(season >= 2012, season <= 2025) %>%
    group_by(player_id, season) %>%
    summarise(
      target_fp_ppr = mean(fantasy_points_ppr, na.rm = TRUE),
      target_games = n_distinct(week),
      .groups = "drop"
    ) %>%
    rename(target_season = season)
}

join_features_targets <- function(season_features, targets_df) {
  season_features %>%
    mutate(target_season = season + 1L) %>%
    left_join(targets_df, by = c("player_id", "target_season"))
}

# targets
rb_targets_next <- build_targets(nfl_rb_weekly)
wr_targets_next <- build_targets(nfl_wr_weekly)
te_targets_next <- build_targets(nfl_te_weekly)

# model dfs (with possible NA targets)
rb_model_df <- join_features_targets(rb_season_features, rb_targets_next)
wr_model_df <- join_features_targets(wr_season_features, wr_targets_next)
te_model_df <- join_features_targets(te_season_features, te_targets_next)

# training dfs (drop missing targets)
rb_train_df <- rb_model_df %>% filter(!is.na(target_fp_ppr))
wr_train_df <- wr_model_df %>% filter(!is.na(target_fp_ppr))
te_train_df <- te_model_df %>% filter(!is.na(target_fp_ppr))

# sanity
bind_rows(
  rb_train_df %>% summarise(pos="RB", rows=n(), feature_min=min(season), feature_max=max(season), target_min=min(target_season), target_max=max(target_season)),
  wr_train_df %>% summarise(pos="WR", rows=n(), feature_min=min(season), feature_max=max(season), target_min=min(target_season), target_max=max(target_season)),
  te_train_df %>% summarise(pos="TE", rows=n(), feature_min=min(season), feature_max=max(season), target_min=min(target_season), target_max=max(target_season))
)



# helper: build a single-season (2025) per-game feature table
build_predict_features_one_season <- function(weekly_df, stat_cols, season_value = 2025L) {
  weekly_df %>%
    filter(season == season_value) %>%
    group_by(player_id, season) %>%
    summarise(
      games = n_distinct(week),
      across(
        all_of(stat_cols),
        ~ mean(.x, na.rm = TRUE),
        .names = "{.col}_pg"
      ),
      .groups = "drop"
    )
}

# QB predict-only uses qb_stat_cols (with fantasy_points_ppr already removed)
qb_predict_2026_df <- build_predict_features_one_season(nfl_qb_weekly, qb_stat_cols, 2025L)

# RB/WR/TE predict-only use the shared skill list
rb_predict_2026_df <- build_predict_features_one_season(nfl_rb_weekly, skill_stat_cols, 2025L)
wr_predict_2026_df <- build_predict_features_one_season(nfl_wr_weekly, skill_stat_cols, 2025L)
te_predict_2026_df <- build_predict_features_one_season(nfl_te_weekly, skill_stat_cols, 2025L)

# sanity counts
bind_rows(
  qb_predict_2026_df %>% summarise(pos="QB", rows=n()),
  rb_predict_2026_df %>% summarise(pos="RB", rows=n()),
  wr_predict_2026_df %>% summarise(pos="WR", rows=n()),
  te_predict_2026_df %>% summarise(pos="TE", rows=n())
)

write.csv(qb_predict_2026_df, "data/processed/nfl_to_nfl_qb_predict_2026.csv", row.names = FALSE)
write.csv(rb_predict_2026_df, "data/processed/nfl_to_nfl_rb_predict_2026.csv", row.names = FALSE)
write.csv(wr_predict_2026_df, "data/processed/nfl_to_nfl_wr_predict_2026.csv", row.names = FALSE)
write.csv(te_predict_2026_df, "data/processed/nfl_to_nfl_te_predict_2026.csv", row.names = FALSE)
