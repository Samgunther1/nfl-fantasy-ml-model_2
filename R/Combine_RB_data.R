library(tidyverse)

# ── 1. Load raw training sets ─────────────────────────────────────────────────
cfb <- read_csv("data/processed/cfb_to_nfl_rb_modeling.csv")
nfl <- read_csv("data/processed/nfl_to_nfl_rb_train.csv")

cat("CFB rows:", nrow(cfb), "\n")
cat("NFL rows:", nrow(nfl), "\n")

# ── 2. Build draft info lookup from CFB ───────────────────────────────────────
draft_lookup <- cfb %>%
  select(nfl_player_id, overall, round, pick, height, weight,
         pre_draft_ranking, pre_draft_position_ranking, pre_draft_grade) %>%
  distinct(nfl_player_id, .keep_all = TRUE)

# ── 3. Join draft info onto NFL table ─────────────────────────────────────────
nfl_with_draft <- nfl %>%
  left_join(draft_lookup, by = c("player_id" = "nfl_player_id"))

cat("NFL rows total:", nrow(nfl_with_draft), "\n")
cat("Rows with draft info:", sum(!is.na(nfl_with_draft$overall)), "\n")
cat("Rows missing draft info:", sum(is.na(nfl_with_draft$overall)), "\n")

# ── 4. Standardize CFB columns ────────────────────────────────────────────────
cfb_clean <- cfb %>%
  mutate(college_flag = 1) %>%
  select(
    player_id                = nfl_player_id,
    player_display_name      = nfl_name,
    season                   = nfl_rookie_season,
    games,
    overall, round, pick, height, weight,
    pre_draft_ranking, pre_draft_position_ranking, pre_draft_grade,
    carries_pg               = rushing_car_pg,
    rushing_yards_pg         = rushing_yds_pg,
    rushing_tds_pg           = rushing_td_pg,
    receptions_pg            = receiving_rec_pg,
    receiving_yards_pg       = receiving_yds_pg,
    receiving_tds_pg         = receiving_td_pg,
    fumble_recovery_own_pg   = fumbles_rec_pg,
    rushing_fumbles_lost_pg  = fumbles_lost_pg,
    rushing_fumbles_pg       = fumbles_fum_pg,
    target_fp_ppr            = avg_weekly_ppr,
    target_games             = n_weeks,
    college_flag
  )

# ── 5. Standardize NFL columns ────────────────────────────────────────────────
nfl_clean <- nfl_with_draft %>%
  mutate(college_flag = 0) %>%
  select(
    player_id, player_display_name, season, games,
    overall, round, pick, height, weight,
    pre_draft_ranking, pre_draft_position_ranking, pre_draft_grade,
    carries_pg, rushing_yards_pg, rushing_tds_pg,
    receptions_pg, receiving_yards_pg, receiving_tds_pg,
    fumble_recovery_own_pg, rushing_fumbles_lost_pg, rushing_fumbles_pg,
    target_fp_ppr, target_games,
    college_flag
  )

# ── 6. Union ──────────────────────────────────────────────────────────────────
combined <- bind_rows(cfb_clean, nfl_clean)

cat("CFB rows:", nrow(cfb_clean), "\n")
cat("NFL rows:", nrow(nfl_clean), "\n")
cat("Combined rows:", nrow(combined), "\n")
cat("Combined columns:", ncol(combined), "\n")
glimpse(combined)

# ── 7. Check NA rates ─────────────────────────────────────────────────────────
na_summary <- combined %>%
  summarise(across(everything(), ~ round(mean(is.na(.)) * 100, 1))) %>%
  pivot_longer(everything(), names_to = "column", values_to = "pct_missing") %>%
  arrange(desc(pct_missing))

print(na_summary, n = 30)

# ── 8. Impute sparse NAs with 0 ───────────────────────────────────────────────
combined_clean <- combined %>%
  mutate(across(c(fumble_recovery_own_pg, rushing_fumbles_lost_pg,
                  rushing_fumbles_pg, carries_pg, rushing_yards_pg,
                  rushing_tds_pg, receptions_pg, receiving_yards_pg,
                  receiving_tds_pg),
                ~ replace_na(., 0)))

# Verify
combined_clean %>%
  summarise(across(c(fumble_recovery_own_pg, rushing_fumbles_lost_pg,
                     rushing_fumbles_pg, carries_pg, rushing_yards_pg,
                     rushing_tds_pg, receptions_pg, receiving_yards_pg,
                     receiving_tds_pg),
                   ~ sum(is.na(.)))) %>%
  print()

# ── 9. Export ─────────────────────────────────────────────────────────────────
write_csv(combined_clean, "data/processed/rb_combined_training.csv")
cat("Export complete\n")
cat("Final dimensions:", nrow(combined_clean), "rows x", ncol(combined_clean), "columns\n")
