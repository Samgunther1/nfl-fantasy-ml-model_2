library(tidyverse)

# ── 1. Load raw prediction sets ───────────────────────────────────────────────
cfb_pred <- read_csv("data/processed/cfb_to_nfl_te_predict_2026.csv")
nfl_pred <- read_csv("data/processed/nfl_to_nfl_te_predict_2026.csv")
cat("CFB rows:", nrow(cfb_pred), "\n")
cat("NFL rows:", nrow(nfl_pred), "\n")

# ── 2. Standardize CFB pred columns ───────────────────────────────────────────
cfb_pred_clean <- cfb_pred %>%
  mutate(college_flag = 1) %>%
  select(
    player_id                = nfl_player_id,
    player_display_name      = athlete_name,
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
    college_flag
  )

# ── 3. Backfill draft info for NFL pred rows from training data ────────────────
training_data <- read_csv("data/processed/te_combined_training.csv")

draft_lookup_pred <- training_data %>%
  select(player_id, overall, round, pick, height, weight,
         pre_draft_ranking, pre_draft_position_ranking, pre_draft_grade) %>%
  distinct(player_id, .keep_all = TRUE)

nfl_pred_clean <- nfl_pred %>%
  mutate(college_flag = 0) %>%
  select(player_id, player_display_name, college_flag,
         carries_pg, rushing_yards_pg, rushing_tds_pg,
         receptions_pg, receiving_yards_pg, receiving_tds_pg,
         fumble_recovery_own_pg, rushing_fumbles_lost_pg, rushing_fumbles_pg) %>%
  left_join(draft_lookup_pred, by = "player_id")

cat("NFL pred rows total:", nrow(nfl_pred_clean), "\n")
cat("Rows with draft info:", sum(!is.na(nfl_pred_clean$overall)), "\n")
cat("Rows missing draft info:", sum(is.na(nfl_pred_clean$overall)), "\n")

# ── 4. Union CFB and NFL pred sets ────────────────────────────────────────────
pred_combined <- bind_rows(cfb_pred_clean, nfl_pred_clean) %>%
  mutate(across(c(fumble_recovery_own_pg, rushing_fumbles_lost_pg,
                  rushing_fumbles_pg, carries_pg, rushing_yards_pg,
                  rushing_tds_pg, receptions_pg, receiving_yards_pg,
                  receiving_tds_pg),
                ~ replace_na(., 0)))

# ── 5. Backfill remaining missing draft info via name match ───────────────────
draft_name_lookup <- read_csv("data/raw/cfb_draft_data.csv") %>%
  filter(!is.na(overall), position == 'Tight End') %>%
  arrange(desc(season)) %>%
  distinct(name, .keep_all = TRUE) %>%
  select(name, overall, round, pick, height, weight,
         pre_draft_ranking, pre_draft_position_ranking, pre_draft_grade)

pred_combined <- pred_combined %>%
  left_join(draft_name_lookup, by = c("player_display_name" = "name"), suffix = c("", "_nm")) %>%
  mutate(
    overall                    = coalesce(overall, overall_nm),
    round                      = coalesce(round, round_nm),
    pick                       = coalesce(pick, pick_nm),
    height                     = coalesce(height, height_nm),
    weight                     = coalesce(weight, weight_nm),
    pre_draft_ranking          = coalesce(pre_draft_ranking, pre_draft_ranking_nm),
    pre_draft_position_ranking = coalesce(pre_draft_position_ranking, pre_draft_position_ranking_nm),
    pre_draft_grade            = coalesce(pre_draft_grade, pre_draft_grade_nm)
  ) %>%
  select(-ends_with("_nm"))

cat("Combined pred rows:", nrow(pred_combined), "\n")
cat("Combined pred columns:", ncol(pred_combined), "\n")
cat("Rows with draft info after backfill:", sum(!is.na(pred_combined$overall)), "\n")
cat("Rows still missing draft info:", sum(is.na(pred_combined$overall)), "\n")

# ── 6. Export ─────────────────────────────────────────────────────────────────
write_csv(pred_combined, "data/processed/te_pred_combined.csv")
cat("Export complete\n")
