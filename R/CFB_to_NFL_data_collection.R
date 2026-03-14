library(dplyr)
library(tidyr)
library(arrow)
library(cfbfastR)
library(nflreadr)

# ── 1. CFB Player Stats ───────────────────────────────────────────────────────
years <- 2004:2025
weeks <- 1:14
all_data <- list()
index <- 1

for (year in years) {
  for (week in weeks) {
    cat("Pulling:", year, "Week", week, "\n")
    
    tryCatch({
      tmp <- cfbd_game_player_stats(year = year, week = week)
      
      if (!is.null(tmp) && nrow(tmp) > 0) {
        tmp$season <- year
        tmp$week <- week
        all_data[[index]] <- tmp
        index <- index + 1
      } else {
        cat("  No data for", year, "Week", week, "- skipping\n")
      }
      
    }, error = function(e) {
      cat("  Error for", year, "Week", week, ":", conditionMessage(e), "- skipping\n")
    })
  }
}

player_stats <- bind_rows(all_data)

# ── 2. CFB Rosters (position lookup) ─────────────────────────────────────────
all_data <- list()
index <- 1

for (year in years) {
  cat("Pulling roster:", year, "\n")
  
  tryCatch({
    tmp <- load_cfb_rosters(seasons = year)
    tmp$season <- year
    all_data[[index]] <- tmp
    index <- index + 1
  }, error = function(e) {
    cat("  Error for", year, ":", conditionMessage(e), "- skipping\n")
  })
}

player_details <- do.call(rbind, all_data)
player_details <- player_details %>%
  mutate(athlete_id = as.integer(athlete_id))

pos_lookup <- player_details %>%
  select(athlete_id, season, position) %>%
  filter(!is.na(position), position != "") %>%
  distinct(athlete_id, season, position) %>%
  group_by(athlete_id, season) %>%
  slice(1) %>%
  ungroup()

# ── 3. Join position onto player stats ────────────────────────────────────────
player_all <- player_stats %>%
  left_join(pos_lookup, by = c("athlete_id" = "athlete_id", "season" = "season")) %>%
  rename(college_athlete_id = athlete_id)

# ── 4. CFB Draft Data ─────────────────────────────────────────────────────────
all_data <- list()
index <- 1
draft_years <- 1970:2025

for (year in draft_years) {
  cat("Pulling draft:", year, "\n")
  
  tryCatch({
    tmp <- cfbd_draft_picks(year = year)
    
    if (!is.null(tmp) && nrow(tmp) > 0) {
      tmp$season <- year
      all_data[[index]] <- tmp
      index <- index + 1
    } else {
      cat("  No data for", year, "- skipping\n")
    }
    
  }, error = function(e) {
    cat("  Error for", year, ":", conditionMessage(e), "- skipping\n")
  })
}

player_draft <- bind_rows(all_data)
player_draft <- player_draft %>%
  mutate(college_athlete_id = as.integer(college_athlete_id))

# Export draft data
write.csv(player_draft, "data/raw/cfb_draft_data.csv", row.names = FALSE)
cat("Draft data exported\n")

# Draft columns we want to bring into player_all
draft_info_cols <- c("overall", "round", "pick", "height", "weight",
                     "pre_draft_ranking", "pre_draft_position_ranking",
                     "pre_draft_grade", "nfl_athlete_id")

# ── 5. Two-step draft join ────────────────────────────────────────────────────

# Step A — ID-based lookup for modern records
draft_by_id <- player_draft %>%
  filter(!is.na(college_athlete_id)) %>%
  arrange(desc(season)) %>%
  distinct(college_athlete_id, .keep_all = TRUE) %>%
  select(college_athlete_id, all_of(draft_info_cols))

# Step B — name-based lookup for old records without ID
draft_by_name <- player_draft %>%
  filter(is.na(college_athlete_id)) %>%
  arrange(desc(season)) %>%
  distinct(name, .keep_all = TRUE) %>%
  select(name, all_of(draft_info_cols))

# Join by ID first
player_all <- player_all %>%
  left_join(draft_by_id, by = "college_athlete_id")

# Fill remaining NAs via name match
player_all <- player_all %>%
  left_join(
    draft_by_name,
    by = c("athlete_name" = "name"),
    suffix = c("", "_nm")
  ) %>%
  mutate(
    overall                    = coalesce(overall, overall_nm),
    round                      = coalesce(round, round_nm),
    pick                       = coalesce(pick, pick_nm),
    height                     = coalesce(height, height_nm),
    weight                     = coalesce(weight, weight_nm),
    pre_draft_ranking          = coalesce(pre_draft_ranking, pre_draft_ranking_nm),
    pre_draft_position_ranking = coalesce(pre_draft_position_ranking, pre_draft_position_ranking_nm),
    pre_draft_grade            = coalesce(pre_draft_grade, pre_draft_grade_nm),
    nfl_athlete_id             = coalesce(nfl_athlete_id, nfl_athlete_id_nm)
  ) %>%
  select(-ends_with("_nm"))

# Sanity check
cat("Total rows:", nrow(player_all), "\n")
cat("Rows with draft info:", sum(!is.na(player_all$overall)), "\n")
cat("Rows without draft info:", sum(is.na(player_all$overall)), "\n")

# ── 6. Export CFB data ────────────────────────────────────────────────────────
write.csv(player_all, "data/raw/cfb_player_stats.csv", row.names = FALSE)

# ── 7. NFL Player Stats ───────────────────────────────────────────────────────
nfl_years <- min(years):max(years)
nfl_stats <- load_player_stats(
  seasons = nfl_years,
  summary_level = "week"
)

write.csv(nfl_stats, "data/raw/nfl_player_stats.csv", row.names = FALSE)