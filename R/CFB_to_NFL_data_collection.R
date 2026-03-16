library(dplyr)
library(tidyr)
library(arrow)
library(cfbfastR)
library(nflreadr)

# ── 0. Load existing data (if present) ───────────────────────────────────────
cfb_path   <- "data/raw/cfb_player_stats.csv"
draft_path <- "data/raw/cfb_draft_data.csv"
nfl_path   <- "data/raw/nfl_player_stats.csv"

# CFB player stats
if (file.exists(cfb_path)) {
  cat("Reading existing CFB player stats...\n")
  player_all_existing <- read.csv(cfb_path)
  pulled_cfb <- player_all_existing %>%
    distinct(season, week) %>%
    mutate(season = as.integer(season), week = as.integer(week))
} else {
  player_all_existing <- NULL
  pulled_cfb <- tibble(season = integer(), week = integer())
}

# CFB draft data
if (file.exists(draft_path)) {
  cat("Reading existing CFB draft data...\n")
  player_draft_existing <- read.csv(draft_path)
  pulled_draft_years <- unique(as.integer(player_draft_existing$season))
} else {
  player_draft_existing <- NULL
  pulled_draft_years <- integer()
}

# NFL player stats
if (file.exists(nfl_path)) {
  cat("Reading existing NFL player stats...\n")
  nfl_existing <- read.csv(nfl_path)
  pulled_nfl_seasons <- unique(as.integer(nfl_existing$season))
} else {
  nfl_existing <- NULL
  pulled_nfl_seasons <- integer()
}

# ── 1. CFB Player Stats ───────────────────────────────────────────────────────
years <- 2004:2025
weeks <- 1:14

# Build full grid of desired year/week combos, subtract what we already have
desired_cfb <- expand.grid(season = years, week = weeks) %>%
  anti_join(pulled_cfb, by = c("season", "week")) %>%
  arrange(season, week)

if (nrow(desired_cfb) == 0) {
  cat("CFB player stats: nothing new to pull.\n")
  new_cfb_stats <- NULL
} else {
  cat(sprintf("CFB player stats: pulling %d year/week combos...\n", nrow(desired_cfb)))
  all_data <- list()
  index <- 1
  
  for (i in seq_len(nrow(desired_cfb))) {
    year <- desired_cfb$season[i]
    week <- desired_cfb$week[i]
    cat("Pulling:", year, "Week", week, "\n")
    
    tryCatch({
      tmp <- cfbd_game_player_stats(year = year, week = week)
      
      if (!is.null(tmp) && nrow(tmp) > 0) {
        tmp$season <- year
        tmp$week   <- week
        all_data[[index]] <- tmp
        index <- index + 1
      } else {
        cat("  No data for", year, "Week", week, "- skipping\n")
      }
    }, error = function(e) {
      cat("  Error for", year, "Week", week, ":", conditionMessage(e), "- skipping\n")
    })
  }
  
  new_cfb_stats <- if (length(all_data) > 0) bind_rows(all_data) else NULL
}

# ── 2. CFB Rosters (position lookup) ─────────────────────────────────────────
# Pull rosters for any year that appears in new stats (position join happens later)
new_stat_years <- if (!is.null(new_cfb_stats)) unique(new_cfb_stats$season) else integer()

if (length(new_stat_years) == 0) {
  cat("CFB rosters: nothing new to pull.\n")
  pos_lookup_new <- NULL
} else {
  cat(sprintf("CFB rosters: pulling %d season(s)...\n", length(new_stat_years)))
  all_data <- list()
  index <- 1
  
  for (year in new_stat_years) {
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
  
  if (length(all_data) > 0) {
    player_details_new <- do.call(rbind, all_data) %>%
      mutate(athlete_id = as.integer(athlete_id))
    
    pos_lookup_new <- player_details_new %>%
      select(athlete_id, season, position) %>%
      filter(!is.na(position), position != "") %>%
      distinct(athlete_id, season, position) %>%
      group_by(athlete_id, season) %>%
      slice(1) %>%
      ungroup()
  } else {
    pos_lookup_new <- NULL
  }
}

# ── 3. Join position onto new player stats ────────────────────────────────────
if (!is.null(new_cfb_stats) && !is.null(pos_lookup_new)) {
  new_cfb_stats <- new_cfb_stats %>%
    left_join(pos_lookup_new, by = c("athlete_id" = "athlete_id", "season" = "season")) %>%
    rename(college_athlete_id = athlete_id)
} else if (!is.null(new_cfb_stats)) {
  new_cfb_stats <- new_cfb_stats %>%
    rename(college_athlete_id = athlete_id)
}

# ── 4. CFB Draft Data ─────────────────────────────────────────────────────────
draft_years    <- 1970:2025
new_draft_years <- setdiff(draft_years, pulled_draft_years)

if (length(new_draft_years) == 0) {
  cat("CFB draft data: nothing new to pull.\n")
  player_draft_new <- NULL
} else {
  cat(sprintf("CFB draft data: pulling %d year(s)...\n", length(new_draft_years)))
  all_data <- list()
  index <- 1
  
  for (year in new_draft_years) {
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
  
  player_draft_new <- if (length(all_data) > 0) bind_rows(all_data) else NULL
}

# Combine old + new draft data, then export
player_draft <- bind_rows(player_draft_existing, player_draft_new) %>%
  mutate(college_athlete_id = as.integer(college_athlete_id))

if (!is.null(player_draft_new)) {
  write.csv(player_draft, draft_path, row.names = FALSE)
  cat("Draft data exported\n")
}

# ── 5. Build draft lookup tables ──────────────────────────────────────────────
draft_info_cols <- c("overall", "round", "pick", "height", "weight",
                     "pre_draft_ranking", "pre_draft_position_ranking",
                     "pre_draft_grade", "nfl_athlete_id")

draft_by_id <- player_draft %>%
  filter(!is.na(college_athlete_id)) %>%
  arrange(desc(season)) %>%
  distinct(college_athlete_id, .keep_all = TRUE) %>%
  select(college_athlete_id, all_of(draft_info_cols))

draft_by_name <- player_draft %>%
  filter(is.na(college_athlete_id)) %>%
  arrange(desc(season)) %>%
  distinct(name, .keep_all = TRUE) %>%
  select(name, all_of(draft_info_cols))

# ── 6. Apply draft info to new CFB stats & combine with existing ──────────────
apply_draft_info <- function(df, by_id, by_name) {
  df %>%
    left_join(by_id, by = "college_athlete_id") %>%
    left_join(by_name, by = c("athlete_name" = "name"), suffix = c("", "_nm")) %>%
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
}

if (!is.null(new_cfb_stats)) {
  new_cfb_stats <- apply_draft_info(new_cfb_stats, draft_by_id, draft_by_name)
}

# Stack existing + new, then re-apply draft info to existing rows in case
# draft data was updated (e.g. a newly drafted player's old college rows)
player_all <- bind_rows(player_all_existing, new_cfb_stats)

if (!is.null(player_draft_new)) {
  # New draft picks arrived — refresh draft columns across the whole file
  cat("New draft data found; refreshing draft columns on all rows...\n")
  player_all <- player_all %>%
    select(-any_of(c(draft_info_cols))) %>%   # drop stale draft cols
    apply_draft_info(draft_by_id, draft_by_name)
}

# ── 7. Export CFB data ────────────────────────────────────────────────────────
if (!is.null(new_cfb_stats) || !is.null(player_draft_new)) {
  cat("Total rows:", nrow(player_all), "\n")
  cat("Rows with draft info:", sum(!is.na(player_all$overall)), "\n")
  cat("Rows without draft info:", sum(is.na(player_all$overall)), "\n")
  
  write.csv(player_all, cfb_path, row.names = FALSE)
  cat("CFB player stats exported\n")
} else {
  cat("No CFB changes — skipping export.\n")
}

# ── 8. NFL Player Stats ───────────────────────────────────────────────────────
nfl_years        <- min(years):max(years)
new_nfl_seasons  <- setdiff(nfl_years, pulled_nfl_seasons)

if (length(new_nfl_seasons) == 0) {
  cat("NFL player stats: nothing new to pull.\n")
} else {
  cat(sprintf("NFL player stats: pulling %d season(s)...\n", length(new_nfl_seasons)))
  
  nfl_new <- load_player_stats(
    seasons      = new_nfl_seasons,
    summary_level = "week"
  )
  
  nfl_stats <- bind_rows(nfl_existing, nfl_new)
  write.csv(nfl_stats, nfl_path, row.names = FALSE)
  cat("NFL player stats exported\n")
}
