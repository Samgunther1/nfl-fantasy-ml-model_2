library(dplyr)
library(tidyr)
library(stringr)


# Clean cfb data
cfb_stats <- read.csv('data/processed/cfb_to_nfl_prediction_inputs.csv')


#Split data by position
cfb_qb_df  <- cfb_stats %>% filter(position == "QB")
cfb_rb_df  <- cfb_stats %>% filter(position == "RB")
cfb_wr_df  <- cfb_stats %>% filter(position == "WR")
cfb_te_df  <- cfb_stats %>% filter(position == "TE")

#create player key
make_name_key <- function(x) {
  x %>%
    str_to_lower() %>%
    str_replace_all("[^a-z\\s]", " ") %>%   # drop punctuation (kills the "." in "Jr.")
    str_squish() %>%
    # standardize common suffix tokens (keep them)
    str_replace_all("\\bjunior\\b", "jr") %>%
    str_replace_all("\\bsenior\\b", "sr")
}

cfb_qb_df <- cfb_qb_df %>% mutate(player_key = make_name_key(athlete_name))
cfb_rb_df <- cfb_rb_df %>% mutate(player_key = make_name_key(athlete_name))
cfb_wr_df <- cfb_wr_df %>% mutate(player_key = make_name_key(athlete_name))
cfb_te_df <- cfb_te_df %>% mutate(player_key = make_name_key(athlete_name))


#Drop columns that are not used for each position

cfb_qb_df <- cfb_qb_df %>%
  select(
    -starts_with("receiving"),
    -starts_with("kick"),
    -starts_with("punting"),
    -starts_with("defensive"),
    -starts_with("punt"),
    -team,
    -conference,
    -home_away,
    -game_id,
    -team_points
  )

cfb_rb_df <- cfb_rb_df %>%
  select(
    -starts_with("kick"),
    -starts_with("punting"),
    -starts_with("defensive"),
    -starts_with("punt"),
    -starts_with('passing'),
    -team,
    -conference,
    -home_away,
    -game_id,
    -team_points
  )


cfb_wr_df <- cfb_wr_df %>%
  select(
    -starts_with("kick"),
    -starts_with("punting"),
    -starts_with("defensive"),
    -starts_with("punt"),
    -starts_with('passing'),
    -team,
    -conference,
    -home_away,
    -game_id,
    -team_points
  )

cfb_te_df <- cfb_te_df %>%
  select(
    -starts_with("kick"),
    -starts_with("punting"),
    -starts_with("defensive"),
    -starts_with("punt"),
    -starts_with('passing'),
    -team,
    -conference,
    -home_away,
    -game_id,
    -team_points
  )

#filter to max season
qb_last <-cfb_qb_df %>%
  group_by(player_key) %>%
  filter(season == max(season, na.rm = TRUE)) %>%
  ungroup()

rb_last <-cfb_rb_df %>%
  group_by(player_key) %>%
  filter(season == max(season, na.rm = TRUE)) %>%
  ungroup()

wr_last <-cfb_wr_df %>%
  group_by(player_key) %>%
  filter(season == max(season, na.rm = TRUE)) %>%
  ungroup()

te_last <-cfb_te_df %>%
  group_by(player_key) %>%
  filter(season == max(season, na.rm = TRUE)) %>%
  ungroup()

#aggregate stats
safe_max <- function(x) if (all(is.na(x))) NA_real_ else max(x, na.rm = TRUE)

qb_season <- qb_last %>%
  group_by(player_key) %>%
  summarise(
    athlete_name = first(athlete_name),
    season = max(season, na.rm = TRUE),
    position = first(position),
    games = n_distinct(week),
    
    # ✅ CREATE draft columns (unknown for 2025 class)
    overall = NA_real_,
    round = NA_real_,
    pick = NA_real_,
    pre_draft_ranking = NA_real_,
    pre_draft_position_ranking = NA_real_,
    pre_draft_grade = NA_real_,
    
    # optional: tell the model this is "pre-draft / unknown"
    draft_info_available = 0,
    
    # ✅ measurements (keep if they exist, else NA)
    height = if ("height" %in% names(cur_data_all())) first(height) else NA_real_,
    weight = if ("weight" %in% names(cur_data_all())) first(weight) else NA_real_,
    
    fumbles_rec  = sum(fumbles_rec,  na.rm = TRUE),
    fumbles_lost = sum(fumbles_lost, na.rm = TRUE),
    fumbles_fum  = sum(fumbles_fum,  na.rm = TRUE),
    
    interceptions_td  = sum(interceptions_td,  na.rm = TRUE),
    interceptions_yds = sum(interceptions_yds, na.rm = TRUE),
    interceptions_int = sum(interceptions_int, na.rm = TRUE),
    
    rushing_long = safe_max(rushing_long),
    rushing_td   = sum(rushing_td,   na.rm = TRUE),
    rushing_yds  = sum(rushing_yds,  na.rm = TRUE),
    rushing_car  = sum(rushing_car,  na.rm = TRUE),
    
    passing_int         = sum(passing_int,         na.rm = TRUE),
    passing_td          = sum(passing_td,          na.rm = TRUE),
    passing_yds         = sum(passing_yds,         na.rm = TRUE),
    passing_completions = sum(passing_completions, na.rm = TRUE),
    passing_attempts    = sum(passing_attempts,    na.rm = TRUE),
    
    passing_avg = ifelse(passing_attempts > 0, passing_yds / passing_attempts, NA_real_),
    rushing_avg = ifelse(rushing_car > 0, rushing_yds / rushing_car, NA_real_),
    passing_qbr = mean(passing_qbr, na.rm = TRUE),
    
    athlete_id = first(college_athlete_id),
    
    .groups = "drop"
  )

                       


safe_max <- function(x) {
  if (all(is.na(x))) NA_real_ else max(x, na.rm = TRUE)
}

skill_season_totals <- function(df) {
  
  # columns you want guaranteed in the output
  required_cols <- c(
    "overall","round","pick","height","weight",
    "pre_draft_ranking","pre_draft_position_ranking","pre_draft_grade"
  )
  
  # add any missing columns as NA_real_
  missing_cols <- setdiff(required_cols, names(df))
  if (length(missing_cols) > 0) {
    df[missing_cols] <- NA_real_
  }
  
  df %>%
    group_by(player_key) %>%
    summarise(
      games = n_distinct(week),
      
      # ✅ DRAFT / MEASUREMENTS (now always exist)
      overall = first(overall),
      round = first(round),
      pick = first(pick),
      height = first(height),
      weight = first(weight),
      pre_draft_ranking = first(pre_draft_ranking),
      pre_draft_position_ranking = first(pre_draft_position_ranking),
      pre_draft_grade = first(pre_draft_grade),
      
      # optional: tell the model draft info isn't available yet (2025 class)
      draft_info_available = ifelse(all(is.na(overall)) & all(is.na(round)) & all(is.na(pick)), 0, 1),
      
      # Totals
      fumbles_rec  = sum(fumbles_rec,  na.rm = TRUE),
      fumbles_lost = sum(fumbles_lost, na.rm = TRUE),
      fumbles_fum  = sum(fumbles_fum,  na.rm = TRUE),
      
      interceptions_td  = sum(interceptions_td,  na.rm = TRUE),
      interceptions_yds = sum(interceptions_yds, na.rm = TRUE),
      interceptions_int = sum(interceptions_int, na.rm = TRUE),
      
      receiving_td  = sum(receiving_td,  na.rm = TRUE),
      receiving_yds = sum(receiving_yds, na.rm = TRUE),
      receiving_rec = sum(receiving_rec, na.rm = TRUE),
      
      rushing_td  = sum(rushing_td,  na.rm = TRUE),
      rushing_yds = sum(rushing_yds, na.rm = TRUE),
      rushing_car = sum(rushing_car, na.rm = TRUE),
      
      # Longs (safe max)
      receiving_long = safe_max(receiving_long),
      rushing_long   = safe_max(rushing_long),
      
      # Recomputed season-level rates
      receiving_avg = ifelse(receiving_rec > 0, receiving_yds / receiving_rec, NA_real_),
      rushing_avg   = ifelse(rushing_car > 0, rushing_yds / rushing_car, NA_real_),
      
      .groups = "drop"
    )
}

rb_season <- skill_season_totals(rb_last)
wr_season <- skill_season_totals(wr_last)
te_season <- skill_season_totals(te_last)

#check to make sure keys are unique
assert_unique_key <- function(df, key_col, df_name = "dataframe") {
  dup_check <- df %>%
    count({{ key_col }}) %>%
    filter(n > 1)
  
  if (nrow(dup_check) > 0) {
    stop(paste("Duplicate values detected in", df_name, "for key:", deparse(substitute(key_col))))
  }
}

assert_unique_key(qb_season, player_key, "qb_season")
assert_unique_key(rb_season, player_key, "rb_season")
assert_unique_key(wr_season, player_key, "wr_season")
assert_unique_key(te_season, player_key, "te_season")





write.csv(qb_season, "data/processed/cfb_to_nfl_qb_prediction_inputs.csv", row.names = FALSE)
write.csv(rb_season, "data/processed/cfb_to_nfl_rb_prediction_inputs.csv", row.names = FALSE)     
write.csv(wr_season, "data/processed/cfb_to_nfl_wr_prediction_inputs.csv", row.names = FALSE)     
write.csv(te_season, "data/processed/cfb_to_nfl_te_prediction_inputs.csv", row.names = FALSE)
