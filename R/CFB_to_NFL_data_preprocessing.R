
library(dplyr)
library(tidyr)
library(stringr)


# Clean cfb data
cfb_stats <- read.csv('data/raw/cfb_player_stats_2010_2024.csv')
nfl_stats <- read.csv('data/raw/nfl_player_stats_2011_2025.csv')

#Split data by position
cfb_qb_df  <- cfb_stats %>% filter(position.x == "QB")
cfb_rb_df  <- cfb_stats %>% filter(position.x == "RB")
cfb_wr_df  <- cfb_stats %>% filter(position.x == "WR")
cfb_te_df  <- cfb_stats %>% filter(position.x == "TE")

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
  filter(season.x == max(season.x, na.rm = TRUE)) %>%
  ungroup()

rb_last <-cfb_rb_df %>%
  group_by(player_key) %>%
  filter(season.x == max(season.x, na.rm = TRUE)) %>%
  ungroup()

wr_last <-cfb_wr_df %>%
  group_by(player_key) %>%
  filter(season.x == max(season.x, na.rm = TRUE)) %>%
  ungroup()

te_last <-cfb_te_df %>%
  group_by(player_key) %>%
  filter(season.x == max(season.x, na.rm = TRUE)) %>%
  ungroup()

#aggregate stats
safe_max <- function(x) if (all(is.na(x))) NA_real_ else max(x, na.rm = TRUE)

qb_season <- qb_last %>%
  group_by(player_key) %>%
  summarise(
    athlete_name = first(athlete_name),
    season = max(season.x, na.rm = TRUE),
    position = first(position.x),
    games = n_distinct(week),
    
    # 🔹 KEEP DRAFT / MEASUREMENTS FIELDS
    overall = first(overall),
    round = first(round),
    pick = first(pick),
    height = first(height),
    weight = first(weight),
    pre_draft_ranking = first(pre_draft_ranking),
    pre_draft_position_ranking = first(pre_draft_position_ranking),
    pre_draft_grade = first(pre_draft_grade),
    
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
  df %>%
    group_by(player_key) %>%
    summarise(
      games = n_distinct(week),
      
      # 🔹 KEEP DRAFT / MEASUREMENTS FIELDS
      overall = first(overall),
      round = first(round),
      pick = first(pick),
      height = first(height),
      weight = first(weight),
      pre_draft_ranking = first(pre_draft_ranking),
      pre_draft_position_ranking = first(pre_draft_position_ranking),
      pre_draft_grade = first(pre_draft_grade),
      
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



#create weekly average for the rookie season (min season, not all players are rookies but it will not matter if player is not found in cfb set) per player for f points

nfl_stats_qb <- nfl_stats %>% filter(position == "QB")
nfl_stats_rb <- nfl_stats %>% filter(position == "RB")
nfl_stats_wr <- nfl_stats %>% filter(position == "WR")
nfl_stats_te <- nfl_stats %>% filter(position == "TE")


rookie_avg_weekly <- function(df) {
  df %>%
    group_by(player_id, player_display_name, season, position) %>%
    summarise(
      avg_weekly_ppr = mean(fantasy_points_ppr, na.rm = TRUE),
      n_weeks = n_distinct(week),
      .groups = "drop"
    ) %>%
    group_by(player_id) %>%
    slice_min(season, n = 1, with_ties = FALSE) %>%
    ungroup()
}

nfl_qb_weekly_avg <- rookie_avg_weekly(nfl_stats_qb)
nfl_rb_weekly_avg <- rookie_avg_weekly(nfl_stats_rb)
nfl_wr_weekly_avg <- rookie_avg_weekly(nfl_stats_wr)
nfl_te_weekly_avg <- rookie_avg_weekly(nfl_stats_te)



#create same kind of key for nfl players


nfl_qb_weekly_avg <- nfl_qb_weekly_avg %>%
  mutate(player_key = make_name_key(player_display_name))

nfl_rb_weekly_avg <- nfl_rb_weekly_avg %>%
  mutate(player_key = make_name_key(player_display_name))

nfl_wr_weekly_avg <- nfl_wr_weekly_avg %>%
  mutate(player_key = make_name_key(player_display_name))

nfl_te_weekly_avg <- nfl_te_weekly_avg %>%
  mutate(player_key = make_name_key(player_display_name))



assert_unique_key <- function(df, key_col, df_name) {
  dup <- df %>%
    count({{ key_col }}) %>%
    filter(n > 1)
  
  if (nrow(dup) > 0) {
    print(dup)
    stop(paste("Duplicate player_key values detected in", df_name))
  }
}


#remove handful of players where key does not work
nfl_wr_weekly_avg <- nfl_wr_weekly_avg %>%
  add_count(player_key) %>%
  filter(n == 1) %>%
  select(-n)
nfl_te_weekly_avg <- nfl_te_weekly_avg %>%
  add_count(player_key) %>%
  filter(n == 1) %>%
  select(-n)

assert_unique_key(nfl_qb_weekly_avg, player_key, "nfl_qb_weekly_avg")
assert_unique_key(nfl_rb_weekly_avg, player_key, "nfl_rb_weekly_avg")
assert_unique_key(nfl_wr_weekly_avg, player_key, "nfl_wr_weekly_avg")
assert_unique_key(nfl_te_weekly_avg, player_key, "nfl_te_weekly_avg")


# join nfl and cfb palyers
qb_joined <- qb_season %>%
  left_join(
    nfl_qb_weekly_avg %>%
      select(
        player_key,
        nfl_player_id = player_id,
        nfl_name = player_display_name,
        nfl_rookie_season = season,
        avg_weekly_ppr,
        n_weeks
      ),
    by = "player_key"
  )

#remove players who did not actually play in nfl
qb_model <- qb_joined %>%
  filter(!is.na(avg_weekly_ppr))

rb_joined <- rb_season %>% left_join(nfl_rb_weekly_avg %>% select(player_key, nfl_player_id = player_id, nfl_name = player_display_name, nfl_rookie_season = season, avg_weekly_ppr, n_weeks), by="player_key")
wr_joined <- wr_season %>% left_join(nfl_wr_weekly_avg %>% select(player_key, nfl_player_id = player_id, nfl_name = player_display_name, nfl_rookie_season = season, avg_weekly_ppr, n_weeks), by="player_key")
te_joined <- te_season %>% left_join(nfl_te_weekly_avg %>% select(player_key, nfl_player_id = player_id, nfl_name = player_display_name, nfl_rookie_season = season, avg_weekly_ppr, n_weeks), by="player_key")

rb_model <- rb_joined %>%
  filter(!is.na(avg_weekly_ppr))

wr_model <- wr_joined %>%
  filter(!is.na(avg_weekly_ppr))

te_model <- te_joined %>%
  filter(!is.na(avg_weekly_ppr))


write.csv(qb_model, "data/processed/cfb_to_nfl_qb_modeling.csv", row.names = FALSE)
write.csv(rb_model, "data/processed/cfb_to_nfl_rb_modeling.csv", row.names = FALSE)     
write.csv(wr_model, "data/processed/cfb_to_nfl_wr_modeling.csv", row.names = FALSE)     
write.csv(te_model, "data/processed/cfb_to_nfl_te_modeling.csv", row.names = FALSE)     




