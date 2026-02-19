
library(dplyr)
library(tidyr)
library(arrow)
library(cfbfastR)
library(nflreadr)

# Years and weeks
years <- 2010:2024
weeks <- 1:14

all_data <- list()
index <- 1

for (year in years) {
  for (week in weeks) {
    
    cat("Pulling:", year, "Week", week, "\n")
    
    tmp <- cfbd_game_player_stats(year = year, week = week)
    
    # Add year and week columns 
    tmp$season <- year
    tmp$week <- week
    
    all_data[[index]] <- tmp
    index <- index + 1
  }
}

player_stats <- bind_rows(all_data)


all_data <- list()
index <- 1
for (year in years) {
  cat("Pulling:", year)
  
  tmp <-load_cfb_rosters(seasons = year)
  tmp$season <- year
  
  all_data[[index]] <- tmp
  index <- index + 1
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

pos_lookup %>% count(athlete_id, season) %>% filter(n > 1)

player_all <- player_stats %>%
  left_join(pos_lookup, by = c("athlete_id" = "athlete_id", "season" = "season"))

player_all <- rename(player_all, college_athlete_id = athlete_id)

all_data <- list()
index <- 1

for (year in years) {
  
    
    cat("Pulling:", year, "\n")
    
    tmp <- cfbd_draft_picks(year = year)
    
    # Add year and week columns 
    tmp$season <- year
    
    all_data[[index]] <- tmp
    index <- index + 1
  
}

player_draft <- bind_rows(all_data)
player_draft <- player_draft %>% 
  mutate(college_athlete_id = as.integer(college_athlete_id))

player_all <- player_all %>%
  left_join(player_draft, by = ("college_athlete_id" = "college_athlete_id"))

start_year <- min(years)
end_year <- max(years)

file_name <- paste0(
  "data/raw/cfb_player_stats_",
  start_year, "_", end_year, ".csv"
)

write.csv(player_all, file_name, row.names = FALSE)

#pull nfl stats, this will be + 1 year to each end of cfb stats.

nfl_years <- (min(years) + 1):(max(years) + 1)

nfl_stats <- load_player_stats(
  seasons = nfl_years,
  summary_level = "week"
)

write.csv(
  nfl_stats,
  paste0("data/raw/nfl_player_stats_", min(nfl_years), "_", max(nfl_years), ".csv"),
  row.names = FALSE
)




