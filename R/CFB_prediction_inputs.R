library(dplyr)
library(tidyr)
library(arrow)
library(cfbfastR)
library(nflreadr)

# Years and weeks
years <- 2025
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


write.csv(player_all, "data/processed/cfb_to_nfl_prediction_inputs.csv", row.names = FALSE)

