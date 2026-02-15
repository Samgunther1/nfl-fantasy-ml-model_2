
library(dplyr)
library(tidyr)
library(arrow)
library(cfbfastR)

# Years and weeks
years <- 2020:2025
weeks <- 1:14

all_data <- list()
index <- 1

for (year in years) {
  for (week in weeks) {
    
    cat("Pulling:", year, "Week", week, "\n")
    
    tmp <- cfbd_game_player_stats(year = year, week = week)
    
    # Add year and week columns (VERY IMPORTANT)
    tmp$season <- year
    tmp$week <- week
    
    all_data[[index]] <- tmp
    index <- index + 1
  }
}

player_stats <- do.call(rbind, all_data)


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
  slice(1) %>%     # pick one if multiple positions exist
  ungroup()

pos_lookup %>% count(athlete_id, season) %>% filter(n > 1)

player_all <- player_stats %>%
  left_join(pos_lookup, by = c("athlete_id" = "athlete_id", "season" = "season"))


write.csv(player_all, "data/raw/cfb_player_stats_2020_2025.csv", row.names = FALSE)


