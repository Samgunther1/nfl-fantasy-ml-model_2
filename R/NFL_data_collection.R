
library(nflreadr)

nfl_stats <- load_player_stats(seasons = c(2020,2021,2022,2023,2024,2025), summary_level = 'week')
write.csv(nfl_stats, "data/raw/nfl_player_stats_2020_2025.csv", row.names = FALSE)              
