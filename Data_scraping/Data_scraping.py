import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import time
import scraping_functions as scrape

#main file

# get all players
#scrape.find_players('a', 'z')

#returns large list containing smaller lists of 2 elements
player_list = scrape.find_players_by_year('a', 'b', 1980, 1985)

# initializes the list to store the data frames with the player metrics [[player_metrics, [season_1, season_1_data], [season_2, season_2_data], ...], [player_metrics_2, [...], ...]
player_data_frames = []

# iterates over the list of players for the given year range
for player in player_list:
    
    # get the player-specific metrics
    player_metrics = scrape.get_player_metrics(player)
    # initialize the list containing all seasons for a player
    player_season_stats = []
    
    # iterate through the given years for season stats
    for season_start in range(1980, 1981):
        # get the season stats for a specific year
        season_stats = scrape.get_player_season_stats(player, season_start)
        # append to the list containing all seasons for a player
        player_season_stats.append([season_start, season_stats])
    
    # adds the player_metrics followed by a list containing all the player information for each season in the range
    player_data_frames.append([player_metrics, player_season_stats])
