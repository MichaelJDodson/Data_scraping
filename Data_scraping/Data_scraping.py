# standard library
import re
import time
import sys

# third party library
from bs4 import BeautifulSoup
import pandas as pd
import requests

# local library
import scraping_functions as scrape


# main file
# define main function here at some point

# if the script is being executed as a "main" program
if __name__ == "__main__":
    # determine which portion of code to run
    section_to_run = 2
    
    if section_to_run == 1:
        #returns large list containing smaller lists of 2 elements
        player_list = scrape.find_players_by_year('a', 'b', 1980, 1985)

        # initializes the list to store the data frames with the player metrics [[player_metrics, [season_1, season_1_data], [season_2, season_2_data], ...], [player_metrics_2, [...], ...]
        player_data_frames = []

        start_year = 1980
        end_year = 1981

        # iterates over the list of players for the given year range
        for player in player_list:
            
            # get the player-specific metrics
            player_metrics = scrape.get_player_metrics(player)
            # initialize the list containing all seasons for a player
            player_season_stats = []
            
            # iterate through the given years for season stats
            for season_start in range(start_year, (end_year + 1)):
                # get the season stats for a specific year
                season_stats = scrape.get_player_season_stats(player, season_start)
                # append to the list containing all seasons for a player
                player_season_stats.append([season_start, season_stats])
            
            # adds the player_metrics followed by a list containing all the player information for each season in the range
            player_data_frames.append([player_metrics, player_season_stats])
        
        # store the data using pickle   
        scrape.pickle_data(player_data_frames, scrape.variable_to_string_literal(player_data_frames))
    elif section_to_run == 2:
        season_schedule = scrape.full_games_schedule(1980,1980)
        
        
        
