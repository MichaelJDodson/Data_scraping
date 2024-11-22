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
        player_list = scrape.find_players_by_year('a', 'b', 1980, 1982)
        
    elif section_to_run == 2:
        #returns large list containing smaller lists of 2 elements
        player_list = scrape.find_players_by_year('a', 'b', 1980, 1982)

        # initializes the list to store the data frames with the player metrics [[player_metrics, [season_1, season_1_data], [season_2, season_2_data], ...], [player_metrics_2, [...], ...]
        player_data_frames = []
        year_range = range(1980, 1981)
        scrape.get_player_season_stats(player_list, year_range)
        
        # get the player-specific metrics
        #player_metrics = scrape.get_player_metrics(player)

    elif section_to_run == 3:
        season_schedule = scrape.full_games_schedule(1980,1980)
    
    elif section_to_run == 4:
        scrape.get_html(rf'https://www.basketball-reference.com/players/a/aingeda01/gamelog/1982/', rf'C:\Users\Michael\Code\Python\Data_scraping\test_folder\test_data.html')
        
    else:
        print('No section of code could run')
        
