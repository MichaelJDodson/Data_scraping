import re
import sys
import time

import pandas as pd
import requests
from bs4 import BeautifulSoup

# local library
import scraping_functions as scrape

# main file
# define main function here at some point

# if the script is being executed as a "main" program
if __name__ == "__main__":
    # determine which portion of code to run
    section_to_run = 6
    if section_to_run == 1:
        scrape.find_players("a", "z")

    elif section_to_run == 2:
        # returns large list containing smaller lists of 2 elements
        player_list = scrape.find_players_by_year("a", "z", 1980, 1981)

    elif section_to_run == 3:
        # returns large list containing smaller lists of 2 elements
        player_list = scrape.find_players_by_year("a", "z", 1980, 1981)

        # range (inclusive, exclusive)
        year_range = range(1980, 1981)
        scrape.get_player_season_stats(player_list, year_range)

        # get the player-specific metrics
        # player_metrics = scrape.get_player_metrics(player)

    elif section_to_run == 4:
        season_schedule = scrape.full_games_schedule(1980, 1981)

    elif section_to_run == 5:
        # range (inclusive, exclusive)
        set_range = range(1980, 1981)
        game_data_df = scrape.collect_players_in_game(set_range)

        # pickle the data for later use

    elif section_to_run == 6:
        scrape.pickled_players_in_games_to_csv()

    else:
        print("No section of code could run")
