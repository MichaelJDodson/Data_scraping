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

    match section_to_run:
        case 1:
            scrape.find_players("a", "z")

        case 2:
            # returns large list containing smaller lists of 2 elements
            player_list = scrape.find_players_by_year("a", "z", 1980, 1981)

        case 3:
            # returns large list containing smaller lists of 2 elements
            player_list = scrape.find_players_by_year("a", "z", 1980, 1981)

            # range (inclusive, exclusive)
            year_range = range(1980, 1981)
            scrape.get_player_season_stats(player_list, year_range)

            # get the player-specific metrics
            # player_metrics = scrape.get_player_metrics(player)

        case 4:
            season_schedule = scrape.full_games_schedule(1980, 1981)

        case 5:
            # range (inclusive, exclusive)
            set_range = range(1980, 1981)
            game_data_df = scrape.collect_players_in_game(set_range)

            # pickle the data for later use

        case 6:
            scrape.pickled_players_in_games_to_csv()

        case 7:
            # print(scrape.get_team_abbreviations())
            with open(
                rf"C:\Users\Michael\Code\Python\Data_scraping\team_name_df.txt", "w"
            ) as file:
                file.write(scrape.get_team_abbreviations().to_string())

        case _:
            print("No section of code could run")
