import csv
import json
import os
import pickle
import random
import re
import sys
import time
from datetime import datetime
from io import StringIO
from pathlib import Path

import pandas as pd
import xmltojson
from bs4 import BeautifulSoup
from pandas import DataFrame
from selenium import webdriver
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.service import Service

# contains all the functions necessary for Data_scraping on https://www.basketball-reference.com


# use selenium to load page for given url for dynamic html scraping; if saving the html data from a page it does not return a string
def selenium_request(
    firefox_driver: webdriver.Firefox,
    request_url: str,
    save_html: bool = False,
    file_path: str = None,
) -> str:

    # max retries
    retry = 10

    for attempt in range(retry):
        try:

            # open URL
            firefox_driver.get(request_url)

            # allow time for JavaScript to load
            time.sleep(10)

            # page source
            html_source = firefox_driver.page_source

            # if saving html data
            if save_html and file_path:
                with open(file_path, "w", encoding="utf-8") as file:
                    file.write(html_source)
                print(f"HTML saved to {file_path}")

            # quit driver and return HTML if not saving
            if not save_html:
                return html_source

            # break the loop if successful
            break
        # exponential backoff
        except (WebDriverException, TimeoutException) as e:
            # handle Selenium-specific exceptions
            wait = 2**attempt + random.uniform(0, 1)
            print(f"Attempt {attempt + 1} failed: {e}. Retrying in {wait:.2f} seconds.")
            time.sleep(wait)


# only want to initialize the driver a single time within a function before iterating over a list of urls * make sure to quit the driver after use
def initialize_selenium_driver() -> webdriver.Firefox:
    options = Options()
    # run without GUI, extensions, and gpu to reduce resource usage
    options.add_argument("--headless")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-gpu")

    # use a specific version of GeckoDriver (manually installed)
    gecko_path = rf"C:\Users\Michael\geckodriver-v0.35.0-win64\geckodriver.exe"
    if not os.path.exists(gecko_path):
        raise FileNotFoundError(
            f"GeckoDriver not found at {gecko_path}. Please download it manually."
        )

    service = Service(gecko_path)

    driver = webdriver.Firefox(service=service, options=options)

    return driver


# error handles for issues that may arise
def basic_error_handling(possible_error):
    if isinstance(possible_error, ValueError):
        print("ValueError: Invalid value provided.")
    elif isinstance(possible_error, ZeroDivisionError):
        print("ZeroDivisionError: Division by zero is not allowed.")
    elif isinstance(possible_error, FileNotFoundError):
        print("File not found!")
    elif isinstance(possible_error, PermissionError):
        print("You don't have permission to access this file!")
    else:
        print(f"An unexpected error occurred: {possible_error}")


# handles html table conversion to a dictionary; allows for transformation to JSON if needed
def table_to_dictionary(table) -> list:
    # *** in general the tables I target contain a thead section for headers, even if I don't extract the headers directly from there
    if not table.find("thead"):
        return None

    # extract rows
    all_rows_data = []
    for table_row in table.find("tbody").find_all("tr"):
        # make dictionary for label assignments
        row_data_dict = {}

        # iterate over tags in table rows
        for row_cell_data in table_row.find_all(["td", "th"]):
            # .get() to retrieve data in a given attribute
            single_cell_data = row_cell_data.get("data-stat")
            if not single_cell_data:
                continue

            data_header = single_cell_data.replace("*", "")
            main_data_text = row_cell_data.get_text().replace("*", "")
            # add {header : main_text} pair to the dict
            row_data_dict[data_header] = main_data_text

            for a_attribute_cell_data in row_cell_data.find_all("a"):
                if a_attribute_cell_data is None:
                    continue

                # assign url header and info into the dict
                url_header = rf"{single_cell_data}_url"
                url_data = a_attribute_cell_data["href"]
                # check if key is already in dict
                # append if it is
                if row_data_dict.get(url_header):
                    row_data_dict[url_header].append(url_data)
                # create it if it isn't
                else:
                    row_data_dict[url_header] = [url_data]

        # append row data
        if row_data_dict:
            all_rows_data.append(row_data_dict)

    return all_rows_data


# utilize the pickle library to save the contents of a list or other data structure for later use (*not for DataFrames)
def pickle_data(data_for_later, file_name: str):
    try:
        with open(
            rf"C:\Users\Michael\Code\Python\Data_scraping\pickled_data\{file_name}.pkl",
            "wb",
        ) as file:
            pickle.dump(data_for_later, file)
    except Exception as e:
        basic_error_handling(e)


# retrieve the string literal of a variable name for ease of use when naming files
def variable_to_string_literal(variable):
    for name, value in globals().items():
        if value is variable:
            return name
    return None


# takes a text file containing information for all NBA teams and transfers the info to a DataFrame for comparisons; * These abbreviations are the ones used by basketball-reference, not necessarily the "official" ones
def get_team_abbreviations() -> DataFrame:
    # headers for the relevant data
    team_headers = ["team_location", "team_abbreviation", "team_name", "year_active"]
    # list to collect the rows of data
    team_table = []
    # open text file containing team name and abbreviations for all historical and current NBA teams, excluding only a few repeat teams that "re branded" circa ~1950's
    try:
        with open(
            rf"C:\Users\Michael\Code\Python\Data_scraping\nba_team_names.txt", "r"
        ) as file:
            for line in file:
                # strip leading/trailing whitespace and split by tab
                row = line.strip().split("\t")
                team_table.append(row)
    except Exception as e:
        basic_error_handling(e)

    # place data into a DataFrame
    team_name_df = pd.DataFrame(team_table, columns=team_headers)
    # make all team abbreviations uppercase
    team_name_df["team_abbreviation"] = team_name_df["team_abbreviation"].apply(
        lambda x: x.upper()
    )
    # separate by comma for teams that have multiple abbreviations; makes the abbreviation elements into a list
    team_name_df["team_abbreviation"] = team_name_df["team_abbreviation"].apply(
        lambda x: x.split(",")
    )

    return team_name_df


# takes in a full team name with the season year, returns the abbreviation
def full_to_abbreviation(full_name: str, year: int) -> str:
    # get abbreviations for all teams
    team_abbreviations = get_team_abbreviations()
    # strip team name and lower case for comparison
    full_name = full_name.strip().lower()

    for full_name_compare in team_abbreviations["team_name"]:
        if full_name == full_name_compare.strip().lower():
            row_index = team_abbreviations.index[
                team_abbreviations["team_name"] == full_name_compare
            ][0]

            charlotte_hornets = "charlotte hornets"
            if full_name_compare.strip().lower() == charlotte_hornets:
                if 1989 <= year <= 2001:
                    # CHH
                    return team_abbreviations.loc[row_index, "team_abbreviation"][0]
                else:
                    # CHO
                    return team_abbreviations.loc[row_index, "team_abbreviation"][1]
            else:
                # default abbreviation if not charlotte
                return team_abbreviations.loc[row_index, "team_abbreviation"][0]
    # return original name if no match
    return full_name


# handles time conversion from hours:minutes:seconds to just a floating point for minutes
def playtime_conversion(time_str: str) -> float:
    # splits up time pieces and makes a list
    time_str_parse = time_str.split(":")
    # determine that number of parts given (most players only have minutes:seconds)
    time_parts = len(time_str_parse)

    # calculate time in minutes
    match time_parts:
        case 1:
            time_obj = datetime.strptime(time_str, "%S")
            total_minutes = float(time_obj.second) / 60.0
            return total_minutes
        case 2:
            time_obj = datetime.strptime(time_str, "%M:%S")
            total_minutes = float(time_obj.minute) + (float(time_obj.second) / 60.0)
            return total_minutes
        case 3:
            time_obj = datetime.strptime(time_str, "%H:%M:%S")
            total_minutes = (
                (float(time_obj.hour) * 60.0)
                + float(time_obj.minute)
                + (float(time_obj.second) / 60.0)
            )
            return total_minutes
        case _:
            return


# handles date conversion for date formats in schedule and player data
def date_change(date: str, is_player: bool = False) -> str:
    # use datetime library for ensuring all dates are compared in the same form, see https://docs.python.org/3/library/datetime.html#format-codes
    if is_player:
        # parse date info
        parsed_date = datetime.strptime(date, "%Y-%m-%d")
    elif not is_player:
        # parse date info
        parsed_date = datetime.strptime(date, "%a, %b %d, %Y")

    # reformat date
    new_date = parsed_date.strftime("%m/%d/%y")
    # return updated date
    return new_date


# reformats player age from format "Year-days", to years as a float
def reformat_player_age(age_as_str: str) -> float:
    player_year_search = re.search(r"(\d+)-", age_as_str)
    if not player_year_search:
        return

    year = float(player_year_search.group(1))

    player_day_search = re.search(r"-(\d+)", age_as_str)
    if not player_day_search:
        return

    day = float(player_year_search.group(1))

    year_length = 365.0

    year_as_float = year + (day / year_length)

    return year_as_float


# reformats Win_loss_margin to be a floating point, and removes the W/L, which can be easily determined
def reformat_win_loss_margin(game_result: str) -> float:
    result_search = re.search(r"((\d+))", game_result)
    if not result_search:
        return

    # get win/loss margin from result_search; of form +int or -int
    win_loss_margin = result_search.group(1)

    win_search = re.search(r"+", win_loss_margin)

    if not win_search:

        loss_search = re.search(r"-", win_loss_margin)

        if not loss_search:
            return

        loss_data = re.search(r"-(\d+)", loss_search.group(1))

        if not loss_data:
            return

        return float(loss_data.group(1))

    win_data = re.search(r"+(\d+)", win_search.group(1))

    if not win_data:
        return

    return float(win_data.group(1))


# assigns all players a unique integer label for later model training; takes in a list of player dictionaries and returns a modified dictionary with the int label
def player_label(player_name_url_list: list) -> list:

    # range to pull random numbers from; ~4800 NBA players ever; want to avoid simply assigning ascending numbers according to players in alphabetical order
    start_int_range: int = 1
    range_scale: int = 20
    end_int_range: int = range_scale * len(player_name_url_list)

    # create list of random integer pulling from the population within the range; no repeats
    random_numbers = random.sample(
        range(start_int_range, end_int_range + 1), len(player_name_url_list)
    )

    # assigns random int within the specified range to each player
    for i, player_data in player_name_url_list:
        if player_data.get("player_label"):
            print(rf"Player already has a player_label")
            continue

        player_name_url_list[i]["player_label"] = random_numbers[i]

    # write to text file
    with open(
        rf"C:\Users\Michael\Code\Python\Data_scraping\unique_player_labels\labeled_players.json",
        "w",
    ) as file:
        file.write(player_name_url_list)

    # pickle data
    pickle_data(
        player_name_url_list,
        rf"C:\Users\Michael\Code\Python\Data_scraping\unique_player_labels\labeled_players.pkl",
    )

    return player_name_url_list


# converts player weight and height to metric as a float
def convert_player_info_to_metric(player_dict_objects: list):
    for i, player in enumerate(player_dict_objects):
        if not player.get("height") or not player.get("weight"):
            print(
                rf"Error converting weight and height to metric for {player.get("player")}"
            )
            return

        feet_search = re.search(r"(\d+)-", player)
        if feet_search:
            feet_to_cm = float(feet_search.group(1)) * 30.48

        inches_search = re.search(r"-(\d+)", player)
        if inches_search:
            inches_to_cm = float(feet_search.group(1)) * 2.54

        if not feet_to_cm or not inches_to_cm:
            print(
                rf"Error converting weight and height to metric for {player.get("player")}"
            )
            return

        # height changed to cm
        player_dict_objects[i]["height"] = feet_to_cm + inches_to_cm
        # weight changed to kg
        player_dict_objects[i]["weight"] = float(player.get("weight")) * 0.453592


# find players based on the seasons that they have played, pulling from html data already saved using the find_players function; returns list of dictionaries
def find_players_by_year(
    start_letter: str, end_letter: str, start_year: int, end_year: int
) -> list:
    # create array of letters
    alphabet_range = [chr(i) for i in range(ord(start_letter), ord(end_letter) + 1)]

    # list of dictionaries to contain player names, int label, and url extension
    player_names_with_url = []

    # take data from saved location
    for letter in alphabet_range:
        # define file location
        player_last_name_letter_file = rf"C:\Users\Michael\Code\Python\Data_scraping\alphabetic_players_grouped\letter_{letter}_players.html"

        # check if the html file exists
        if not os.path.isfile(player_last_name_letter_file):
            web_driver = initialize_selenium_driver()
            base_player_url = "https://www.basketball-reference.com/players/"
            # pass the full url after appending to the end of the baseline url from list, along with file save location
            selenium_request(
                firefox_driver=web_driver,
                request_url=rf"{base_player_url}{letter}",
                save_html=True,
                file_path=player_last_name_letter_file,
            )

            # quit webdriver
            web_driver.quit()

        # open html file
        try:
            with open(
                rf"C:\Users\Michael\Code\Python\Data_scraping\alphabetic_players_grouped\letter_{letter}_players.html",
                "r",
                encoding="utf-8",
            ) as file:
                contents = file.read()
        except Exception as e:
            basic_error_handling(e)

        soup = BeautifulSoup(contents, "html.parser")

        # find all player tables; should only be one
        all_tables = soup.find_all("table", id="players")
        # list of dictionary objects; each represents a player
        dict_table_data = []

        # iterate across the list of found table labels and
        for table_info in all_tables:
            tables_info_to_dict = table_to_dictionary(table_info)
            if tables_info_to_dict is not None:
                dict_table_data = [*dict_table_data, *tables_info_to_dict]

        # save all players of a given letter into a JSON file
        if dict_table_data:
            with open(
                rf"C:\Users\Michael\Code\Python\Data_scraping\alphabetic_players_grouped\letter_{letter}_players.json",
                "w",
            ) as file:
                file.write(dict_table_data)
        elif not dict_table_data:
            print(rf"Error with Json_data writing for letter {letter}")

        # iterates through the table data dictionary for players of a given last name starting letter
        for player_object in dict_table_data:

            # gets the years played bounds
            lower_year_bound = int(player_object["year_min"])
            upper_year_bound = int(player_object["year_max"])

            # checks the years played by the player
            if (lower_year_bound >= start_year or upper_year_bound >= start_year) and (
                lower_year_bound <= end_year
            ):
                # if the player played during the years specified, add the info to the dictionary, then add that dict to player_names_with_url
                temp_player_name_url_dict = {}
                if player_object.get("player"):
                    temp_player_name_url_dict["player"].append(
                        player_object.get("player")
                    )
                else:
                    temp_player_name_url_dict["player"].append(None)

                if player_object.get("player_url"):
                    # url list should always contain a single url
                    temp_player_name_url_dict["player_url"].append(
                        player_object.get("player_url")
                    )
                else:
                    temp_player_name_url_dict["player_url"].append(None)
                # add to overall player dictionary list
                player_names_with_url.append(temp_player_name_url_dict)

    # run all players through player label function to be assigned a random number
    labeled_players = player_label(player_names_with_url)

    # returns a list containing the player name with part of the url to navigate to their data page
    return labeled_players


# retrieve the player season statistics for all games in a given range of seasons using a list containing dictionaries of player info
def get_player_season_stats(player_name_with_url_list: list, season_range: range):
    # make list from year range
    season_list = list(season_range)

    # iterate over list of player info [ player name, player url]
    for player_info in player_name_with_url_list:

        # player html save file location
        player_html_file = rf"C:\Users\Michael\Code\Python\Data_scraping\player_specific_data\{player_info[0]}_data.html"

        if not os.path.isfile(player_html_file):
            web_driver = initialize_selenium_driver()
            # base url
            baseline_url = "https://www.basketball-reference.com"
            # pass the full url after appending to the end of the baseline url from list, along with file save location
            selenium_request(
                firefox_driver=web_driver,
                request_url=rf"{baseline_url}{player_info[2]}",
                save_html=True,
                file_path=player_html_file,
            )

            # quit web driver
            web_driver.quit()

        # open player html page
        with open(
            player_html_file,
            "r",
            encoding="utf-8",
        ) as file:
            contents = file.read()

        # make the soup
        soup_1 = BeautifulSoup(contents, "html.parser")
        # find the table of all season stats
        table_1 = soup_1.find("table", id="per_game_stats")

        # iterate over year range
        for season_year in season_list:
            # used to ensure that a season is only saved once since some pages contain multiple lines for the same year, containing the same data
            was_year_saved = False

            for element in table_1.find("tbody").find_all("tr"):
                # ends table loop looking for a given year if already found
                if was_year_saved:
                    break

                # check to see if element ends up being NoneType / None. jumps to next for-loop iteration
                search_headers = element.find("th")
                if search_headers is None:
                    print(rf"No headers found for {player_info[0]} in {season_year}")
                    continue
                search_hyperlink = search_headers.find("a")
                if search_hyperlink is None:
                    print(
                        rf"No year hyperlink data found for {player_info[0]} in {season_year}"
                    )
                    continue

                # use re to collect the number for start year by removing the information after the hyphen
                table_year = re.search(
                    r"(\d+)-", element.find("th").find("a").get_text()
                ).group(1)

                if table_year:
                    # convert to int and compare to season_start_year and ensure that the year has not been saved yet
                    if int(table_year) == season_year:
                        # assigns the year_url from the href data if the year is correct
                        year_url = element.find("th").a["href"]

                        # get html data from specific season
                        page_contents = selenium_request(
                            firefox_driver=web_driver,
                            request_url=rf"{baseline_url}{year_url}",
                        )
                        # Parse the HTML with BeautifulSoup
                        soup_2 = BeautifulSoup(page_contents, "html.parser")

                        # find the 'pgl_basic' table by its tag and ID
                        table_2 = soup_2.find("table", id="pgl_basic")
                        # find the 'pgl_basic_playoffs' table by its tag and ID
                        table_3 = soup_2.find("table", id="pgl_basic_playoffs")

                        # list for headers
                        headers = []
                        # list for row data
                        rows = []

                        # adds on the player metrics to the DataFrame (height, weight, etc...)

                        # determines if the regular season table (table_2) exists in the html
                        if table_2:
                            # add player metric headers to list first
                            for metric_headers in list(player_metrics_series.index):
                                headers.append(metric_headers)

                            # extract headers with improved handling for whitespace and non-breaking spaces
                            for table_header in table_2.find("thead").find_all("th"):
                                # remove whitespace
                                header = (
                                    table_header.get(
                                        "data-tip", table_header.string.strip()
                                    )
                                    .replace("\xa0", " ")
                                    .replace('"', " ")
                                    .strip()
                                )
                                # if header is still empty after stripping whitespace
                                if not header:
                                    header = table_header.get(
                                        "data-stat", "Unknown Header"
                                    )

                                headers.append(header)

                        # determines if the table exists in the html
                        if table_2:
                            # extract rows, skipping any repeated header rows and ensuring only valid data rows; records the games for the regular season
                            for row in table_2.find_all("tr"):
                                # add player metric data to each row (for ease of use later for player/game comparisons)
                                metric_row_data = list(player_metrics_series.values)

                                cells = [
                                    td.get_text().replace("\xa0", " ").replace('"', " ")
                                    for td in row.find_all(["th", "td"])
                                ]

                                # concatenate row lists with * operator
                                metric_and_stats_row = [*metric_row_data, *cells]

                                # Only add rows with the correct number of columns (matching the header count)
                                if len(metric_and_stats_row) == len(headers):
                                    rows.append(metric_and_stats_row)

                        # determines if the table exists in the html
                        if table_3:
                            # append the playoff games to the row data if the player made it to the playoffs that season
                            for row in table_3.find_all("tr"):
                                # add player metric data to each row (for ease of use later for player/game comparisons)
                                metric_row_data = list(player_metrics_series.values)

                                cells = [
                                    td.get_text().replace("\xa0", " ").replace('"', " ")
                                    for td in row.find_all(["th", "td"])
                                ]

                                # concatenate row lists with * operator
                                metric_and_stats_row = [*metric_row_data, *cells]

                                # Only add rows with the correct number of columns (matching the header count)
                                if len(metric_and_stats_row) == len(headers):
                                    rows.append(metric_and_stats_row)
                        else:
                            print(rf"no play-off data for {player_info[0]}")

                        # error testing
                        # print(headers)
                        # print(rows)

                        # headers are stored in a way that does not make them the first row in the DataFrame
                        season_df = pd.DataFrame(rows, columns=headers)
                        # drop duplicate data rows, if not already properly done in the row for loop
                        season_df = season_df.drop_duplicates()
                        # drops the first row of numbers imported and then resets the indexes
                        season_df = season_df.drop(index=0).reset_index(drop=True)
                        # drop column with no useful data
                        season_df = season_df.drop(columns={"Rank", "Season Game"})
                        # rename a handful of columns
                        season_df = season_df.rename(
                            columns={
                                "Player's age on February 1 of the season": "Player_age",
                                "game_location": "Game_location",
                                "game_result": "Win_loss_margin",
                            }
                        )
                        # change the default "@" in the location column
                        season_df["Game_location"] = season_df["Game_location"].apply(
                            lambda x: "Away" if x == "@" else "Home"
                        )
                        # fix date format for ease of comparison later
                        season_df["Date"] = season_df["Date"].apply(
                            lambda x: date_change(date=x, is_player=True)
                        )
                        # change game started from 0/1 to T/F boolean
                        season_df["Games Started"] = season_df["Games Started"].apply(
                            lambda x: True if int(x) == 1 else False
                        )

                        # *** change all numbers to floating points for later manipulation

                        # reformat win/loss margin as float
                        season_df["Win_loss_margin"] = season_df[
                            "Win_loss_margin"
                        ].apply(lambda x: reformat_win_loss_margin(x))
                        # fix age format to a floating point
                        season_df["Player_age"] = season_df["Player_age"].apply(
                            lambda x: reformat_player_age(x)
                        )
                        # fix minutes played to a floating point
                        season_df["Minutes Played"] = season_df["Minutes Played"].apply(
                            lambda x: playtime_conversion(x)
                        )

                        # change several columns to floats all at once
                        season_df[
                            [
                                "Field Goals",
                                "Field Goal Attempts",
                                "Field Goal Percentage",
                                "3-Point Field Goals",
                                "3-Point Field Goal Attempts",
                                "3-Point Field Goal Percentage",
                                "Free Throws",
                                "Free Throw Attempts",
                                "Free Throw Percentage",
                                "Offensive Rebounds",
                                "Defensive Rebounds",
                                "Total Rebounds",
                                "Assists",
                                "Steals",
                                "Blocks",
                                "Turnovers",
                                "Personal Fouls",
                                "Points",
                                "Game Score",
                            ]
                        ] = season_df[
                            [
                                "Field Goals",
                                "Field Goal Attempts",
                                "Field Goal Percentage",
                                "3-Point Field Goals",
                                "3-Point Field Goal Attempts",
                                "3-Point Field Goal Percentage",
                                "Free Throws",
                                "Free Throw Attempts",
                                "Free Throw Percentage",
                                "Offensive Rebounds",
                                "Defensive Rebounds",
                                "Total Rebounds",
                                "Assists",
                                "Steals",
                                "Blocks",
                                "Turnovers",
                                "Personal Fouls",
                                "Points",
                                "Game Score",
                            ]
                        ].apply(
                            lambda x: float(x)
                        )

                        # save to CSV, removing row indexes and keeping the headers
                        season_df.to_csv(
                            rf"C:\Users\Michael\Code\Python\Data_scraping\player_csv\{season_year}_{player_info[0]}.csv",
                            index=False,
                            header=True,
                        )
                        # updates to record that that year's season was saved
                        was_year_saved = True

                        print(
                            rf"Game log data saved to {season_year}_{player_info[0]}.csv"
                        )
                else:
                    print(
                        rf"No player season data found for {player_info[0]} in {season_year}"
                    )


# used to find full game schedules for the years in the given range
def full_games_schedule(start_year: int, end_year: int) -> DataFrame:
    # web driver
    web_driver = initialize_selenium_driver()
    # list containing all DataFrames with each season's data; contains data of form: [year, season_schedule_df]
    seasons_schedules_headers = ["Year", "Season_schedule_df"]
    all_seasons_schedules_dfs = pd.DataFrame(columns=seasons_schedules_headers)

    # iterate over year range
    for year in range(start_year, (end_year + 1)):

        # save the html data for the page; corrects for difference in url and season start year
        selenium_request(
            firefox_driver=web_driver,
            request_url=rf"https://www.basketball-reference.com/leagues/NBA_{(year + 1)}_games.html",
            save_html=True,
            file_path=rf"C:\Users\Michael\Code\Python\Data_scraping\season_schedule\{year}_schedule.html",
        )

        # open file containing the HTML data (with error handles)
        with open(
            rf"C:\Users\Michael\Code\Python\Data_scraping\season_schedule\{year}_schedule.html",
            "r",
            encoding="utf-8",
        ) as file:
            contents = file.read()

        # make soup
        soup = BeautifulSoup(contents, "html.parser")
        # find the div containing the month urls for a given season
        month_data = soup.find("div", class_="filter")

        # added to help with debugging
        if month_data is None:
            print(f"No month data found for year {year}. Skipping...")
            continue

        # initialize headers_done to minimize redundant task of collecting table headers
        headers_done = False
        # list for headers
        headers = []
        # list for row data
        rows = []

        for month in month_data.find_all("a", href=True):
            # base site url
            base_url = "https://www.basketball-reference.com"
            # obtains the html data for the given month of the season
            season_month_data = selenium_request(
                firefox_driver=web_driver, request_url=rf"{base_url}{month['href']}"
            )
            # make soup
            soup_2 = BeautifulSoup(season_month_data, "html.parser")
            # find table with season data
            table = soup_2.find("table", id="schedule")

            # added to help with debugging
            if table is None:
                print(
                    f"No table found for month {month['href']} in year {year}. Skipping..."
                )
                continue

            if not headers_done:
                # extract headers with improved handling for whitespace and non-breaking spaces
                for table_header in table.find("thead").find_all("th"):
                    # remove whitespace
                    header = (
                        table_header.get("data-tip", table_header.string.strip())
                        .replace("\xa0", " ")
                        .replace('"', " ")
                        .strip()
                    )
                    # if header is still empty after stripping whitespace
                    if not header:
                        header = table_header.get("data-stat", "Unknown Header")

                    headers.append(header)
                # update headers_done
                headers_done = True

            # extract rows, skipping any repeated header rows and ensuring only valid data rows; records the games for the regular season
            for row in table.find("tbody").find_all("tr"):
                # skip rows that have header information
                if "thead" in row.get("class", []):
                    print("skipped a row")
                    continue
                else:
                    cells = [
                        td.get_text().replace("\xa0", " ").replace('"', " ")
                        for td in row.find_all(["th", "td"])
                    ]
                    # Only add rows with the correct number of columns (matching the header count)
                    if len(cells) == len(headers):
                        rows.append(cells)

        # make DataFrame for the given season schedule
        season_schedule_df = pd.DataFrame(rows, columns=headers)
        # drop duplicate data rows, if not already properly done in the row for-loop
        season_schedule_df = season_schedule_df.drop_duplicates()
        # rename away/home headers along with the point columns to be easier to work with later
        season_schedule_df = season_schedule_df.rename(
            columns={
                "Visitor/Neutral": "Away",
                "Home/Neutral": "Home",
                "Points": "Away_points",
                "PTS": "Home_points",
            }
        )
        # remove a handful of columns that do not have useful data
        season_schedule_df = season_schedule_df.drop(
            columns={"Notes", "box_score_text", "overtimes", "Length of Game"}
        )

        # full team names to their abbreviations in Home
        season_schedule_df["Home"] = season_schedule_df["Home"].apply(
            lambda x: full_to_abbreviation(x, year)
        )
        # full team names to their abbreviations in Away
        season_schedule_df["Away"] = season_schedule_df["Away"].apply(
            lambda x: full_to_abbreviation(x, year)
        )
        # fix date format
        season_schedule_df["Date"] = season_schedule_df["Date"].apply(
            lambda x: date_change(date=x, is_player=False)
        )

        # temp list to make season DataFrame
        df_list = [[year, season_schedule_df]]
        # creates new DataFrame that contains both the season year and season schedule using the same headers as all_season_schedules to concatenate
        total_season_info_df = pd.DataFrame(df_list, columns=seasons_schedules_headers)
        # add total_season_info_df to the full DataFrame of all seasons
        all_seasons_schedules_dfs = pd.concat(
            [all_seasons_schedules_dfs, total_season_info_df]
        )
        # save to CSV, removing row indexes and keeping the headers
        season_schedule_df.to_csv(
            rf"C:\Users\Michael\Code\Python\Data_scraping\season_schedule\{year}_season_games.csv",
            index=False,
            header=True,
        )

        print(rf"Season data saved to {year}_season_games.csv")

    # quit web driver
    web_driver.quit()
    return all_seasons_schedules_dfs


# takes in a list of season schedules; assumes you already have all necessary player data saved for access; this returns a russian doll of DataFrames
def collect_players_in_game(year_range: range) -> pd.DataFrame:
    year_list = list(year_range)
    aggregate_headers = ["Season_year", "Season_game_data"]
    aggregate_of_all_game_info_df = pd.DataFrame(columns=aggregate_headers)

    # *** capitalized "Index" matters for accessing it as a label in a named tuple created from .itertuples() method

    for schedule_year in year_list:
        # open season schedule
        schedule_path = Path(
            f"C:/Users/Michael/Code/Python/Data_scraping/season_schedule/{schedule_year}_season_games.csv"
        )
        season_game_schedule_df = pd.read_csv(schedule_path)

        all_game_headers = ["Game_date", "Home_team_stats", "Away_team_stats"]
        all_game_info_df = pd.DataFrame(columns=all_game_headers)

        # initialize DataFrames
        for game_from_schedule in season_game_schedule_df.itertuples():

            # collect team stats
            home_team_aggregate_series = pd.Series(
                {
                    "Team": game_from_schedule.Home,
                    "Score": game_from_schedule.Home_points,
                    "Team_win": game_from_schedule.Home_points
                    > game_from_schedule.Away_points,
                    "Players_game_stats": pd.DataFrame(),
                }
            )
            away_team_aggregate_series = pd.Series(
                {
                    "Team": game_from_schedule.Away,
                    "Score": game_from_schedule.Away_points,
                    "Team_win": game_from_schedule.Away_points
                    > game_from_schedule.Home_points,
                    "Players_game_stats": pd.DataFrame(),
                }
            )

            # add game data
            temp_game_df = pd.DataFrame(
                {
                    "Game_date": [game_from_schedule.Date],
                    "Home_team_stats": [home_team_aggregate_series],
                    "Away_team_stats": [away_team_aggregate_series],
                }
            )
            all_game_info_df = pd.concat(
                [all_game_info_df, temp_game_df], ignore_index=True
            )

        temp_season_df = pd.DataFrame(
            {"Season_year": [schedule_year], "Season_game_data": [all_game_info_df]}
        )
        aggregate_of_all_game_info_df = pd.concat(
            [aggregate_of_all_game_info_df, temp_season_df], ignore_index=True
        )

    # Process player data for all games
    for season_year_data in aggregate_of_all_game_info_df.itertuples():
        season_year = season_year_data.Season_year
        folder_path = Path("C:/Users/Michael/Code/Python/Data_scraping/player_csv")

        # finds all files that contain the wildcard *TEXT*.csv and iterates over them
        for file in folder_path.glob(f"*{season_year}*.csv"):
            player_csv_df = pd.read_csv(file)

            # error checking
            # print(type(player_csv_df))
            # print(player_csv_df.head())

            for player_game_data in player_csv_df.itertuples():
                for game_data in season_year_data.Season_game_data.itertuples():
                    if player_game_data.Date == game_data.Game_date:
                        # print verification
                        print("DATE MATCHED")

                        # add player data to home or away team

                        # home team
                        if (
                            game_data.Home_team_stats["Team"].strip()
                            == player_game_data.Team.strip()
                        ):

                            # navigating nested data is very irritating
                            # checking for issues within home team
                            assert isinstance(
                                aggregate_of_all_game_info_df.loc[
                                    season_year_data.Index,
                                    "Season_game_data",
                                ]
                                .loc[game_data.Index, "Home_team_stats"]
                                .loc["Players_game_stats"],
                                pd.DataFrame,
                            ), "Players_game_stats should be a DataFrame"
                            # print verification
                            print("HOME DATAFRAME ASSERTION TRUE")

                            # *** capitalized "Index" matters for accessing it as a label in a named tuple created from .itertuples() method

                            # stores a boolean for if there is an empty DataFrame within Players_game_stats for the home team
                            players_game_stats_home_location_check = (
                                aggregate_of_all_game_info_df.loc[
                                    season_year_data.Index,
                                    "Season_game_data",
                                ]
                                .loc[game_data.Index, "Home_team_stats"]
                                .loc["Players_game_stats"]
                                .empty
                            )

                            # runs if the DataFrame is empty
                            if players_game_stats_home_location_check:
                                # takes the same headers as what are in player_csv_df
                                # add the relevant player stats along with the headers
                                temp_player_df = pd.DataFrame(
                                    [player_csv_df.loc[player_game_data.Index]]
                                )
                                # write over the empty DataFrame
                                # sometimes .at is necessary in place of .loc
                                aggregate_of_all_game_info_df.at[
                                    season_year_data.Index,
                                    "Season_game_data",
                                ].at[game_data.Index, "Home_team_stats"].at[
                                    "Players_game_stats"
                                ] = temp_player_df
                            # runs if the DataFrame has player information already
                            elif not players_game_stats_home_location_check:
                                # takes the same headers as what are in player_csv_df
                                # add the relevant player stats along with the headers
                                temp_player_df = pd.DataFrame(
                                    [player_csv_df.loc[player_game_data.Index]]
                                )

                                # concatenate with the DataFrame already there
                                aggregate_of_all_game_info_df.at[
                                    season_year_data.Index,
                                    "Season_game_data",
                                ].at[game_data.Index, "Home_team_stats"].at[
                                    "Players_game_stats"
                                ] = pd.concat(
                                    [
                                        aggregate_of_all_game_info_df.loc[
                                            season_year_data.Index,
                                            "Season_game_data",
                                        ]
                                        .loc[game_data.Index, "Home_team_stats"]
                                        .loc["Players_game_stats"],
                                        temp_player_df,
                                    ],
                                    ignore_index=True,
                                )

                        # away team
                        if (
                            game_data.Away_team_stats["Team"].strip()
                            == player_game_data.Team.strip()
                        ):

                            # navigating nested data is very irritating
                            # checking for issues within home team
                            assert isinstance(
                                aggregate_of_all_game_info_df.loc[
                                    season_year_data.Index,
                                    "Season_game_data",
                                ]
                                .loc[game_data.Index, "Away_team_stats"]
                                .loc["Players_game_stats"],
                                pd.DataFrame,
                            ), "Players_game_stats should be a DataFrame"
                            # print verification
                            print("AWAY DATAFRAME ASSERTION TRUE")

                            # stores a boolean for if there is an empty DataFrame within Players_game_stats for the away team
                            players_game_stats_away_location_check = (
                                aggregate_of_all_game_info_df.loc[
                                    season_year_data.Index,
                                    "Season_game_data",
                                ]
                                .loc[game_data.Index, "Away_team_stats"]
                                .loc["Players_game_stats"]
                                .empty
                            )

                            # runs if the DataFrame is empty
                            if players_game_stats_away_location_check:
                                # takes the same headers as what are in player_csv_df
                                # add the relevant player stats along with the headers
                                temp_player_df = pd.DataFrame(
                                    [player_csv_df.loc[player_game_data.Index]]
                                )
                                # write over the empty DataFrame
                                # sometimes .at is necessary in place of .loc
                                aggregate_of_all_game_info_df.at[
                                    season_year_data.Index,
                                    "Season_game_data",
                                ].at[game_data.Index, "Away_team_stats"].at[
                                    "Players_game_stats"
                                ] = temp_player_df
                            # runs if the DataFrame has player information already
                            elif not players_game_stats_away_location_check:
                                # takes the same headers as what are in player_csv_df
                                # add the relevant player stats along with the headers
                                temp_player_df = pd.DataFrame(
                                    [player_csv_df.loc[player_game_data.Index]]
                                )

                                # concatenate with the DataFrame already there
                                aggregate_of_all_game_info_df.at[
                                    season_year_data.Index,
                                    "Season_game_data",
                                ].at[game_data.Index, "Away_team_stats"].at[
                                    "Players_game_stats"
                                ] = pd.concat(
                                    [
                                        aggregate_of_all_game_info_df.loc[
                                            season_year_data.Index,
                                            "Season_game_data",
                                        ]
                                        .loc[game_data.Index, "Away_team_stats"]
                                        .loc["Players_game_stats"],
                                        temp_player_df,
                                    ],
                                    ignore_index=True,
                                )

    # for season_year_data in aggregate_of_all_game_info_df.itertuples():
    #    for game_data in season_year_data.Season_game_data.itertuples():
    #        # error testing
    #        print(
    #            list(
    #                aggregate_of_all_game_info_df.loc[
    #                    season_year_data.Index,
    #                    "Season_game_data",
    #                ]
    #                .loc[game_data.Index, "Home_team_stats"]
    #                .loc["Players_game_stats"]
    #                .columns
    #            )
    #        )

    # pickle data for easy access later using pandas method specifically to help maintain data types and structure
    aggregate_of_all_game_info_df.to_pickle(
        rf"C:\Users\Michael\Code\Python\Data_scraping\pickled_data\All_seasons_game_data_df.pkl"
    )

    return aggregate_of_all_game_info_df


# take pickled DataFrame specifically from collect_players_in_game and convert to a csv to for easy readability
def pickled_players_in_games_to_csv():
    pickled_file_path = rf"C:\Users\Michael\Code\Python\Data_scraping\pickled_data\All_seasons_game_data_df.pkl"

    # read the pickled data into a DataFrame
    all_seasons_df = pd.read_pickle(pickled_file_path)

    # the DataFrame has data of the nested form:
    # pickled_data Headers: ["Season_year", "Season_game_data"]
    #       Season_year: int, Season_game_data: pd.DataFrame
    # Season_game_data Headers: ["Game_date", "Home_team_stats", "Away_team_stats"]
    #       Game_date: str = DD/MM/YY, Home_team_stats: pd.Series, Away_team_stats: pd.Series
    # Home/Away _team_stats Headers/labels: ["Team", "Score", "Team_win", "Players_game_stats"]
    #       Team: str, Score: int, Team_win: bool, Players_game_stats: pd.DataFrame
    # Players_game_stats Headers: *Same Headers as those found in {season_year}_{player_info[0]}.csv, for get_player_season_stats

    # iterate through season years
    for season_data in all_seasons_df.itertuples():
        with open(
            rf"C:\Users\Michael\Code\Python\Data_scraping\pickled_data\{season_data.Season_year}_season_compiled.csv",
            "w",
            newline="",
        ) as file:

            # lots of perceivably "extra" brackets[] in the following region. To future self: I assure you, they are not extra. Keep them if you want the csv to format properly.

            # iterate over the games in a given season
            for game_data in season_data.Season_game_data.itertuples():

                # error testing
                # print(
                #    all_seasons_df.loc[
                #        season_data.Index,
                #        "Season_game_data",
                #   ]
                #    .loc[game_data.Index, "Home_team_stats"]
                #   .loc["Players_game_stats"]
                #    .columns
                # )

                # start writing into the csv file
                writer = csv.writer(file)

                # write in the season_year
                writer.writerow(
                    [season_data._fields[season_data._fields.index("Season_year")]]
                )
                writer.writerow([season_data.Season_year])

                # blank line
                writer.writerow([])

                # write in game date
                writer.writerow(
                    [game_data._fields[game_data._fields.index("Game_date")]]
                )
                writer.writerow([game_data.Game_date])

                # blank line
                writer.writerow([])

                # .get_loc gives the integer location for a label/index, while .loc[] allows you to directly access the data at that point

                # home team stats

                # team name
                writer.writerow(
                    [
                        list(game_data.Home_team_stats.index)[
                            game_data.Home_team_stats.index.get_loc("Team")
                        ]
                    ]
                )
                writer.writerow([game_data.Home_team_stats.Team])

                # team score
                writer.writerow(
                    [
                        list(game_data.Home_team_stats.index)[
                            game_data.Home_team_stats.index.get_loc("Score")
                        ]
                    ]
                )
                writer.writerow([game_data.Home_team_stats.Score])

                # team_win
                writer.writerow(
                    [
                        list(game_data.Home_team_stats.index)[
                            game_data.Home_team_stats.index.get_loc("Team_win")
                        ]
                    ]
                )
                writer.writerow([game_data.Home_team_stats.Team_win])

                # take note that upon serialization and deserialization (pickling), nested structures (DataFrames) can lose their original type fidelity when accessed via itertuples().

                # print(type(game_data.Home_team_stats.Players_game_stats))
                # print(game_data.Home_team_stats.Players_game_stats)

                # team player stats

                # write the headers
                writer.writerows(
                    [
                        all_seasons_df.loc[
                            season_data.Index,
                            "Season_game_data",
                        ]
                        .loc[game_data.Index, "Home_team_stats"]
                        .loc["Players_game_stats"]
                        .columns
                    ]
                )

                # write the table data (.values returns an array of the table data)
                writer.writerows(
                    all_seasons_df.loc[
                        season_data.Index,
                        "Season_game_data",
                    ]
                    .loc[game_data.Index, "Home_team_stats"]
                    .loc["Players_game_stats"]
                    .values
                )

                # blank line
                writer.writerow([])

                # away team stats

                # team name
                writer.writerow(
                    [
                        list(game_data.Away_team_stats.index)[
                            game_data.Away_team_stats.index.get_loc("Team")
                        ]
                    ]
                )
                writer.writerow([game_data.Away_team_stats.Team])

                # team score
                writer.writerow(
                    [
                        list(game_data.Away_team_stats.index)[
                            game_data.Away_team_stats.index.get_loc("Score")
                        ]
                    ]
                )
                writer.writerow([game_data.Away_team_stats.Score])

                # team_win
                writer.writerow(
                    [
                        list(game_data.Away_team_stats.index)[
                            game_data.Away_team_stats.index.get_loc("Team_win")
                        ]
                    ]
                )
                writer.writerow([game_data.Away_team_stats.Team_win])

                # team player stats

                # write the headers
                writer.writerows(
                    [
                        all_seasons_df.loc[
                            season_data.Index,
                            "Season_game_data",
                        ]
                        .loc[game_data.Index, "Away_team_stats"]
                        .loc["Players_game_stats"]
                        .columns
                    ]
                )

                # write the table data (.values returns an array of the table data)
                writer.writerows(
                    all_seasons_df.loc[
                        season_data.Index,
                        "Season_game_data",
                    ]
                    .loc[game_data.Index, "Away_team_stats"]
                    .loc["Players_game_stats"]
                    .values
                )

                # blank line
                writer.writerow([])

                # error check
                if (
                    all_seasons_df.loc[
                        season_data.Index,
                        "Season_game_data",
                    ]
                    .loc[game_data.Index, "Away_team_stats"]
                    .loc["Players_game_stats"]
                    .empty
                ):

                    print("TEAM DATA MISSING!!")
