import requests
from bs4 import BeautifulSoup
import pandas as pd
from pandas import DataFrame
import re
import time
import pickle
from pathlib import Path
from datetime import datetime

# contains all the functions necessary for Data_scraping on https://www.basketball-reference.com

# handles http GET requests and returns the page contents as bytes to "make soup"
def get_response_data(url: str) -> bytes:
    response = requests.get(url)
    # ensure requests are not made too frequent
    time.sleep(5)
    if response.status_code == 200:
        # to deal with non-breaking spaces in conversion of HTML and ensure UTF-8 encoding
        response.encoding = 'utf-8'
        return response.content
    else:
        print("Failed to retrieve the page. Status code:", response.status_code)

# pass a url and file save path to save all html data to a file
def get_html(url: str, path: str):
    # ensure requests are not made too frequent
    time.sleep(5)
    response = requests.get(url)
    if response.status_code == 200:
        # open the file path and write the contents into the file (with error handles)
        with open(path, 'w', encoding='utf-8') as file:
            file.write(response.text)
    else: 
        print("Failed to retrieve the page. Status code:", response.status_code)
        
# pass alphabet range and year range for search for player metrics/statistics
def find_players(start_letter: str, end_letter: str):
    all_players_url = "https://www.basketball-reference.com/players/"
    # create array of letters
    alphabet_range = [chr(i) for i in range(ord(start_letter), ord(end_letter) + 1)]

    for letter in alphabet_range:
        # concatenate each letter to make the new url's to browse through all players
        new_url = rf"{all_players_url}{letter}"
        # pass the url information to save the html data
        get_html(new_url, rf'C:\Users\Michael\Code\Python\Data_scraping\alphabetic_players_grouped\letter_{letter}_data.html')
        # sets a delay of a few seconds to try and space the number of requests to avoid 426 error
        time.sleep(10)

# find players based on the seasons that they have played, pulling from html data already saved using the find_players function
def find_players_by_year(start_letter: str, end_letter: str, start_year: int, end_year: int) -> list :
    # create array of letters
    alphabet_range = [chr(i) for i in range(ord(start_letter), ord(end_letter) + 1)]
    
    # empty array to collect player names
    player_names_with_url = []
    
    # take data from saved location
    for letter in alphabet_range:
        
        # open file containing the HTML data (with error handles)
        with open(rf'C:\Users\Michael\Code\Python\Data_scraping\alphabetic_players_grouped\letter_{letter}_data', 'r', encoding='utf-8') as file:
            contents = file.read()
        
        soup = BeautifulSoup(contents, "html.parser")
        table = soup.find("table", id="players").find("tbody")
        
        # iterates through the table data for players of a given last name starting letter
        for element in table.find_all("tr"):
            # gets the years played bounds
            lower_year_bound = int(element.find('td', {'data-stat': 'year_min'}).get_text())
            upper_year_bound = int(element.find('td', {'data-stat': 'year_max'}).get_text())
            
            # checks the years played by the player
            if (lower_year_bound >= start_year or upper_year_bound >= start_year) and ( lower_year_bound <= end_year) :
                # if the player played during the years specified, create a list containing the name and url, then add that list to player_names_with_url
                player_names_with_url.append([element.find('th', {'data-stat': 'player'}).find('a').get_text(), element.find('th', {'data-stat': 'player'}).find('a')['href']])
    
    # returns a list containing the player name with part of the url to navigate to their data page
    return player_names_with_url
                
# retrieve the player metrics by passing the list containing the name and url of the player
def get_player_metrics(player_name_with_url: list) -> DataFrame:
    
    baseline_url = "https://www.basketball-reference.com"
     
    # pass the full url after appending to the end of the baseline url from list
    page_data = get_response_data(rf'{baseline_url}{player_name_with_url[1]}')
    # make the soup
    soup = BeautifulSoup(page_data, 'html.parser')
    
    # find specific branch of HTML data
    full_player_stats = soup.find('div', id='meta')
    
    # retrieve the str of data, ensure a space between lines/words, replace strange characters 
    player_metric_string = full_player_stats.get_text(separator=" ").replace(u'\xa0', ' ').replace(u'\u25aa', '').strip()
    
    # normalize whitespace to a single space; split() breaks the string into words by whitespace; join() fuses them back together with only a single whitespace between each word
    single_line_output = ' '.join(player_metric_string.split())
    
    # array of strings containing player info, starting with their name
    player_metrics = [player_name_with_url[0]]
    
    # use re to collect the text between "Position:" and "Shoots:" to get player position
    position = re.search(r'Position:\s*(.*?)\s*Shoots:', single_line_output)
    if position:
        # assigns the first instance this pattern is found within the given string
        player_metrics.append(position.group(1))
    
    # use re to collect the text immediately after "Shoots:" for player dominant shooting hand
    shoots = re.search(r'Shoots:\s*(\w+)', single_line_output)
    if shoots:
        player_metrics.append(shoots.group(1))
    
    # use re to collect the number for height
    height = re.search(r'(\d+)cm', single_line_output)
    if height:
        player_metrics.append(height.group(1))
    
    # use re to collect the number for weight
    weight = re.search(r'(\d+)kg', single_line_output)
    if weight:
        player_metrics.append(weight.group(1))
    
    # use re to collect the text for college
    college = re.search(r'College:\s*(\w+)', single_line_output)
    if college:
        player_metrics.append(college.group(1))
    else:
        player_metrics.append("n/a")
    
    player_metric_headers = ['Name','Position','Shoots', 'Height', 'Weight', 'College']
    
    # create the DataFrame containing player info
    player_metrics_df = pd.DataFrame([player_metrics], columns=player_metric_headers)
        
    return player_metrics_df

# retrieve the player season statistics for all games in a given season using a 2-element list containing player name and url to their stats, and a season start year as an integer
def get_player_season_stats(player_name_with_url: list, season_start_year: int) -> DataFrame:
    # base url
    baseline_url = "https://www.basketball-reference.com"
    # pass the full url after appending to the end of the baseline url from list, along with file save location
    get_html(rf'{baseline_url}{player_name_with_url[1]}', rf'C:\Users\Michael\Code\Python\Data_scraping\player_specific_data\{player_name_with_url[0]}_data.html')
    
    # open file containing the HTML data (with error handles)
    with open(rf'C:\Users\Michael\Code\Python\Data_scraping\player_specific_data\{player_name_with_url[0]}_data.html', 'r', encoding='utf-8') as file:
        contents = file.read()
    
    # make the soup
    soup_1 = BeautifulSoup(contents, "html.parser")
    # find the table of all season stats
    table_1 = soup_1.find('table', id='per_game_stats')
    
    # used to ensure that a season is only saved once since some pages contain multiple lines for the same year, with the same url and data
    was_year_saved = False
    
    # search the table for a given year
    for element in table_1.find('tbody').find_all('tr'):
        
        # use re to collect the number for start year by removing the information after the hyphen
        year = re.search(r'(\d+)-', element.find('th').find('a').get_text())
        if year:
            # convert to int and compare to season_start_year and ensure that the year has not yet been saved
            if int(year.group(1)) == season_start_year and was_year_saved == False:
                # assigns the year_url from the href data if the year is correct
                year_url = element.find('th').a['href']
    
                # get html data from specific season
                season_data = get_response_data(rf'{baseline_url}{year_url}')
                # make some more soup
                soup_2 = BeautifulSoup(season_data, "html.parser")
                # find the 'pgl_basic' table by its tag and ID
                table_2 = soup_2.find('table', id='pgl_basic')
                # array for headers
                headers = []
                
                # extract headers with improved handling for whitespace and non-breaking spaces
                for table_header in table_2.find('thead').find_all('th'):
                    # remove whitespace
                    header = table_header.get('data-tip', table_header.string.strip()).replace(u'\xa0', ' ').strip()
                    # if header is still empty after stripping whitespace
                    if not header:
                        header = table_header.get('data-stat', 'Unknown Header')
                        
                    headers.append(header)
                
                # array for row data
                rows = []
                
                # extract rows, skipping any repeated header rows and ensuring only valid data rows; records the games for the regular season
                for row in table_2.find_all('tr'):
                    cells = [td.get_text().replace(u'\xa0', ' ') for td in row.find_all(['th', 'td'])]
                    # Only add rows with the correct number of columns (matching the header count)
                    if len(cells) == len(headers):
                        rows.append(cells)
                  
                # find the 'pgl_basic_playoffs' table by its tag and ID
                table_3 = soup_2.find('table', id='pgl_basic_playoffs')
                
                # determines if the table exists in the html
                if table_3:
                    # append the playoff games to the row data if the player made it to the playoffs that season    
                    for row in table_3.find_all('tr'):
                        cells = [td.get_text().replace(u'\xa0', ' ') for td in row.find_all(['th', 'td'])]
                        # Only add rows with the correct number of columns (matching the header count)
                        if len(cells) == len(headers):
                            rows.append(cells)
                
                # headers are stored in a way that does not make them the first row in the DataFrame
                season_df = pd.DataFrame(rows, columns=headers)
                # drop duplicate data rows, if not already properly done in the row for loop
                season_df = season_df.drop_duplicates()
                # drop first column due to redundant data
                season_df = season_df.drop(season_df.columns[0], axis=1)
                # drops the first row of numbers imported and then resets the indexes
                season_df = season_df.drop(index=0).reset_index(drop=True)
                # change the default "@" in the location column
                season_df['game_location'] = season_df['game_location'].apply(lambda x: "Away" if x == "@" else "Home")
                # rename the age column
                season_df = season_df.rename(columns = {'Player\'s age on February 1 of the season':'player_age'})
                # fix date format for ease of comparison later
                for date in season_df['Date']:
                    # use datetime library for ensuring all dates are compared in the same form, see https://docs.python.org/3/library/datetime.html#format-codes
                    # parse date info
                    parsed_date = datetime.strptime(date, "%Y-%m-%d")
                    #reformat date
                    new_date = parsed_date.strftime("%m/%d/%y")
                    # update the date
                    date = new_date
                
                # save to CSV, removing row indexes and keeping the headers
                season_df.to_csv(rf'C:\Users\Michael\Code\Python\Data_scraping\player_csv\{season_start_year}_{player_name_with_url[0]}.csv', index=False, header=True)
                # updates to record that that year's season was saved
                was_year_saved = True
                
                print(rf"Game log data saved to {season_start_year}_{player_name_with_url[0]}.csv")
                
                return season_df

# utilize the pickle library to save the contents of a list or other data structure for later use
def pickle_data(data_for_later, file_name: str):
    with open(rf'C:\Users\Michael\Code\Python\Data_scraping\pickled_data\{file_name}', 'wb') as file:
        pickle.dump(data_for_later, file)
        
# retrieve the string literal of a variable name for ease of use when naming files 
def variable_to_string_literal(variable):
    for name, value in globals().items():
        if value is variable:
            return name
    return None

# used to find all player statistics compiled for a game
def full_games_schedule(start_year: int, end_year: int) -> DataFrame:
    # list containing all DataFrames with each season's data; contains data of form: [year, season_schedule_df]
    all_seasons_schedules_headers = ['Year', 'Season_schedule_df']
    all_seasons_schedules_dfs = pd.DataFrame(columns=all_seasons_schedules_headers)
    # retrieve the team names and abbreviations for later use
    team_abbreviations = get_team_abbreviations()
    
    for year in range(start_year, (end_year + 1)):
            
        # save the html data for the page; corrects for difference in url and season start year
        get_html(rf'https://www.basketball-reference.com/leagues/NBA_{(year + 1)}_games.html', rf'C:\Users\Michael\Code\Python\Data_scraping\season_schedule\{year}_schedule.html')
        
        # open file containing the HTML data (with error handles)
        with open(rf'C:\Users\Michael\Code\Python\Data_scraping\season_schedule\{year}_schedule.html', 'r', encoding='utf-8') as file:
            contents = file.read()
        
        # make soup
        soup = BeautifulSoup(contents, "html.parser")
        # find the div containing the month urls for a given season
        month_data = soup.find('div', class_='filter')
        
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
        
        for month in month_data.find_all('a', href=True):
            # base site url
            base_url = "https://www.basketball-reference.com"
            # obtains the html data for the given month of the season
            season_month_data = get_response_data(rf'{base_url}{month['href']}')
            # make soup
            soup_2 = BeautifulSoup(season_month_data, 'html.parser')
            # find table with season data
            table = soup_2.find('table', id='schedule')
            
            # added to help with debugging
            if table is None:
                print(f"No table found for month {month['href']} in year {year}. Skipping...")
                continue
            
            if not headers_done: 
                # extract headers with improved handling for whitespace and non-breaking spaces
                for table_header in table.find('thead').find_all('th'):
                    # remove whitespace
                    header = table_header.get('data-tip', table_header.string.strip()).replace(u'\xa0', ' ').strip()
                    # if header is still empty after stripping whitespace
                    if not header:
                        header = table_header.get('data-stat', 'Unknown Header')
                        
                    headers.append(header)
                # update headers_done
                headers_done = True
            
            # extract rows, skipping any repeated header rows and ensuring only valid data rows; records the games for the regular season
            for row in table.find('tbody').find_all('tr'):
                cells = [td.get_text().replace(u'\xa0', ' ') for td in row.find_all(['th', 'td'])]
                # Only add rows with the correct number of columns (matching the header count)
                if len(cells) == len(headers):
                    rows.append(cells)
            
        # headers are stored in a way that does not make them the first row in the DataFrame
        season_schedule_df = pd.DataFrame(rows, columns=headers)
        # drop duplicate data rows, if not already properly done in the row for loop
        season_schedule_df = season_schedule_df.drop_duplicates()
        # rename away/home headers along with the point columns to be easier to work with later
        season_schedule_df = season_schedule_df.rename(columns = {'Visitor/Neutral':'Away', 'Home/Neutral':'Home','Points':'Away_points', 'PTS':'Home_points'})
        # remove the 'notes' column
        season_schedule_df = season_schedule_df.drop(season_schedule_df.columns['Notes'], axis=1)
        # handle changing all full team names to their 3-letter abbreviations in Away column
        for full_name in season_schedule_df['Away']:
            for full_name_compare in team_abbreviations['team_name']:
                # ensures that both names are stripped and have the same case for comparison
                if full_name.strip().lower() == full_name_compare.strip().lower():
                    # finds the row index where this condition is met to find the correct abbreviation to change it to; returns a list
                    row_index = team_abbreviations.index[team_abbreviations['team_name'] == full_name_compare].tolist()
                    # sets name to a list of abbreviation(s)
                    full_name = team_abbreviations['team_abbreviation'][row_index[0]]
        # handle changing all full team names to their 3-letter abbreviations in Home column
        for full_name in season_schedule_df['Home']:
            for full_name_compare in team_abbreviations['team_name']:
                # ensures that both names are stripped and have the same case for comparison
                if full_name.strip().lower() == full_name_compare.strip().lower():
                    # finds the row index where this condition is met to find the correct abbreviation to change it to; returns a list
                    row_index = team_abbreviations.index[team_abbreviations['team_name'] == full_name_compare].tolist()
                    # sets name to a list of abbreviation(s)
                    full_name = team_abbreviations['team_abbreviation'][row_index[0]]
        # fix date format for ease of comparison later
        for date in season_schedule_df['Date']:
            # use datetime library for ensuring all dates are compared in the same form, see https://docs.python.org/3/library/datetime.html#format-codes
            # parse date info
            parsed_date = datetime.strptime(date, "%a, %b %d, %Y")
            #reformat date
            new_date = parsed_date.strftime("%m/%d/%y")
            # update the date
            date = new_date
        # append the given season with the year the season started in to the list of all season DataFrames
        all_seasons_schedules_dfs.loc[(year - start_year)] = [year, season_schedule_df]
        # save to CSV, removing row indexes and keeping the headers
        season_schedule_df.to_csv(rf'C:\Users\Michael\Code\Python\Data_scraping\season_schedule\{year}_season_games.csv', index=False, header=True)
        # updates to record that that year's season was saved
        
        print(rf"Season data saved to {year}_season_games.csv")
        
    return all_seasons_schedules_dfs

# takes a text file containing information for all NBA teams and transfers the info to a DataFrame for comparisons
def get_team_abbreviations() -> DataFrame:
    # headers for the relevant data
    team_headers = ['team_location', 'team_abbreviation', 'team_name', 'year_active']
    # list to collect the rows of data
    team_table = []
    # open text file containing team name and abbreviations for all historical and current NBA teams, excluding only a few repeat teams that "re branded" circa ~1950's
    with open(rf'C:\Users\Michael\Code\Python\Data_scraping\nba_team_names.txt', 'r') as file:
        for line in file:
            # strip leading/trailing whitespace and split by tab
            row = line.strip().split('\t')
            team_table.append(row)
    # place data into a DataFrame
    team_name_df = pd.DataFrame(team_table, columns=team_headers)
    # make all team abbreviations uppercase
    team_name_df['team_abbreviation'] = team_name_df['team_abbreviation'].apply(lambda x: x.upper())
    # put all team abbreviations into a list since some have multiple that are used...
    team_name_df['team_abbreviation'] = team_name_df['team_abbreviation'].apply(lambda x: x.split(','))
    
    return team_name_df
    
# takes in a list of season schedules; assumes you already have all necessary player data saved for access; this returns a russian doll of DataFrames
def collect_players_in_game(season_game_schedules_df: DataFrame) -> DataFrame:
    
    # make headers for aggregate of all games
    aggregate_headers = ['Season_start_year', 'Season_game_data']

    # contains all_game_info for every single game across the seasons input
    aggregate_of_all_game_info_df = pd.DataFrame(columns=aggregate_headers)
    
    # *** create all the portions of the DataFrame for a given season before opening any player files so that each player file for a given season only ends up having to be opened once
    
    # iterate over the list of season schedules; season_data has the form: [year, season_schedule_df]
    for season_data in season_game_schedules_df:
        # get the index of the row that the given date is on for comparing other row data
        find_row_index_1 = season_game_schedules_df.index[season_game_schedules_df['Year'] == season_data].tolist()
        row_index_1 = find_row_index_1[0]
        
        # season_year is recorded for later use in iterating over file names **keep**
        season_year_1 = season_data[0]
        # add the year the season started in
        aggregate_of_all_game_info_df.loc[row_index_1, 'Season_start_year'] = season_year_1

        # iterate over the dates of all the games in a given season
        for schedule_date in season_data[1]['Date']:

            # get the index of the row that the given date is on for comparing other row data
            find_row_index_2 = season_data[1].index[season_data[1]['Date'] == schedule_date].tolist()
            row_index_2 = find_row_index_2[0]

            # initialize DataFrame that will contain all data for a singular game
            # has the form: [[[game date],[home_team_name, home_team_score, team_win as boolean, [players'_game_stats]], [away_team_name, away_team_score, team_win as boolean, [players'_game_stats]]], ...]
            # Home_team_stats and Away_team_stats will be added to via for loop later in this method (scroll down)
            all_game_headers = ['Game_date', 'Home_team_stats', 'Away_team_stats']
            all_game_info_df = pd.DataFrame(columns=all_game_headers)

            # add game date
            all_game_info_df.loc[row_index_2, 'Game_date', ] = schedule_date

            # collect team stats in DataFrames
            team_headers = ['Team', 'Score', 'Team_win', 'Players_game_stats' ]
            home_team_aggregate_df = pd.DataFrame(columns=team_headers)
            away_team_aggregate_df = pd.DataFrame(columns=team_headers)

            # all pertinent information for comparing teams for each game instance
            # add home team name
            home_team_aggregate_df.loc[0,'Team'] = season_data[1]['Home'][row_index_2]
            # add home team score
            home_team_score = season_data[1]['Home_points'][row_index_2]
            home_team_aggregate_df.loc[0,'Score'] = home_team_score
            # add away team name
            away_team_aggregate_df.loc[0, 'Team'] = season_data[1]['Away'][row_index_2]
            # add away team score
            away_team_score = season_data[1]['Away_points'][row_index_2]
            away_team_aggregate_df.loc[0, 'Score'] = away_team_score
            
            # determine which team won and set the booleans accordingly, then add info to the appropriate DataFrames
            if home_team_score > away_team_score:
                home_team_aggregate_df.loc[0,'Team_win'] = True
                away_team_aggregate_df.loc[0,'Team_win'] = False
            else:
                home_team_aggregate_df.loc[0,'Team_win'] = False
                away_team_aggregate_df.loc[0,'Team_win'] = True

            # appends to aggregate_of_all_game_info_df
            aggregate_of_all_game_info_df.loc[row_index_1, 'Season_game_data' ] = all_game_info_df


    # iterate back over the newly made DataFrame to fill in player data; aggregate_of_all_game_info_df has data of the form: ['Season_start_year', 'Season_game_data']
    for season_year_data in aggregate_of_all_game_info_df:
        # get the season_year for use in file search
        season_year_2 = season_year_data[0]
        # directory to search using pathlib library
        folder_path = Path('C:\Users\Michael\Code\Python\Data_scraping\player_csv')
        # iterate through files in the folder
        for file in folder_path.iterdir():
            # checks to see if the file name has the year the season started in
            if file.is_file() and season_year_2 in file.name:
                # opens the player file that has data for a given season and places it in a DataFrame
                player_csv_df = pd.read_csv(file)
                
                # iterates over the dates of games the player was a part of
                for player_game_date in player_csv_df['Date']:
                    # get the index of the row that the given date is on for comparing other row data from player games
                    find_row_index_3 = player_csv_df.index[player_csv_df['Date'] == player_game_date].tolist()
                    row_index_3 = find_row_index_3[0]
                    # iterate over each game date in a season_year
                    for game_date in season_year_data[1]['Game_date']:
                        # get the index of the row that the given date is on for comparing other row data from season schedule
                        find_row_index_4 = season_year_data[1].index[season_year_data[1]['Game_date'] == game_date].tolist()
                        row_index_4 = find_row_index_4[0]
                        # if there is a match between game dates for the player and on the season schedule; why ensuring same date format is important from datetime library
                        if player_game_date == game_date:
                            
                            # check both the Home and Away team dfs
                            
                            # checks home team
                            # makes sure to loop through the list of possible team abbreviations, although most have just one *** may change how this functions later
                            for team_abbreviation in season_year_data[1].loc[row_index_4,'Home_team_stats'].loc['Team']:
                                
                                # match date with team that played since several games occur on the same dates
                                if team_abbreviation == player_csv_df.loc[row_index_3, 'Team']:
                                    # check to see if there is a DataFrame to ensure it is only added once
                                    if season_year_data[1].loc[row_index_4,'Home_team_stats'].loc['Players_game_stats'].isnull():
                                        # if the DataFrame does not already exist at that element
                                        # set the column headers to be the same as what is in all the player csv files and add the appropriate player stats
                                        new_df = pd.DataFrame(player_csv_df[row_index_3], columns=player_csv_df.columns)
                                        season_year_data[1].loc[row_index_4,'Home_team_stats'].loc['Players_game_stats'] = new_df
                                    else:
                                        # if the DataFrame already exists at that element
                                        season_year_data[1].loc[row_index_4,'Home_team_stats'].loc['Players_game_stats'].concat(player_csv_df[row_index_3])
                            
                            
                            # checks away team
                            # makes sure to loop through the list of possible team abbreviations, although most have just one *** may change how this functions later
                            for team_abbreviation in season_year_data[1].loc[row_index_4,'Away_team_stats'].loc['Team']:
                                
                                # match date with team that played since several games occur on the same dates
                                if team_abbreviation == player_csv_df.loc[row_index_3, 'Team']:
                                    # check to see if there is a DataFrame to ensure it is only added once
                                    if season_year_data[1].loc[row_index_4,'Away_team_stats'].loc['Players_game_stats'].isnull():
                                        # if the DataFrame does not already exist at that element
                                        # set the column headers to be the same as what is in all the player csv files and add the appropriate player stats
                                        new_df = pd.DataFrame(player_csv_df[row_index_3], columns=player_csv_df.columns)
                                        season_year_data[1].loc[row_index_4,'Away_team_stats'].loc['Players_game_stats'] = new_df
                                    else:
                                        # if the DataFrame already exists at that element
                                        season_year_data[1].loc[row_index_4,'Away_team_stats'].loc['Players_game_stats'].concat(player_csv_df[row_index_3])