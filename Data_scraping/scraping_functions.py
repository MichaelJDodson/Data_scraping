import requests
from bs4 import BeautifulSoup
import pandas as pd
from pandas import DataFrame
import re
import time

# contains all the functions necessary for Data_scraping on https://www.basketball-reference.com

# handles http GET requests and returns the page contents as bytes to "make soup"
def get_response_data(url: str) -> bytes:
    response = requests.get(url)
    time.sleep(5)
    if response.status_code == 200:
        # to deal with non-breaking spaces in conversion of HTML and ensure UTF-8 encoding
        response.encoding = 'utf-8'
        return response.content
    else:
        print("Failed to retrieve the page. Status code:", response.status_code)

# pass a url and file save path to save all html data to a file
def get_html(url: str, path: str):
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
        new_url = f"https://www.basketball-reference.com/players/{letter}"
        # pass the url information to save the html data
        get_html(new_url, rf'C:\Users\Michael\Code\Python\Data_scraping\player_page_html\letter_{letter}_data.html')
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
        with open(rf'C:\Users\Michael\Code\Python\Data_scraping\player_page_html\letter_{letter}_data', 'r', encoding='utf-8') as file:
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
                
# retrieve the player metrics by passing the list containing the names and url
def get_player_metrics(player_names_with_url: list) -> DataFrame:
    
    baseline_url = "https://www.basketball-reference.com"
    
    for player in player_names_with_url: 
        # pass the full url after appending to the end of the baseline url from list
        page_data = get_response_data(f'{baseline_url}{player[1]}')
        # make the soup
        soup = BeautifulSoup(page_data, 'html.parser')
        
        # find specific branch of HTML data
        full_player_stats = soup.find('div', id='meta')
        
        # retrieve the str of data, ensure a space between lines/words, replace strange characters 
        player_metric_string = full_player_stats.get_text(separator=" ").replace(u'\xa0', ' ').replace(u'\u25aa', '').strip()
        
        # normalize whitespace to a single space; split() breaks the string into words by whitespace; join() fuses them back together with only a single whitespace between each word
        single_line_output = ' '.join(player_metric_string.split())
        
        # array of strings containing player info, starting with their name
        player_metrics = [player[0]]
        
        # use re to collect the text between "Position:" and "Shoots:" to get player position
        position = re.search(r'Position:\s*(.*?)\s*Shoots:', single_line_output)
        if position:
            # assigns the first instance this pattern is found within the given string
            player_metrics.append(position.group(1))
        
        # use re to collect the text immediately after "Shoots:" for player dominant shooting hand
        shoots = re.search(r'Shoots:\s*(\w+)', single_line_output)
        if shoots:
            player_metrics.append(shoots.group(1))
        
        # use re to collect the text for height
        height = re.search(r'(\d+)cm', single_line_output)
        if height:
            player_metrics.append(height.group(1))
        
        # use re to collect the text for weight
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


###### unfinished
# retrieve the player season statistics for all games in a given season
def get_player_season_stats(player_name_with_url: list, season_start_year) -> DataFrame:
    # find the 'pgl_basic' table by its tag and ID
    table = soup.find('table', id='pgl_basic')
    
   # extract headers with improved handling for whitespace and non-breaking spaces
    headers = []
    for th in table.find('thead').find_all('th'):
        # remove whitespace
        header = th.get('data-tip', th.string.strip()).replace(u'\xa0', ' ').strip()
        # if header is still empty after stripping whitespace
        if not header:
            header = th.get('data-stat', 'Unknown Header')
            
        headers.append(header)
    
    # extract rows, skipping any repeated header rows and ensuring only valid data rows
    rows = []
    for row in table.find_all('tr'):
        cells = [td.get_text().replace(u'\xa0', ' ') for td in row.find_all(['th', 'td'])]
        # Only add rows with the correct number of columns (matching the header count)
        if len(cells) == len(headers):
            rows.append(cells)
    
    # create DataFrame making sure to define the headers so that specific columns can be manipulated later
    # headers are stored in a way that does not make them the first row in the DataFrame
    df = pd.DataFrame(rows, columns=headers)
    
    # drop duplicate data rows
    df = df.drop_duplicates()
    
    # drop first column due to redundant data
    df = df.drop(df.columns[0], axis=1)
    
    # drops the first row of numbers imported and then resets the indexes
    df = df.drop(index=0).reset_index(drop=True)
    
    # change the default "@" in the location column
    df['game_location'] = df['game_location'].apply(lambda x: "Away" if x == "@" else "Home")
    # change the age column
    df = df.rename(columns = {'Player\'s age on February 1 of the season':'player_age'})
    
    
    # save to CSV, removing row indexes and keeping the headers
    df.to_csv('jordan_1984-85_game_log_2.csv', index=False, header=True)
    
    print("Game log data saved to jordan_1984-85_game_log.csv")
