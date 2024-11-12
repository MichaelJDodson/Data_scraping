import requests
from bs4 import BeautifulSoup
import pandas as pd
import re

# URL for Michael Jordan's 1984-85 game log page
url = "https://www.basketball-reference.com/players/j/jordami01/gamelog/1985/#all_game_log_summary"

# Send a GET request and returns a Response object that contains a 200 or 404 (success or fail, respectively)
response = requests.get(url)

# Handles encoding and HTML parsing
if response.status_code == 200:
    # to deal with non-breaking spaces in conversion of HTML and ensure UTF-8 encoding
    response.encoding = 'utf-8'

    # Parse the page content
    # .content gives the data in bytes
    soup = BeautifulSoup(response.content, 'html.parser')
else:
    print("Failed to retrieve the page. Status code:", response.status_code)

# Handles basic player information
if response.status_code == 200:
    
    # find specific branch of HTML data
    full_player_stats = soup.find('div', id='meta')
    
    # retrieve the str of data, ensure a space between lines/words, replace strange characters 
    player_metric_string = full_player_stats.get_text(separator=" ").replace(u'\xa0', ' ').replace(u'\u25aa', '').strip()
    
    # Normalize whitespace to a single space; split() breaks the string into words by whitespace; join() fuses them back together with only a single whitespace between each word
    single_line_output = ' '.join(player_metric_string.split())
    
    # array of strings containing player info
    player_metrics = []
    
    # Use re to collect the text between "Position:" and "Shoots:" to get player position
    position = re.search(r'Position:\s*(.*?)\s*Shoots:', player_metric_string)
    if position:
        # assigns the first instance this pattern is found within the given string
        player_metrics.append(position.group(1))
    
    # Use re to collect the text immediately after "Shoots:" for player dominant shooting hand
    shoots = re.search(r'Shoots:\s*(\w+)', player_metric_string)
    if shoots:
        player_metrics.append(shoots.group(1))
    
    # Use re to collect the text for height
    height = re.search(r'(\d+)cm', player_metric_string)
    if height:
        player_metrics.append(height.group(1))
     
    # Use re to collect the text for weight
    weight = re.search(r'(\d+)kg', player_metric_string)
    if weight:
        player_metrics.append(weight.group(1))
    
    # Use re to collect the text for college
    college = re.search(r'College:\s*(\w+)', player_metric_string)
    if college:
        player_metrics.append(college.group(1))
    else:
        player_metrics.append("n/a")
    
    player_metric_headers = ['Position','Shoots', 'Height', 'Weight', 'College']
    
    player_metrics_df = pd.DataFrame([player_metrics], columns=player_metric_headers)
    
    #print(player_metrics_df)
else:
    print("Failed to retrieve the page. Status code:", response.status_code)

# Collects all the player statistics for each game in a given season
if response.status_code == 200:
    
    # Find the 'pgl_basic' table by its tag and ID
    table = soup.find('table', id='pgl_basic')
    
   # Extract headers with improved handling for whitespace and non-breaking spaces
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
        cells = [td.string.replace(u'\xa0', ' ') for td in row.find_all(['th', 'td'])]
        # Only add rows with the correct number of columns (matching the header count)
        if len(cells) == len(headers):
            rows.append(cells)
    
    # Create DataFrame making sure to define the headers so that specific columns can be manipulated later
    # headers are stored in a way that does not make them the first row in the DataFrame
    df = pd.DataFrame(rows, columns=headers)
    
    # drop duplicate data rows
    df = df.drop_duplicates()
    
    # drop first column due to redundant data
    df = df.drop(df.columns[0], axis=1)
    
    # Drops the first row of numbers imported and then resets the indexes
    df = df.drop(index=0).reset_index(drop=True)
    
    # change the default "@" in the location column
    df['game_location'] = df['game_location'].apply(lambda x: "Away" if x == "@" else "Home")
    # change the age column
    df = df.rename(columns = {'Player\'s age on February 1 of the season':'player_age'})
    
    
    # Save to CSV, removing row indexes and keeping the headers
    df.to_csv('jordan_1984-85_game_log_2.csv', index=False, header=True)
    
    print("Game log data saved to jordan_1984-85_game_log.csv")
    
else:
    print("Failed to retrieve the page. Status code:", response.status_code)
