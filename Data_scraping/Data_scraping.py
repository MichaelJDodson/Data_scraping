import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import time

#main file


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
        cells = [td.get_text().replace(u'\xa0', ' ') for td in row.find_all(['th', 'td'])]
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
