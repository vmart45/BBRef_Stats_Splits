import re
from typing import Dict, List, Optional, Tuple, Union

import bs4 as bs
import pandas as pd

import requests
session = requests.Session()



def get_split_soup(playerid: str, year: Optional[int] = None, pitching_splits: bool = False) -> bs.BeautifulSoup:
    pitch_or_bat = 'p' if pitching_splits else 'b'
    str_year = 'Career' if year is None else str(year)
    url = f"https://www.baseball-reference.com/players/split.fcgi?id={playerid}&year={str_year}&t={pitch_or_bat}"
    html = session.get(url).text
    soup = bs.BeautifulSoup(html, 'lxml')
    return soup

def _dedup_columns(columns):
    seen = {}
    new_cols = []
    for col in columns:
        if col not in seen:
            seen[col] = 0
            new_cols.append(col)
        else:
            seen[col] += 1
            new_cols.append(f"{col}.{seen[col]}")
    return new_cols

def get_player_info(playerid: str, soup: bs.BeautifulSoup = None) -> Dict:
    '''
    Returns a dictionary with player position, batting and throwing handedness, player height in inches, player weight, and current team from Baseball Reference.
    '''

    if not soup:
        soup = get_split_soup(playerid)
    about_info = soup.find_all(
        "div", {"class": "players"})
    info: List[bs.BeautifulSoup] = [ele for ele in about_info]
    fv = []
    # This for loop goes through the player bio section at the top of the splits page to find all of the <p> tags
    for i in range(len(info)):
        ptags = info[i].find_all('p')

        # This loop goes through each of the <p> tags and finds all text between the tags including the <strong> tags.
        for j in range(len(ptags)):
            InfoRegex = re.compile(r'>(.*?)<', re.DOTALL)
            r = InfoRegex.findall(str(ptags[j]))
            # This loop cleans up the text found in the outer loop and removes non alphanumeric characters.
            for k in range(len(r)):
                pattern = re.compile(r'[\W_]+')
                strings = pattern.sub(' ', r[k])
                if strings and strings != ' ':
                    fv.append(strings)
    player_info_data = {
        'Position': fv[1],
        'Bats': fv[3],
        'Throws': fv[5],
        # 'Height': int(fv[6].split(' ')[0])*12+int(fv[6].split(' ')[1]), # Commented out because I determined that Pablo Sandoval has some weird formatting that ruins this. Uncomment for ht, wt of most players. 
        # 'Weight': int(fv[7][0:3]),
        # 'Team': fv[10]
    }
    return player_info_data


def get_splits(playerid: str, year: Optional[int] = None, player_info: bool = False, pitching_splits: bool = False) -> \
Union[pd.DataFrame, Tuple[pd.DataFrame, Dict]]:
    """
    Returns a dataframe of all split stats for a given player.
    If player_info is True, this will also return a dictionary that includes player position, handedness, height, weight, position, and team
    """
    soup = get_split_soup(playerid, year, pitching_splits)
    comment = soup.find_all(text=lambda text: isinstance(text, bs.Comment))
    raw_data = []
    raw_level_data = []

    for i in range(len(comment)):
        commentsoup = bs.BeautifulSoup(comment[i], 'lxml')
        split_tables = commentsoup.find_all("div", {"class": "table_container"})
        splits = [ele for ele in split_tables]
        headings = []
        level_headings = []
        for j in range(len(splits)):
            split_type = splits[j].find_all('caption')[0].string.strip()
            if split_type[-5:] == 'Level':
                if year == None:
                    level_headings = [th.get_text() for th in splits[j].find("tr").find_all("th")][1:]
                else:
                    level_headings = [th.get_text() for th in splits[j].find("tr").find_all("th")][:]
                level_headings.append('Split Type')
                level_headings.append('Player ID')
                level_headings.append('1B')
                raw_level_data.append(level_headings)
                rows = splits[j].find_all('tr')
                for row in rows:
                    if year == None:
                        level_cols = row.find_all('td')
                    else:
                        level_cols = row.find_all(['th', 'td'])
                    level_cols = [ele.text.strip() for ele in level_cols]
                    if split_type != "By Inning":
                        level_cols.append(split_type)
                        level_cols.append(playerid)
                        raw_level_data.append([ele for ele in level_cols])
            else:
                if year == None:
                    headings = [th.get_text() for th in splits[j].find("tr").find_all("th")][1:]
                else:
                    headings = [th.get_text() for th in splits[j].find("tr").find_all("th")][:]
                headings.append('Split Type')
                headings.append('Player ID')
                headings.append('1B')
                raw_data.append(headings)
                rows = splits[j].find_all('tr')
                for row in rows:
                    if year == None:
                        cols = row.find_all('td')
                    else:
                        cols = row.find_all(['th', 'td'])
                    cols = [ele.text.strip() for ele in cols]
                    if split_type != "By Inning":
                        cols.append(split_type)
                        cols.append(playerid)
                        raw_data.append([ele for ele in cols])

    data = pd.DataFrame(raw_data)
    data = data.rename(columns=data.iloc[0])
    data = data.reindex(data.index.drop(0))
    data = data.set_index(['Player ID', 'Split Type', 'Split'])
    data = data.drop(index=['Split'], level=2)
    data = data.apply(pd.to_numeric, errors='coerce').convert_dtypes()
    data = data.dropna(axis=1, how='all')
    data['1B'] = data['H'] - data['2B'] - data['3B'] - data['HR']
    data = data.loc[playerid]

    if pitching_splits is True:
        level_data = pd.DataFrame(raw_level_data)
        level_data = level_data.rename(columns=level_data.iloc[0])
        level_data = level_data.reindex(level_data.index.drop(0))
        level_data = level_data.set_index(['Player ID', 'Split Type', 'Split'])
        level_data = level_data.drop(index=['Split'], level=2)
        level_data = level_data.apply(pd.to_numeric, errors='coerce').convert_dtypes()
        level_data = level_data.dropna(axis=1, how='all')
        level_data = level_data.loc[playerid]

    if player_info is False:
        if pitching_splits is True:
            return data, level_data
        else:
            return data
    else:
        player_info_data = get_player_info(playerid=playerid, soup=soup)
        if pitching_splits is True:
            return data, player_info_data, level_data
        else:
            return data, player_info_data


if __name__ == "__main__":
    from Main import get_splits
    data, level_data = get_splits("skenepa01", year=2024, pitching_splits=True)
    data.to_csv("skenes_splits.csv", index=True)
    level_data.to_csv("skenes_splits_game_level.csv", index=True)
    print("✅ CSV files saved: skenes_splits.csv and skenes_splits_game_level.csv")



