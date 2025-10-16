import re
from typing import Dict, List, Optional, Tuple, Union
import bs4 as bs
import pandas as pd
from datasources.bref import BRefSession  # ← your existing class

# Use your safe, rate-limited session
session = BRefSession()


def get_split_soup(playerid: str, year: Optional[int] = None, pitching_splits: bool = False) -> bs.BeautifulSoup:
    """Fetches the Baseball Reference splits page for a player safely."""
    pitch_or_bat = 'p' if pitching_splits else 'b'
    str_year = 'Career' if year is None else str(year)
    url = f"https://www.baseball-reference.com/players/split.fcgi?id={playerid}&year={str_year}&t={pitch_or_bat}"

    resp = session.get(url)
    if resp == -1 or resp.status_code != 200:
        raise ValueError(f"Error fetching data for {playerid} (HTTP {resp.status_code if resp != -1 else 'Unknown'})")

    soup = bs.BeautifulSoup(resp.text, 'lxml')
    return soup


def get_splits(playerid: str, year: Optional[int] = None, pitching_splits: bool = False) -> pd.DataFrame:
    """
    Returns a DataFrame of split stats for a given player.
    Works for both hitters and pitchers using BRefSession for safe scraping.
    """
    soup = get_split_soup(playerid, year, pitching_splits)
    comments = soup.find_all(string=lambda text: isinstance(text, bs.Comment))

    all_rows = []
    for comment in comments:
        comment_soup = bs.BeautifulSoup(comment, 'lxml')
        tables = comment_soup.find_all("table")
        for table in tables:
            caption = table.find("caption")
            if not caption:
                continue
            split_type = caption.text.strip()

            rows = table.find_all("tr")
            if not rows:
                continue

            headers = [th.get_text(strip=True) for th in rows[0].find_all("th")]
            if not headers:
                continue

            data_rows = []
            for row in rows[1:]:
                cols = [td.get_text(strip=True) for td in row.find_all(["th", "td"])]
                if len(cols) != len(headers):
                    continue
                data_rows.append(cols)

            if not data_rows:
                continue

            df = pd.DataFrame(data_rows, columns=headers)
            df["Split Type"] = split_type
            all_rows.append(df)

    if not all_rows:
        raise ValueError(f"No splits found for {playerid} ({'Pitching' if pitching_splits else 'Batting'})")

    combined = pd.concat(all_rows, ignore_index=True)
    return combined

