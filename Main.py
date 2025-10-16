import re
from typing import Dict, List, Optional
import bs4 as bs
import pandas as pd
import requests

session = requests.Session()


def get_split_soup(playerid: str, year: Optional[int] = None, pitching_splits: bool = False) -> bs.BeautifulSoup:
    pitch_or_bat = 'p' if pitching_splits else 'b'
    str_year = 'Career' if year is None else str(year)
    url = f"https://www.baseball-reference.com/players/split.fcgi?id={playerid}&year={str_year}&t={pitch_or_bat}"

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/118.0.5993.117 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
    }

    html = session.get(url, headers=headers).text
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
    if not soup:
        soup = get_split_soup(playerid)
    about_info = soup.find_all("div", {"class": "players"})
    info: List[bs.BeautifulSoup] = [ele for ele in about_info]
    fv = []

    for i in range(len(info)):
        ptags = info[i].find_all('p')
        for j in range(len(ptags)):
            InfoRegex = re.compile(r'>(.*?)<', re.DOTALL)
            r = InfoRegex.findall(str(ptags[j]))
            for k in range(len(r)):
                pattern = re.compile(r'[\W_]+')
                strings = pattern.sub(' ', r[k])
                if strings and strings != ' ':
                    fv.append(strings)

    player_info_data = {
        'Position': fv[1] if len(fv) > 1 else None,
        'Bats': fv[3] if len(fv) > 3 else None,
        'Throws': fv[5] if len(fv) > 5 else None,
    }
    return player_info_data


def get_splits(playerid: str, year: Optional[int] = None, pitching_splits: bool = False) -> pd.DataFrame:
    """
    Returns a DataFrame of split stats for a given player.
    Safe for both hitters and pitchers.
    """
    pitch_or_bat = 'p' if pitching_splits else 'b'
    str_year = 'Career' if year is None else str(year)
    url = f"https://www.baseball-reference.com/players/split.fcgi?id={playerid}&year={str_year}&t={pitch_or_bat}"

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/118.0.5993.117 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
    }

    r = requests.get(url, headers=headers)
    if r.status_code != 200:
        raise ValueError(f"Error fetching data for {playerid} (HTTP {r.status_code})")

    soup = bs.BeautifulSoup(r.text, 'lxml')
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
        raise ValueError(f"No splits found for {playerid} ({'Pitching' if pitching_splits else 'Batting'}) in {str_year}")

    combined = pd.concat(all_rows, ignore_index=True)
    return combined
