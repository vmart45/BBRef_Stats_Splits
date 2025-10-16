import re
import datetime
from time import sleep
from typing import Dict, List, Optional, Tuple, Union

import bs4 as bs
import pandas as pd

# ---- BRefSession: Safe Baseball Reference client ----
try:
    from curl_cffi import requests
    USE_CURL = True
except ImportError:
    import requests
    USE_CURL = False


class BRefSession:
    """
    Baseball Reference-safe session that automatically rate limits requests.
    Uses curl_cffi if available for Chrome impersonation, else falls back to requests.
    """

    def __init__(self, max_requests_per_minute: int = 10) -> None:
        self.max_requests_per_minute = max_requests_per_minute
        self.last_request: Optional[datetime.datetime] = None
        self.session = requests.Session()

    def get(self, url: str, **kwargs: any):
        if self.last_request:
            delta = datetime.datetime.now() - self.last_request
            sleep_length = (60 / self.max_requests_per_minute) - delta.total_seconds()
            if sleep_length > 0:
                sleep(sleep_length)

        self.last_request = datetime.datetime.now()

        try:
            if USE_CURL:
                resp = self.session.get(url, impersonate="chrome", **kwargs)
            else:
                headers = {
                    "User-Agent": (
                        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/122.0.0.0 Safari/537.36"
                    ),
                    "Accept-Language": "en-US,en;q=0.9",
                    "Referer": "https://www.google.com/",
                }
                resp = self.session.get(url, headers=headers, **kwargs)
            resp.raise_for_status()
            return resp
        except Exception as e:
            raise ValueError(f"Error fetching {url}: {e}")


# ---- Initialize session ----
session = BRefSession()


# ---- Utility Functions ----
def get_split_soup(playerid: str, year: Optional[int] = None, pitching_splits: bool = False) -> bs.BeautifulSoup:
    pitch_or_bat = "p" if pitching_splits else "b"
    str_year = "Career" if year is None else str(year)
    url = f"https://www.baseball-reference.com/players/split.fcgi?id={playerid}&year={str_year}&t={pitch_or_bat}"
    html = session.get(url).content
    soup = bs.BeautifulSoup(html, "lxml")
    return soup


def get_player_info(playerid: str, soup: bs.BeautifulSoup = None) -> Dict:
    """Extract basic player info (position, handedness, etc.) from BRef."""
    if not soup:
        soup = get_split_soup(playerid)

    about_info = soup.find_all("div", {"class": "players"})
    fv = []
    for info in about_info:
        for p in info.find_all("p"):
            matches = re.findall(r">(.*?)<", str(p), re.DOTALL)
            for m in matches:
                cleaned = re.sub(r"[\W_]+", " ", m).strip()
                if cleaned:
                    fv.append(cleaned)

    player_info_data = {
        "Position": fv[1] if len(fv) > 1 else "",
        "Bats": fv[3] if len(fv) > 3 else "",
        "Throws": fv[5] if len(fv) > 5 else "",
    }
    return player_info_data


# ---- Core Split Extraction ----
def get_splits(
    playerid: str,
    year: Optional[int] = None,
    player_info: bool = False,
    pitching_splits: bool = False,
) -> Union[pd.DataFrame, Tuple[pd.DataFrame, Dict]]:
    """
    Returns a dataframe of all split stats for a given player.
    Pitching splits automatically merge season-level and game-level side-by-side.
    """
    soup = get_split_soup(playerid, year, pitching_splits)
    comments = soup.find_all(string=lambda text: isinstance(text, bs.Comment))

    raw_data, raw_level_data = [], []

    for comment in comments:
        commentsoup = bs.BeautifulSoup(comment, "lxml")
        split_tables = commentsoup.find_all("div", {"class": "table_container"})

        for table in split_tables:
            caption = table.find("caption")
            if not caption:
                continue
            split_type = caption.string.strip()
            rows = table.find_all("tr")

            # Gather headers
            headings = [th.get_text() for th in rows[0].find_all("th")]
            if year is None and headings and headings[0] == "I":
                headings = headings[1:]
            headings += ["Split Type", "Player ID"]

            # Pick list based on type
            target = raw_level_data if split_type.endswith("Level") else raw_data
            target.append(headings)

            for row in rows[1:]:
                cols = [ele.text.strip() for ele in row.find_all(["th", "td"])]
                if not cols or split_type == "By Inning":
                    continue
                cols += [split_type, playerid]
                target.append(cols)

    # Convert lists → DataFrames
    def clean(df_raw):
        if not df_raw:
            return pd.DataFrame()
        df = pd.DataFrame(df_raw)
        df.columns = df.iloc[0]
        df = df.drop(0).dropna(axis=1, how="all")
        return df

    data = clean(raw_data)
    level_data = clean(raw_level_data) if pitching_splits else pd.DataFrame()

    # Merge side-by-side if pitching
    if pitching_splits and not data.empty and not level_data.empty:
        combined = pd.merge(
            data,
            level_data,
            on="Split",
            how="outer",
            suffixes=("", "_GameLevel"),
        )
    else:
        combined = data

    # Add player info at the top if requested
    if player_info:
        info = get_player_info(playerid, soup)
        for key, val in info.items():
            combined.loc[-1] = [f"{key}: {val}"] + ["" for _ in range(len(combined.columns) - 1)]
        combined.index = combined.index + 1
        combined = combined.sort_index()

    # Convert numeric columns properly
    for col in combined.columns:
        combined[col] = pd.to_numeric(combined[col], errors="ignore")

    return combined

