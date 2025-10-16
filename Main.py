import re
import datetime
from time import sleep
from typing import Dict, List, Optional, Tuple, Union
import bs4 as bs
import pandas as pd

try:
    from curl_cffi import requests
    USE_CURL = True
except ImportError:
    import requests
    USE_CURL = False

class BRefSession:
    def __init__(self, max_requests_per_minute: int = 10):
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

session = BRefSession()

def get_split_soup(playerid: str, year: Optional[int] = None, pitching_splits: bool = False) -> bs.BeautifulSoup:
    pitch_or_bat = "p" if pitching_splits else "b"
    str_year = "Career" if year is None else str(year)
    url = f"https://www.baseball-reference.com/players/split.fcgi?id={playerid}&year={str_year}&t={pitch_or_bat}"
    html = session.get(url).content
    return bs.BeautifulSoup(html, "lxml")

def get_player_info(playerid: str, soup: bs.BeautifulSoup = None) -> Dict:
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
    return {
        "Position": fv[1] if len(fv) > 1 else "",
        "Bats": fv[3] if len(fv) > 3 else "",
        "Throws": fv[5] if len(fv) > 5 else "",
    }

def get_splits(
    playerid: str,
    year: Optional[int] = None,
    player_info: bool = False,
    pitching_splits: bool = False,
) -> Union[pd.DataFrame, Tuple[pd.DataFrame, pd.DataFrame]]:
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
            if not rows:
                continue

            headers = [th.get_text(strip=True) for th in rows[0].find_all("th")]
            if year is None and headers and headers[0] == "I":
                headers = headers[1:]
            headers += ["Split Type", "Player ID"]

            target = raw_level_data if split_type.endswith("Level") else raw_data

            # Add header row before every group of stats
            target.append(headers)

            for row in rows[1:]:
                cols = [ele.text.strip() for ele in row.find_all(["th", "td"])]
                if not cols or split_type == "By Inning":
                    continue
                cols += [split_type, playerid]
                target.append(cols)

    def clean(df_raw):
        if not df_raw:
            return pd.DataFrame()

        # Build DataFrame by splitting at header rows
        dataframes = []
        i = 0
        while i < len(df_raw):
            header_row = df_raw[i]
            # Find next header or end
            j = i + 1
            while j < len(df_raw) and not (
                len(df_raw[j]) == len(header_row) and
                all(x == y for x, y in zip(df_raw[j], header_row))
            ):
                j += 1
            group = df_raw[i+1:j]
            if group:
                df = pd.DataFrame(group, columns=header_row)
                if "Player ID" in df.columns:
                    df = df.drop(columns=["Player ID"])
                dataframes.append(df)
                # Add blank row after each group
                blank = pd.DataFrame([[""] * len(df.columns)], columns=df.columns)
                dataframes.append(blank)
            i = j

        if dataframes:
            return pd.concat(dataframes, ignore_index=True)
        else:
            return pd.DataFrame()

    data = clean(raw_data)
    level_data = clean(raw_level_data) if pitching_splits else pd.DataFrame()

    if pitching_splits:
        return data, level_data
    else:
        return data

if __name__ == "__main__":
    skenes, skenes_level = get_splits("skenepa01", year=2025, pitching_splits=True)
    skenes.to_csv("skenes_splits.csv", index=False)
    skenes_level.to_csv("skenes_splits_level.csv", index=False)
    print("✅ Exported skenes_splits.csv and skenes_splits_level.csv successfully!")

    # trout = get_splits("troutmi01", year=2025, pitching_splits=False)
    # trout.to_csv("trout_splits.csv", index=False)
    # print("✅ Exported trout_splits.csv successfully!")
