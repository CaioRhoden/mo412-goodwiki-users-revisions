from pathlib import Path
from typing import Any

import pandas as pd
import polars as pl
import requests

from tyro import cli

DATA_PATH = "../data/goodwiki.parquet"
SAVING_PATH = "../data"
URL = "https://en.wikipedia.org/w/api.php"
HEADERS = {
    "User-Agent": "GoodWikiUsersRevisions/1.0 (caior@example.com)"
}

def make_request(
    title: str,
    rvstart: str = "2023-01-01T00:00:00Z",
    rvend: str = "2023-09-04T00:00:00Z"
) -> list[dict[str, Any]]:
    S = requests.Session()

    PARAMS: dict[str, str] = {
        "action": "query",
        "prop": "revisions",
        "titles": title,
        "rvprop": "ids|user|timestamp|flags|userid|size|slotsize|contentmodel|tags",
        "rvslots": "main",
        "formatversion": "2",
        "format": "json",
        "rvlimit": "50",
        "rvend": rvstart,
        "rvstart": rvend
    }

    response = S.get(URL, headers=HEADERS, params=PARAMS)
    data = response.json()

    revisions: list[dict[str, Any]] = data["query"]["pages"][0]["revisions"]

    return revisions


def get_revisions_data(start_idx: int, end_idx: int, checkpoint: int) -> None:
    """
    Fetches and processes revision data for a range of Wikipedia pages.

    This function reads a dataset of Wikipedia pages, retrieves revision data 
    for each page within the specified range, and saves the data in Parquet format 
    at regular checkpoints.

    Args:
        start_idx (int): The starting index of the pages to process.
        end_idx (int): The ending index (exclusive) of the pages to process.
        checkpoint (int): The number of pages to process before saving the data 
                          to a Parquet file.

    Returns:
        None
    """
    if not Path(DATA_PATH).exists():
        pd.read_parquet("hf://datasets/euirim/goodwiki/09_04_2023_v1.parquet") \
           .to_parquet(DATA_PATH)

    goodwiki = pl.read_parquet(DATA_PATH)
    revisions_data: list[dict[str, Any]] = []

    for row in range(start_idx, end_idx):
        title = goodwiki["title"][row]
        pageId = goodwiki["pageid"][row]

        revisions = make_request(title)

        ### Get all revisions starting 2017-01-01
        revisions_list: list[dict[str, Any]] = []

        lastFound = False
        while not lastFound:
            if len(revisions) < 50:
                lastFound = True

            for rev in revisions:
                rev["pageid"] = pageId
                rev["title"] = title
                revisions_list.append(rev)

            if not lastFound:
                rvend = revisions[-1]["timestamp"]

                revisions = make_request(title, rvend=rvend)
    

        revisions_data.extend(revisions_list)
        print(f"Processed {row+1}/{len(goodwiki)}: {title} with {len(revisions_list)} revisions.")

        if (row + 1) % checkpoint == 0 or (row + 1) == end_idx:
            df = pl.DataFrame(revisions_data)
            df.write_parquet(f"{SAVING_PATH}/revisions_{start_idx}_{end_idx}_{row+1}.parquet")
            print(f"Checkpoint reached at {row+1}. Data saved.")
            revisions_data.clear()  # Clear the list after saving


if __name__ == "__main__":
    cli(get_revisions_data)