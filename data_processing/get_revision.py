from turtle import st
import polars as pl
import requests
import argparse
import datetime

DATA_PATH = "../data/09_04_2023_v1.parquet"
SAVING_PATH = "../data"
URL = "https://en.wikipedia.org/w/api.php"
HEADERS = {
    "User-Agent": "GoodWikiUsersRevisions/1.0 (caior@example.com)"
}

def make_request(title, rvstart="2023-01-01T00:00:00Z", rvend="2023-09-04T00:00:00Z") -> list[dict]:
    S = requests.Session()
    PARAMS = {
        "action": "query",
        "prop": "revisions",
        "titles": title,
        "rvprop": "ids|user|timestamp|flags|userid|size|slotsize|contentmodel|tags",
        "rvslots": "main",
        "formatversion": "2",
        "format": "json",
        "rvlimit": 50,
        "rvend": rvstart,
        "rvstart": rvend

    }
    response = S.get(URL, headers=HEADERS, params=PARAMS)
    data = response.json()
    try:
        revisions = data["query"]["pages"][0]["revisions"]
    except (KeyError, IndexError):
        revisions = []
    return revisions


def get_revisions_data(start_idx, end_idx, checkpoint):
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
    goodwiki = pl.read_parquet(DATA_PATH)
    revisions_data = []

    for row in range(start_idx, end_idx):
        title = goodwiki["title"][row]
        pageId = goodwiki["pageid"][row]

        revisions = make_request(title)

        ### Get all revisions starting 2017-01-01
        revisions_list = []
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
            revisions_data = []  # Clear the list after saving


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch Wikipedia revisions data.")
    parser.add_argument("--start_idx", type=int, required=True, help="Starting index of pages to process.")
    parser.add_argument("--end_idx", type=int, required=True, help="Ending index (exclusive) of pages to process.")
    parser.add_argument("--checkpoint", type=int, default=1000, help="Number of pages to process before saving data.")
    args = parser.parse_args()

    get_revisions_data(args.start_idx, args.end_idx, args.checkpoint)
