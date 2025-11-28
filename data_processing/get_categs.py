from __future__ import annotations
from tqdm import tqdm
from itertools import batched
import requests

import pickle
import pandas as pd

pages = pd.read_parquet('data/goodwiki.parquet')

def is_invalid_category(category: str) -> bool:
    category = category.replace(' ', '_')

    return (
        category.startswith('Wikipedia_categories_') or
        category.startswith('All_Wikipedia_') or
        category.startswith('Redirects_') or
        category == "Good_articles" or
        category == "Source_attribution" or
        category.startswith("Pages_using_") or
        category.startswith("Short_description_") or
        category.startswith("Articles_with_") or
        category.startswith("All_articles_with_") or
        category.startswith("Articles_needing_") or
        category.startswith("Articles_containing_") or
        category.startswith("CS1_") or
        category.startswith("Use_mdy_") or
        category.startswith("Commons_category_")
    )

API_URL: str = "https://en.wikipedia.org/w/api.php"
HEADERS = {
    "User-Agent": "GoodWikiUsersRevisions/1.0 (caior@example.com)"
}
PARAMS: dict[str, str] = {
    "action": "query",
    "prop": "categories",
    "cllimit": "max",
    "format": "json",
    "redirects": "1"
}

S = requests.Session()

last_continue: dict[str, str] = {}
categ_lists: dict[int, list[str]] = {}

batch_size = 1
for batch in tqdm(batched(pages['pageid'].astype(str), n=batch_size), total=-(-len(pages)//batch_size), miniters=100.):
    PARAMS['pageids'] = "|".join(batch)
    last_continue.clear()

    while True:
        params = PARAMS.copy()
        params.update(last_continue)

        response = S.get(API_URL, headers=HEADERS, params=params)
        data = response.json()

        # print(data)

        for pid, page_data in data['query']['pages'].items():
            pid_int = int(pid)

            if pid_int not in categ_lists:
                categ_lists[pid_int] = []
            
            
            if 'categories' in page_data:
                for cat in page_data['categories']:
                    category_title = cat['title'].replace('Category:', '')
                    if not is_invalid_category(category_title):
                        categ_lists[pid_int].append(category_title)

        if 'continue' in data:
            last_continue = data['continue']

        else:
            break

    for pid in batch:
        pid_int = int(pid)

        if pid_int not in categ_lists or len(categ_lists[pid_int]) == 0:
            print(f"No categories found for page ID {pid}.")


with open('data/categ_lists.pkl', 'wb') as f:
    pickle.dump(categ_lists, f)