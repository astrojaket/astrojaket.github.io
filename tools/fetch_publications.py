# tools/fetch_publications.py
import os
import re
from pathlib import Path

import requests
import yaml

ORCID = "0000-0003-4844-9838"
AUTHOR_SURNAME = "Taylor"  # change only if your SciX author strings use a different surname
OUTFILE = Path("_data/publications.yml")

TOKEN = os.environ.get("ADS_API_TOKEN")
if not TOKEN:
    raise RuntimeError("ADS_API_TOKEN is missing from the environment.")

url = "https://api.adsabs.harvard.edu/v1/search/query"
params = {
    "q": f"orcid:{ORCID}",
    "fl": "title,author,bibcode,year,date",
    "sort": "date desc",
    "rows": 500,
}
headers = {"Authorization": f"Bearer {TOKEN}"}

resp = requests.get(url, params=params, headers=headers, timeout=60)
resp.raise_for_status()
payload = resp.json()

docs = payload.get("response", {}).get("docs", [])

first_author = []
coauthor = []

seen = set()

for doc in docs:
    authors = doc.get("author") or []
    if not authors:
        continue

    bibcode = doc.get("bibcode") or ""
    if bibcode in seen:
        continue
    seen.add(bibcode)

    title = (doc.get("title") or ["Untitled"])[0]
    year = doc.get("year") or ""
    link = f"https://ui.adsabs.harvard.edu/abs/{bibcode}/abstract" if bibcode else "https://ui.adsabs.harvard.edu/"

    paper = {
        "title": title,
        "authors": ", ".join(authors),
        "year": year,
        "link": link,
    }

    author_index = None
    for idx, author_name in enumerate(authors[:4]):
        if re.search(rf"\b{re.escape(AUTHOR_SURNAME)}\b", author_name, flags=re.IGNORECASE):
            author_index = idx
            break

    if author_index is None:
        continue

    if author_index == 0:
        first_author.append(paper)
    elif author_index in (1, 2, 3):
        coauthor.append(paper)

# newest-first
def sort_key(p):
    return str(p.get("year", "")), p.get("title", "")

first_author = sorted(first_author, key=sort_key, reverse=True)
coauthor = sorted(coauthor, key=sort_key, reverse=True)

OUTFILE.parent.mkdir(parents=True, exist_ok=True)
with OUTFILE.open("w", encoding="utf-8") as f:
    yaml.safe_dump(
        {"first_author": first_author, "coauthor": coauthor},
        f,
        sort_keys=False,
        allow_unicode=True,
    )

print(f"Wrote {len(first_author)} first-author papers and {len(coauthor)} coauthor papers to {OUTFILE}")