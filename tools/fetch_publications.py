import os
import re
from pathlib import Path

import requests
import yaml

ORCID = "0000-0003-4844-9838"
OUTFILE = Path("_data/publications.yml")
TOKEN = os.environ.get("ADS_API_TOKEN")

if not TOKEN:
    raise RuntimeError("ADS_API_TOKEN is missing from the environment.")

URL = "https://api.adsabs.harvard.edu/v1/search/query"
PARAMS = {
    "q": f"orcid:{ORCID}",
    "fl": "title,author,bibcode,year,date",
    "sort": "date desc",
    "rows": 500,
}
HEADERS = {"Authorization": f"Bearer {TOKEN}"}

SELF_PATTERNS = [
    r"\bJ\.?\s*Taylor\b",
    r"\bJake\s+Taylor\b",
    r"\bTaylor,\s*J\.?\b",
    r"\bTaylor,\s*Jake\b",
]


def author_is_self(author_name: str) -> bool:
    name = re.sub(r"\s+", " ", author_name.strip())

    # Exclude unrelated Bell papers up front.
    if re.search(r"\bBell\b", name, flags=re.IGNORECASE):
        return False

    return any(re.search(pattern, name, flags=re.IGNORECASE) for pattern in SELF_PATTERNS)


def author_summary(authors: list[str]) -> str:
    shown = authors[:4]
    summary = ", ".join(shown)
    if len(authors) > 4:
        summary += " et al."
    return summary


def paper_sort_key(paper: dict) -> tuple:
    date = paper.get("date") or ""
    year = paper.get("year") or ""
    title = paper.get("title") or ""
    return (str(date), str(year), str(title))


response = requests.get(URL, params=PARAMS, headers=HEADERS, timeout=60)
response.raise_for_status()
payload = response.json()
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
    date = doc.get("date") or ""
    link = (
        f"https://ui.adsabs.harvard.edu/abs/{bibcode}/abstract"
        if bibcode
        else "https://ui.adsabs.harvard.edu/"
    )

    paper = {
        "title": title,
        "authors": f"{author_summary(authors)} ({year})" if year else author_summary(authors),
        "year": year,
        "date": date,
        "link": link,
    }

    author_index = None
    for idx, author_name in enumerate(authors[:4]):
        if author_is_self(author_name):
            author_index = idx
            break

    if author_index is None:
        continue

    if author_index == 0:
        first_author.append(paper)
    elif author_index in (1, 2, 3):
        coauthor.append(paper)

first_author = sorted(first_author, key=paper_sort_key, reverse=True)
coauthor = sorted(coauthor, key=paper_sort_key, reverse=True)

OUTFILE.parent.mkdir(parents=True, exist_ok=True)
with OUTFILE.open("w", encoding="utf-8") as f:
    yaml.safe_dump(
        {"first_author": first_author, "coauthor": coauthor},
        f,
        sort_keys=False,
        allow_unicode=True,
    )

print(f"Wrote {len(first_author)} first-author papers and {len(coauthor)} coauthor papers to {OUTFILE}")
