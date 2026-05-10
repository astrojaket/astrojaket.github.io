
import requests
import yaml
import os
ORCID = "0000-0003-4844-9838"
ADS_TOKEN = os.environ["ADS_API_TOKEN"]

url = "https://api.adsabs.harvard.edu/v1/search/query"

params = {
    "q": f"orcid:{ORCID}",
    "fl": "title,author,bibcode,year",
    "sort": "date desc",
    "rows": 200
}

headers = {
    "Authorization": f"Bearer {ADS_TOKEN}"
}

r = requests.get(url, params=params, headers=headers)
docs = r.json()["response"]["docs"]

first_author = []
coauthor = []

for d in docs:
    authors = d.get("author", [])
    title = d.get("title", ["Untitled"])[0]

    paper = {
        "title": title,
        "authors": ", ".join(authors),
        "year": d.get("year", ""),
        "link": f"https://ui.adsabs.harvard.edu/abs/{d.get('bibcode')}/abstract"
    }

    for idx, author in enumerate(authors[:4]):
        if "Taylor" in author:
            if idx == 0:
                first_author.append(paper)
            else:
                coauthor.append(paper)
            break

with open("_data/publications.yml", "w") as f:
    yaml.dump({
        "first_author": first_author,
        "coauthor": coauthor
    }, f, sort_keys=False)

print("Updated publications.yml")
