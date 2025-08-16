#!/usr/bin/env python3
import json, os, sys, urllib.request, urllib.error, datetime

API = "https://mast.stsci.edu/api/v0/invoke"

def to_mjd(dt):
    return (dt.timestamp()/86400.0) + 40587.0

def mast_request(collection, mjd_start, mjd_end):
    req = {
        "service":"Mast.Caom.Filtered",
        "format":"json",
        "params":{
            "columns":"obs_collection,obsid,obs_id,obs_title,proposal_id,proposal_pi,instrument_name,target_name,t_min,t_max",
            "filters":[
                {"paramName":"obs_collection","values":[collection]},
                {"paramName":"t_min","values":[{"max":mjd_end}]},
                {"paramName":"t_max","values":[{"min":mjd_start}]}
            ]
        },
        "pagesize": 2000,
        "page": 1
    }
    return req

def call_mast(request):
    data = ("request=" + json.dumps(request)).encode("utf-8")
    req = urllib.request.Request(API, data=data, headers={
        "Content-Type":"application/x-www-form-urlencoded;charset=UTF-8",
        "Accept":"application/json"
    })
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read().decode("utf-8"))

def main():
    end = datetime.datetime.utcnow()
    start = end - datetime.timedelta(hours=24)
    mjd_start = to_mjd(start)
    mjd_end   = to_mjd(end)

    out = {"generated_utc": end.replace(microsecond=0).isoformat()+"Z"}

    for coll, key in [("JWST","jwst"), ("HST","hst")]:
        req = mast_request(coll, mjd_start, mjd_end)
        try:
            res = call_mast(req)
            if res.get("status","") not in ("COMPLETE",""):
                print(f"{coll} status: {res.get('status')}", file=sys.stderr)
            out[key] = res.get("data", [])
        except Exception as e:
            print(f"Error fetching {coll}: {e}", file=sys.stderr)
            out[key] = []

    os.makedirs("assets/data", exist_ok=True)
    with open("assets/data/mast_24h.json","w",encoding="utf-8") as f:
        json.dump(out, f)
    print("Wrote assets/data/mast_24h.json with", len(out.get("jwst",[])), "JWST and", len(out.get("hst",[])), "HST rows.")

if __name__ == "__main__":
    main()