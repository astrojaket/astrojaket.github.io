#!/usr/bin/env python3
"""
JWST-only, exoplanet-only fetcher for the last 24 hours (CAOM via MAST).
- Filters to exoplanet targets using local TrExoLiSTS JWST CSV:
    assets/data/jwst_trexolists_extended.csv
- Robust and tunable via CLI:
    --slice-hours    size of each time slice (default: 1 hour)
    --pagesize       MAST pagesize per request (default: 100)
    --timeout        per-request timeout in seconds (default: 200)
    --retries        retries per request (default: 6)
    --instruments    comma-separated allowlist (e.g., NIRCam,NIRSpec,NIRISS,MIRI)
- Output:
    assets/data/mast_24h.json  (JWST rows only)
"""
import argparse, csv, json, os, sys, time, datetime, urllib.request

API = "https://mast.stsci.edu/api/v0/invoke"
TREXO_CSV_LOCAL = "assets/data/jwst_trexolists_extended.csv"

def to_mjd(dt):
    return (dt.timestamp()/86400.0) + 40587.0

def normalize(name):
    if not name: return ""
    return ''.join(ch for ch in name.lower() if ch.isalnum())

def load_whitelist(csv_path):
    targets = set()
    if not os.path.exists(csv_path):
        print(f"[warn] TrExoLiSTS file not found at {csv_path}.", file=sys.stderr)
        return targets
    with open(csv_path, encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        headers = {h.lower():h for h in (reader.fieldnames or [])}
        def col(row, name):
            h = headers.get(name.lower()); return (row.get(h, "") if h else "").strip()
        for row in reader:
            host = col(row, "hostname_nn")
            letter = col(row, "letter_nn")
            if host:
                hnorm = normalize(host)         # e.g. "wasp43"
                targets.add(hnorm)
                if letter and len(letter) == 1 and letter.isalpha():
                    targets.add(normalize(host + letter.lower()))  # "wasp43b"
    print(f"[info] Loaded {len(targets)} unique whitelist names from TrExoLiSTS.", file=sys.stderr)
    return targets

def mast_request_jwst(mjd_start, mjd_end, page=1, pagesize=100, instruments=None):
    # Minimal columns to keep payloads small
    filters = [
        {"paramName":"obs_collection","values":["JWST"]},
        {"paramName":"t_min","values":[{"max":mjd_end}]},
        {"paramName":"t_max","values":[{"min":mjd_start}]}
    ]
    if instruments:
        filters.append({"paramName":"instrument_name","values":instruments})
    return {
        "service":"Mast.Caom.Filtered",
        "format":"json",
        "params":{
            "columns":"obs_collection,obsid,obs_id,proposal_id,instrument_name,target_name,t_min,t_max",
            "filters": filters
        },
        "pagesize": pagesize,
        "page": page
    }

def call_mast(request_obj, timeout=200, retries=6, backoff=2.0):
    data = ("request=" + json.dumps(request_obj)).encode("utf-8")
    last_err = None
    for attempt in range(retries+1):
        try:
            req = urllib.request.Request(API, data=data, headers={
                "Content-Type":"application/x-www-form-urlencoded;charset=UTF-8",
                "Accept":"application/json",
                "Connection":"close",
                "User-Agent":"astrojaket-now-observing-fetch/2.0 (+https://astrojake.com)"
            })
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except Exception as e:
            last_err = e
            if attempt < retries:
                sleep_s = (backoff ** attempt)
                print(f"[retry {attempt+1}/{retries}] {e}; sleeping {sleep_s:.1f}s...", file=sys.stderr, flush=True)
                time.sleep(sleep_s)
            else:
                raise last_err

def fetch_jwst_exoplanets(start_dt, end_dt, whitelist, slice_hours=1, pagesize=100, timeout=200, retries=6, instruments=None):
    rows = []
    t1 = end_dt
    slices = int(max(1, round(24 / float(slice_hours))))
    for _ in range(slices):
        t0 = t1 - datetime.timedelta(hours=slice_hours)
        mjd_start = to_mjd(t0)
        mjd_end   = to_mjd(t1)
        print(f"[JWST] slice {t0.isoformat()}Z -> {t1.isoformat()}Z", file=sys.stderr, flush=True)
        page = 1
        while True:
            req = mast_request_jwst(mjd_start, mjd_end, page=page, pagesize=pagesize, instruments=instruments)
            res = call_mast(req, timeout=timeout, retries=retries, backoff=2.0)
            data = res.get("data", [])
            print(f"  page {page}: rows={len(data)}", file=sys.stderr, flush=True)
            for r in data:
                tn = normalize(r.get("target_name",""))
                if tn in whitelist:
                    rows.append(r)
            if len(data) < pagesize:
                break
            page += 1
        t1 = t0
        time.sleep(0.4)  # gentle pacing
    # de-dup by obsid
    seen = set(); uniq = []
    for r in rows:
        k = r.get("obsid") or (r.get("obs_id"), r.get("t_min"), r.get("t_max"))
        if k in seen: continue
        seen.add(k); uniq.append(r)
    return uniq

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--slice-hours", type=float, default=1.0, help="Slice size in hours (default: 1)")
    parser.add_argument("--pagesize", type=int, default=100, help="MAST pagesize (default: 100)")
    parser.add_argument("--timeout", type=int, default=200, help="Per-request timeout seconds (default: 200)")
    parser.add_argument("--retries", type=int, default=6, help="Retries per request (default: 6)")
    parser.add_argument("--instruments", type=str, default="NIRCam,NIRSpec,NIRISS,MIRI,FGS", help="Comma-separated JWST instrument names to include")
    args = parser.parse_args()

    end = datetime.datetime.utcnow()
    start = end - datetime.timedelta(hours=24)

    wl = load_whitelist(TREXO_CSV_LOCAL)
    instruments = [s.strip() for s in (args.instruments or "").split(",") if s.strip()] or None

    out = {
        "generated_utc": end.replace(microsecond=0).isoformat()+"Z",
        "window_utc": [start.replace(microsecond=0).isoformat()+"Z", end.replace(microsecond=0).isoformat()+"Z"],
        "jwst": []
    }
    try:
        out["jwst"] = fetch_jwst_exoplanets(
            start, end, wl,
            slice_hours=args.slice_hours,
            pagesize=args.pagesize,
            timeout=args.timeout,
            retries=args.retries,
            instruments=instruments
        )
        print(f"[done] JWST exoplanet rows: {len(out['jwst'])}", file=sys.stderr, flush=True)
    except Exception as e:
        print(f"Error fetching JWST: {e}", file=sys.stderr, flush=True)
        out["jwst_error"] = str(e)

    os.makedirs("assets/data", exist_ok=True)
    with open("assets/data/mast_24h.json","w",encoding="utf-8") as f:
        json.dump(out, f)
    print("Wrote assets/data/mast_24h.json", flush=True)

if __name__ == "__main__":
    main()
