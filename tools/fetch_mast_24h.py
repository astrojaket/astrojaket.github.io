#!/usr/bin/env python3
"""
JWST-only, exoplanet-only fetcher (MAST/CAOM) for a recent window.
- Uses local TrExoLiSTS JWST CSV at assets/data/jwst_trexolists_extended.csv
- Tunables via CLI:
    --days          window length in days (default: 1)
    --slice-hours   size of each time slice (default: 6)
    --pagesize      CAOM pagesize (default: 80)
    --timeout       per-request timeout (default: 300)
    --retries       retries per request (default: 8)
    --instruments   comma list (default: NIRCam,NIRSpec,NIRISS,MIRI,FGS)
    --out           output JSON path (default: assets/data/mast_7d.json)
"""
import argparse, csv, json, os, sys, time, datetime, urllib.request

API = "https://mast.stsci.edu/api/v0/invoke"
TREXO_CSV_LOCAL = "assets/data/jwst_trexolists_extended.csv"

def to_mjd(dt): return (dt.timestamp()/86400.0) + 40587.0
def normalize(name): return ''.join(ch for ch in (name or '').lower() if ch.isalnum())

def load_whitelist(csv_path):
    targets = set()
    if not os.path.exists(csv_path):
        print(f"[warn] TrExoLiSTS file not found at {csv_path}.", file=sys.stderr)
        return targets
    with open(csv_path, encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        headers = {h.lower(): h for h in (reader.fieldnames or [])}
        def col(row, name):
            h = headers.get(name.lower()); return (row.get(h, "") if h else "").strip()
        for row in reader:
            host = col(row, "hostname_nn")
            letter = col(row, "letter_nn")
            if host:
                hnorm = normalize(host)
                targets.add(hnorm)  # host (e.g., "wasp43")
                if letter and len(letter) == 1 and letter.isalpha():
                    targets.add(normalize(host + letter.lower()))  # host+letter (e.g., "wasp43b")
    print(f"[info] Loaded {len(targets)} unique whitelist names from TrExoLiSTS.", file=sys.stderr)
    return targets

def mast_request_jwst(mjd_start, mjd_end, page=1, pagesize=80, instruments=None):
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

def call_mast(request_obj, timeout=300, retries=8, backoff=2.0):
    data = ("request=" + json.dumps(request_obj)).encode("utf-8")
    last_err = None
    for attempt in range(retries+1):
        try:
            req = urllib.request.Request(API, data=data, headers={
                "Content-Type":"application/x-www-form-urlencoded;charset=UTF-8",
                "Accept":"application/json",
                "Connection":"close",
                "User-Agent":"astrojaket-now-observing-fetch/3.0 (+https://astrojake.com)"
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

def fetch_jwst_exoplanets(start_dt, end_dt, whitelist, slice_hours=6, pagesize=80, timeout=300, retries=8, instruments=None):
    rows = []
    t1 = end_dt
    total_hours = (end_dt - start_dt).total_seconds() / 3600.0
    slices = max(1, int(round(total_hours / float(slice_hours))))
    for _ in range(slices):
        t0 = t1 - datetime.timedelta(hours=slice_hours)
        if t0 < start_dt: t0 = start_dt
        mjd_start, mjd_end = to_mjd(t0), to_mjd(t1)
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
            if len(data) < pagesize: break
            page += 1
        t1 = t0
        if t1 <= start_dt: break
        time.sleep(0.3)  # gentle pacing
    # de-dup by obsid
    seen = set(); uniq = []
    for r in rows:
        k = r.get("obsid") or (r.get("obs_id"), r.get("t_min"), r.get("t_max"))
        if k in seen: continue
        seen.add(k); uniq.append(r)
    return uniq

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--days", type=float, default=1.0, help="window length in days (default 1)")
    p.add_argument("--slice-hours", type=float, default=6.0, help="slice size in hours (default 6)")
    p.add_argument("--pagesize", type=int, default=80, help="MAST pagesize (default 80)")
    p.add_argument("--timeout", type=int, default=300, help="per-request timeout seconds (default 300)")
    p.add_argument("--retries", type=int, default=8, help="retries per request (default 8)")
    p.add_argument("--instruments", type=str, default="NIRCam,NIRSpec,NIRISS,MIRI,FGS", help="comma-separated instruments")
    p.add_argument("--out", type=str, default="assets/data/mast_7d.json", help="output JSON path")
    args = p.parse_args()

    end = datetime.datetime.utcnow()
    start = end - datetime.timedelta(days=args.days)

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

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(out, f)
    print(f"Wrote {args.out}", flush=True)

if __name__ == "__main__":
    main()