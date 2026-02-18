"""
TAPS API v2.1 — Redis-backed persistent cache
Sales data survives container restarts.
"""

import os
import json
import time
import logging
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import redis
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware

FLOWHUB_BASE = "https://api.flowhub.co"
PAGE_SIZE = 500
RATE_LIMIT = 0.2
DAYS_DEFAULT = 31
WOS_DEFAULT = 2.5
MAX_WORKERS = 4

EXCLUDE_PRODUCTS = ['EXIT BAG']
EXCLUDE_STORES = ['MBNV', 'Smoke & Mirrors', 'Cultivation']

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("taps")

app = FastAPI(title="TAPS API", version="2.1")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

# ─── REDIS ────────────────────────────────────────────────────────────────────
REDIS_URL = os.environ.get("REDIS_URL", "")
rdb = None

def get_redis():
    global rdb
    if rdb: return rdb
    if not REDIS_URL:
        log.warning("No REDIS_URL — running without persistence")
        return None
    try:
        rdb = redis.from_url(REDIS_URL, decode_responses=True)
        rdb.ping()
        log.info("Redis connected ✓")
        return rdb
    except Exception as e:
        log.error(f"Redis failed: {e}")
        return None

def redis_set(key, data, ttl=86400*7):
    r = get_redis()
    if r:
        try:
            r.setex(key, ttl, json.dumps(data))
        except: pass

def redis_get(key):
    r = get_redis()
    if r:
        try:
            val = r.get(key)
            if val: return json.loads(val)
        except: pass
    return None

# ─── IN-MEMORY CACHE ─────────────────────────────────────────────────────────
cache = {
    "locations": None, "loc_lookup": {},
    "inventory": None, "inventory_ts": None,
    "sales": None, "sales_store_totals": {},
    "sales_raw_ts": None, "sales_last_date": None, "sales_pulling": False,
    "taps": None, "taps_ts": None,
}

# ─── FLOWHUB API ──────────────────────────────────────────────────────────────
def fh_headers():
    cid = os.environ.get("FLOWHUB_CLIENT_ID", "")
    key = os.environ.get("FLOWHUB_API_KEY", "")
    if not cid or not key:
        raise HTTPException(500, "Flowhub credentials not configured")
    return {"clientId": cid, "key": key}

def fh_get(path, params=None, timeout=120):
    url = f"{FLOWHUB_BASE}{path}"
    headers = fh_headers()
    for attempt in range(3):
        try:
            r = requests.get(url, headers=headers, params=params, timeout=timeout)
            if r.status_code == 200:
                return r.json()
            log.warning(f"{path} returned {r.status_code}")
            if r.status_code == 500 and attempt < 2:
                time.sleep(2 ** attempt)
                continue
            return None
        except requests.exceptions.Timeout:
            log.warning(f"Timeout attempt {attempt+1} for {path}")
            if attempt < 2: time.sleep(2 ** attempt)
        except Exception as e:
            log.error(f"Error: {e}")
            return None
    return None

# ─── LOCATIONS ────────────────────────────────────────────────────────────────
def get_locations():
    if cache["locations"]:
        return cache["locations"]
    data = fh_get("/v0/clientsLocations")
    if not data: return []
    locs = data if isinstance(data, list) else data.get("data", data.get("locations", []))
    retail = []
    loc_lookup = {}
    for loc in locs:
        name = loc.get("locationName", loc.get("name", ""))
        if any(ex.lower() in name.lower() for ex in EXCLUDE_STORES): continue
        lid = loc.get("locationId", loc.get("importId", ""))
        store_clean = name.replace("Thrive ", "").split(" - ")[0] if " - RD" in name else name.replace("Thrive ", "")
        loc["_name"] = name
        loc["_id"] = lid
        loc["_clean"] = store_clean
        retail.append(loc)
        loc_lookup[lid] = name
        iid = loc.get("importId", "")
        if iid and iid != lid: loc_lookup[iid] = name
    cache["locations"] = retail
    cache["loc_lookup"] = loc_lookup
    log.info(f"Loaded {len(retail)} locations")
    return retail

# ─── INVENTORY ────────────────────────────────────────────────────────────────
def pull_inventory():
    log.info("Pulling inventory...")
    locations = get_locations()
    if not locations: return None
    loc_lookup = cache.get("loc_lookup", {})
    first_id = locations[0].get("importId", locations[0].get("id", ""))
    data = fh_get("/v0/inventory", params={"locationId": first_id})
    if not data: return None
    items = data.get("data", [])
    log.info(f"Raw API items: {len(items)}")
    inventory = []
    for item in items:
        item_loc = item.get("locationId", "")
        store_name = loc_lookup.get(item_loc)
        if not store_name: continue
        if any(ex.lower() in store_name.lower() for ex in EXCLUDE_STORES): continue
        pname = item.get("productName", item.get("parentProductName", ""))
        if any(ex.lower() in pname.lower() for ex in EXCLUDE_PRODUCTS): continue
        qty = item.get("quantity", 0) or 0
        if qty <= 0: continue
        uc = (item.get("costInMinorUnits", 0) or 0) / 100
        up = (item.get("priceInMinorUnits", item.get("preTaxPriceInPennies", 0)) or 0) / 100
        sc = store_name.replace("Thrive ", "").split(" - ")[0] if " - RD" in store_name else store_name.replace("Thrive ", "")
        inventory.append({"s": sc, "vid": item.get("variantId", ""), "p": pname,
            "cat": item.get("category", item.get("customCategoryName", "")),
            "b": item.get("brand", ""), "sup": item.get("supplierName", "") or "",
            "typ": item.get("type", ""), "str": item.get("strainName", "") or "",
            "oh": qty, "uc": round(uc, 2), "up": round(up, 2),
            "ic": round(qty * uc, 2), "ir": round(qty * up, 2)})
    cache["inventory"] = inventory
    cache["inventory_ts"] = datetime.utcnow().isoformat() + "Z"
    log.info(f"Inventory: {len(inventory)} items, {sum(i['oh'] for i in inventory):,}u, ${sum(i['ic'] for i in inventory):,.2f}")
    return inventory

# ─── SALES: SINGLE STORE ─────────────────────────────────────────────────────
def pull_store_sales(loc, start_date, end_date):
    name = loc["_name"]
    store_clean = loc["_clean"]
    loc_id = loc.get("importId", loc.get("id", ""))
    data = fh_get(f"/v1/orders/findByLocationId/{loc_id}",
                  params={"created_after": start_date, "created_before": end_date, "page_size": 1, "page": 1})
    if not data: return store_clean, []
    total = data.get("total", 0)
    log.info(f"  {name}: {total:,} orders")
    if total == 0: return store_clean, []
    items = []
    page = 1
    pulled = 0
    while pulled < total:
        data = fh_get(f"/v1/orders/findByLocationId/{loc_id}",
                      params={"created_after": start_date, "created_before": end_date,
                              "page_size": PAGE_SIZE, "page": page, "order_by": "asc"})
        if not data: break
        orders = data.get("orders", [])
        if not orders: break
        for order in orders:
            if order.get("orderStatus", "sold").lower() in ("cancelled", "voided") or order.get("voided"): continue
            for it in order.get("itemsInCart", []):
                tp = it.get("totalPrice", 0) or 0
                td = it.get("totalDiscounts", 0) or 0
                items.append({"s": store_clean, "vid": it.get("variantId", ""),
                    "q": it.get("quantity", 0) or 0, "tp": round(tp, 2), "td": round(td, 2),
                    "nr": round(tp - td, 2), "tc": round(it.get("totalCost", 0) or 0, 2)})
        pulled += len(orders)
        log.info(f"    {name}: p{page} ({pulled:,}/{total:,})")
        if len(orders) < PAGE_SIZE: break
        page += 1
        time.sleep(RATE_LIMIT)
    return store_clean, items

# ─── SALES AGGREGATION ───────────────────────────────────────────────────────
def aggregate_sales(raw_items, days):
    weeks = days / 7
    agg = {}
    store_totals = {}
    for item in raw_items:
        key = (item["s"], item["vid"])
        if key not in agg:
            agg[key] = {"s": item["s"], "vid": item["vid"], "q": 0, "tp": 0, "td": 0, "nr": 0, "tc": 0}
        agg[key]["q"] += item["q"]
        agg[key]["tp"] += item["tp"]
        agg[key]["td"] += item["td"]
        agg[key]["nr"] += item["nr"]
        agg[key]["tc"] += item["tc"]
        if item["s"] not in store_totals:
            store_totals[item["s"]] = {"nr": 0, "tp": 0, "td": 0, "tc": 0, "q": 0}
        store_totals[item["s"]]["nr"] += item["nr"]
        store_totals[item["s"]]["tp"] += item["tp"]
        store_totals[item["s"]]["td"] += item["td"]
        store_totals[item["s"]]["tc"] += item["tc"]
        store_totals[item["s"]]["q"] += item["q"]
    result = []
    for d in agg.values():
        d["wv"] = d["q"] / weeks
        result.append(d)
    cache["sales_store_totals"] = store_totals
    return result

# ─── SALES: FULL PULL (PARALLEL) + REDIS PERSIST ─────────────────────────────
def pull_sales_full(days=DAYS_DEFAULT):
    if cache["sales_pulling"]:
        log.info("Sales pull already in progress")
        return cache.get("sales")
    cache["sales_pulling"] = True
    log.info(f"FULL sales pull ({days} days, {MAX_WORKERS} workers)...")
    try:
        locations = get_locations()
        end_date = datetime.utcnow().strftime("%Y-%m-%d")
        start_date = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
        all_items = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = {ex.submit(pull_store_sales, loc, start_date, end_date): loc for loc in locations}
            for f in as_completed(futures):
                sc, items = f.result()
                all_items.extend(items)
                log.info(f"  ✓ {sc}: {len(items):,} items")
        sales_agg = aggregate_sales(all_items, days)
        ts = datetime.utcnow().isoformat() + "Z"
        cache["sales"] = sales_agg
        cache["sales_raw_ts"] = ts
        cache["sales_last_date"] = end_date
        log.info(f"Sales done: {len(all_items):,} raw → {len(sales_agg):,} aggregated")

        # Persist to Redis
        redis_set("taps:sales", sales_agg)
        redis_set("taps:sales_store_totals", cache["sales_store_totals"])
        redis_set("taps:sales_meta", {"ts": ts, "last_date": end_date, "count": len(sales_agg)})
        log.info("Sales persisted to Redis ✓")

        return sales_agg
    finally:
        cache["sales_pulling"] = False

# ─── SALES: INCREMENTAL ──────────────────────────────────────────────────────
def pull_sales_incremental():
    if cache["sales_pulling"]:
        return cache.get("sales")
    if not cache.get("sales"):
        return pull_sales_full()
    cache["sales_pulling"] = True
    last_date = cache.get("sales_last_date")
    if not last_date:
        cache["sales_pulling"] = False
        return pull_sales_full()
    log.info(f"INCREMENTAL sales (since {last_date})...")
    try:
        locations = get_locations()
        end_date = datetime.utcnow().strftime("%Y-%m-%d")
        new_items = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = {ex.submit(pull_store_sales, loc, last_date, end_date): loc for loc in locations}
            for f in as_completed(futures):
                sc, items = f.result()
                new_items.extend(items)
        if new_items:
            weeks = DAYS_DEFAULT / 7
            existing = cache["sales"]
            new_agg = {}
            for item in new_items:
                key = (item["s"], item["vid"])
                if key not in new_agg:
                    new_agg[key] = {"q": 0, "tp": 0, "td": 0, "nr": 0, "tc": 0}
                new_agg[key]["q"] += item["q"]
                new_agg[key]["tp"] += item["tp"]
                new_agg[key]["td"] += item["td"]
                new_agg[key]["nr"] += item["nr"]
                new_agg[key]["tc"] += item["tc"]
            st = cache.get("sales_store_totals", {})
            for entry in existing:
                key = (entry["s"], entry["vid"])
                if key in new_agg:
                    for k in ["q","tp","td","nr","tc"]:
                        entry[k] += new_agg[key][k]
                    entry["wv"] = entry["q"] / weeks
                    del new_agg[key]
            for (s, vid), d in new_agg.items():
                existing.append({"s": s, "vid": vid, **d, "wv": d["q"] / weeks})
            for item in new_items:
                if item["s"] not in st:
                    st[item["s"]] = {"nr": 0, "tp": 0, "td": 0, "tc": 0, "q": 0}
                for k in ["nr","tp","td","tc","q"]:
                    st[item["s"]][k] += item[k]
            cache["sales_store_totals"] = st
            cache["sales"] = existing

        ts = datetime.utcnow().isoformat() + "Z"
        cache["sales_last_date"] = end_date
        cache["sales_raw_ts"] = ts
        log.info(f"Incremental: {len(new_items):,} new items merged")

        # Persist to Redis
        redis_set("taps:sales", cache["sales"])
        redis_set("taps:sales_store_totals", cache["sales_store_totals"])
        redis_set("taps:sales_meta", {"ts": ts, "last_date": end_date, "count": len(cache["sales"])})
        log.info("Sales persisted to Redis ✓")

        return cache["sales"]
    finally:
        cache["sales_pulling"] = False

# ─── LOAD SALES FROM REDIS ON STARTUP ─────────────────────────────────────────
def load_sales_from_redis():
    sales = redis_get("taps:sales")
    if not sales:
        log.info("No cached sales in Redis")
        return False
    store_totals = redis_get("taps:sales_store_totals")
    meta = redis_get("taps:sales_meta")
    cache["sales"] = sales
    cache["sales_store_totals"] = store_totals or {}
    if meta:
        cache["sales_raw_ts"] = meta.get("ts")
        cache["sales_last_date"] = meta.get("last_date")
    log.info(f"Loaded sales from Redis: {len(sales):,} items (pulled {meta.get('ts', '?')})")
    return True

# ─── TAPS ENGINE ──────────────────────────────────────────────────────────────
def run_taps(wos_target=WOS_DEFAULT, days=DAYS_DEFAULT):
    inventory = cache.get("inventory") or pull_inventory()
    sales = cache.get("sales")
    if not inventory:
        raise HTTPException(500, "No inventory data")
    weeks = days / 7
    inv_map = {}
    for item in inventory:
        key = (item["s"], item["vid"])
        if key not in inv_map:
            inv_map[key] = {**item}
        else:
            inv_map[key]["oh"] += item["oh"]
            inv_map[key]["ic"] += item["ic"]
            inv_map[key]["ir"] += item["ir"]
    sales_map = {}
    if sales:
        for entry in sales:
            sales_map[(entry["s"], entry["vid"])] = entry
    vid_vels = {}
    if sales:
        vv = {}
        for e in sales:
            vv.setdefault(e["vid"], []).append(e["wv"])
        for vid, vels in vv.items():
            vid_vels[vid] = sum(vels) / len(vels)
    stores_with_sales = set(e["s"] for e in sales) if sales else set()
    sales_store_totals = cache.get("sales_store_totals", {})
    products = []
    for (store, vid), inv in inv_map.items():
        sd = sales_map.get((store, vid))
        sold = sd["q"] if sd else 0
        vel = sd["wv"] if sd else 0
        nr = sd["nr"] if sd else 0
        tc_sold = sd["tc"] if sd else 0
        if sold == 0 and store not in stores_with_sales and vid in vid_vels:
            vel = vid_vels[vid]
            sold = vel * weeks
        wos = inv["oh"] / vel if vel > 0 else None
        par = max(round(vel * wos_target), 0) if vel > 0 else 0
        oq = max(par - inv["oh"], 0)
        mgn = round((nr - tc_sold) / nr * 100, 1) if nr > 0 else 0
        cls = "A" if vel >= 20 else "B" if vel >= 10 else "C" if vel >= 3 else "D"
        products.append({"s": store, "p": inv["p"][:55], "b": (inv["b"] or "")[:20],
            "cat": inv["cat"], "cls": cls, "wv": round(vel, 2), "oh": inv["oh"],
            "wos": round(wos, 1) if wos and wos < 999 else None,
            "par": par, "oq": oq, "nr": round(nr, 2), "cogs": round(tc_sold, 2),
            "mgn": mgn, "ic": round(inv["ic"], 2), "uc": inv["uc"], "up": inv["up"],
            "sup": (inv["sup"] or "")[:30]})
    tnr = sum(s["nr"] for s in sales_store_totals.values()) if sales_store_totals else 0
    ttp = sum(s["tp"] for s in sales_store_totals.values()) if sales_store_totals else 0
    ttd = sum(s["td"] for s in sales_store_totals.values()) if sales_store_totals else 0
    ttc = sum(s["tc"] for s in sales_store_totals.values()) if sales_store_totals else 0
    tq = sum(s["q"] for s in sales_store_totals.values()) if sales_store_totals else 0
    stats = {"period": f"{(datetime.utcnow()-timedelta(days=days)).strftime('%b %d')} - {datetime.utcnow().strftime('%b %d %Y')}",
        "source": "Flowhub API (Live)", "stores": len(set(p["s"] for p in products)),
        "net_revenue": round(tnr, 2), "gross_sales": round(ttp, 2), "discounts": round(ttd, 2),
        "cogs": round(ttc, 2), "gross_profit": round(tnr - ttc, 2),
        "margin": round((tnr - ttc) / tnr * 100, 1) if tnr > 0 else 0,
        "discount_rate": round(ttd / ttp * 100, 1) if ttp > 0 else 0,
        "units_sold": int(tq), "total_products": len(products),
        "total_inv_cost": round(sum(p["ic"] for p in products), 2),
        "total_inv_units": sum(p["oh"] for p in products),
        "inventory_ts": cache.get("inventory_ts"), "sales_ts": cache.get("sales_raw_ts")}
    ss = []
    for sn in sorted(set(p["s"] for p in products)):
        sp = [p for p in products if p["s"] == sn]
        dead = [p for p in sp if p["wv"] == 0]
        over = [p for p in sp if p["wos"] and p["wos"] > 8 and p["wv"] > 0]
        st = sales_store_totals.get(sn, {})
        ic = sum(p["ic"] for p in sp)
        ss.append({"s": sn, "rev": round(st.get("nr", 0)), "cogs": round(st.get("tc", 0)),
            "margin": round((st.get("nr", 0) - st.get("tc", 0)) / st["nr"] * 100, 1) if st.get("nr", 0) > 0 else 0,
            "disc": round(st.get("td", 0)), "units": int(st.get("q", 0)),
            "products": len(sp), "inv_cost": round(ic), "inv_units": sum(p["oh"] for p in sp),
            "dead_cost": round(sum(p["ic"] for p in dead)),
            "dead_pct": round(sum(p["ic"] for p in dead) / ic * 100, 1) if ic > 0 else 0,
            "over_cost": round(sum(p["ic"] for p in over))})
    result = {"st": stats, "ss": ss, "pd": products}
    cache["taps"] = result
    cache["taps_ts"] = datetime.utcnow().isoformat() + "Z"
    return result

# ─── ENDPOINTS ────────────────────────────────────────────────────────────────

@app.get("/")
def root():
    return {"status": "ok", "app": "TAPS", "version": "2.1",
            "has_cid": bool(os.environ.get("FLOWHUB_CLIENT_ID")),
            "has_key": bool(os.environ.get("FLOWHUB_API_KEY")),
            "has_redis": bool(REDIS_URL)}

@app.get("/api/status")
def status():
    return {"inventory_ts": cache.get("inventory_ts"), "sales_ts": cache.get("sales_raw_ts"),
        "sales_pulling": cache.get("sales_pulling", False),
        "products": len(cache["taps"]["pd"]) if cache.get("taps") else 0,
        "locations": len(cache.get("locations") or []),
        "sales_items": len(cache.get("sales") or []),
        "redis": bool(get_redis())}

@app.get("/api/inventory")
def get_inventory():
    inv = pull_inventory()
    if not inv: raise HTTPException(500, "Failed to pull inventory")
    return {"items": len(inv), "units": sum(i["oh"] for i in inv),
        "cost": round(sum(i["ic"] for i in inv), 2), "ts": cache["inventory_ts"]}

@app.post("/api/refresh-sales")
def refresh_sales(background_tasks: BackgroundTasks, days: int = DAYS_DEFAULT):
    if cache.get("sales_pulling"):
        return {"status": "already_pulling", "sales_ts": cache.get("sales_raw_ts")}
    background_tasks.add_task(pull_sales_full, days)
    return {"status": "started_full", "days": days}

@app.post("/api/refresh-sales-quick")
def refresh_sales_quick(background_tasks: BackgroundTasks):
    if cache.get("sales_pulling"):
        return {"status": "already_pulling"}
    if not cache.get("sales"):
        background_tasks.add_task(pull_sales_full)
        return {"status": "no_cache_full_pull"}
    background_tasks.add_task(pull_sales_incremental)
    return {"status": "started_incremental", "since": cache.get("sales_last_date")}

@app.get("/api/sales-status")
def sales_status():
    return {"pulling": cache.get("sales_pulling", False), "ts": cache.get("sales_raw_ts"),
        "items": len(cache.get("sales") or []), "last_date": cache.get("sales_last_date")}

@app.get("/api/taps")
def get_taps(wos: float = WOS_DEFAULT, days: int = DAYS_DEFAULT, refresh_inventory: bool = False):
    if refresh_inventory or not cache.get("inventory"):
        pull_inventory()
    if not cache.get("sales"):
        # Try Redis first before doing a full pull
        if not load_sales_from_redis():
            pull_sales_full(days)
    result = run_taps(wos, days)
    return result

@app.get("/api/taps/cached")
def get_taps_cached():
    if cache.get("taps"): return cache["taps"]
    raise HTTPException(404, "No cached TAPS data. Call /api/taps first.")

@app.on_event("startup")
async def startup():
    log.info("TAPS API v2.1 — Redis-backed persistent cache")
    r = get_redis()
    if r:
        loaded = load_sales_from_redis()
        if loaded:
            log.info("Sales restored from Redis — ready immediately!")
        else:
            log.info("No sales in Redis. Pull via /api/refresh-sales")
    else:
        log.info("Running without Redis persistence")
    log.info("Ready.")
