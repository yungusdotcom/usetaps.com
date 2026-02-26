"""
TAPS API v3.0 — Redis-first, non-blocking architecture
  - Startup: fast, zero Flowhub calls
  - Reads: Redis-only, stale-while-revalidate
  - Writes: background rebuild with distributed lock + cursor-based incremental
  - Cron: POST /internal/rebuild-cache (Railway cron every 15 min)
"""

import os
import json
import time
import logging
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from typing import Optional

import redis
import requests
from fastapi import FastAPI, BackgroundTasks, HTTPException
from fastapi.middleware.cors import CORSMiddleware

# ─── CONFIG ──────────────────────────────────────────────────────────────────

FLOWHUB_BASE = "https://api.flowhub.co"
PAGE_SIZE = 500
RATE_LIMIT_S = 0.25
DAYS_DEFAULT = 31
WOS_DEFAULT = 2.5
MAX_FETCH_WORKERS = 3
REBUILD_LOCK_TTL = 300
TAPS_CACHE_TTL = 600
SALES_CACHE_TTL = 86400 * 7
SUPPLIER_CACHE_TTL = 86400 * 30
INVENTORY_CACHE_TTL = 3600
CURSOR_TTL = 86400 * 14

EXCLUDE_PRODUCTS = ["EXIT BAG"]
EXCLUDE_STORES = ["MBNV", "Smoke & Mirrors", "Cultivation"]

# ─── COGS OVERRIDES ──────────────────────────────────────────────────────────
# Brand + category/type keyword → override unit cost
# Applied during inventory pull AND TAPS engine to correct Flowhub COGS
COGS_OVERRIDES = [
    {"brand": "Fade", "cat": "Carts", "uc": 10.65},
    {"brand": "Fade", "cat": "Disposables", "uc": 12.48},
    {"brand": "Retreat", "cat": "Carts", "uc": 10.57},
    {"brand": "Retreat", "cat": "Disposables", "uc": 12.40},
    {"brand": "Green & Gold", "cat": "Flower", "uc": 8.63},
    {"brand": "Pistola", "cat": "Flower", "uc": 8.63},
    {"brand": "Hustle & Grow", "cat": "Flower", "uc": 6.78},
    {"brand": "H&G", "cat": "Flower", "uc": 6.78},
    {"brand": "Haus", "cat": "Flower", "uc": 6.78},
]


def get_cogs_override(brand: str, cat: str, product_name: str) -> Optional[float]:
    """Check if a product matches a COGS override. Returns override unit cost or None."""
    brand_l = (brand or "").strip().lower()
    cat_l = (cat or "").lower()
    for rule in COGS_OVERRIDES:
        if rule["brand"].lower() != brand_l:
            continue
        if rule["cat"].lower() != cat_l:
            continue
        return rule["uc"]
    return None

CANNABIS_CATS = ["Flower", "Pre Rolls", "Concentrates", "Carts", "Disposables",
                 "Edibles", "Infused Flower", "Capsules", "Tinctures", "Topicals"]

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
log = logging.getLogger("taps")

app = FastAPI(title="TAPS API", version="3.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"],
)

# ─── REDIS — eager connect at startup ────────────────────────────────────────

REDIS_URL = os.environ.get("REDIS_URL", "")
rdb: Optional[redis.Redis] = None


def connect_redis() -> Optional[redis.Redis]:
    global rdb
    if not REDIS_URL:
        log.error("REDIS_URL not set — persistence DISABLED. Set REDIS_URL env var.")
        return None
    try:
        client = redis.from_url(
            REDIS_URL, decode_responses=True,
            socket_connect_timeout=5, socket_timeout=5, retry_on_timeout=True,
        )
        client.ping()
        info = client.info("server")
        log.info(f"Redis connected ✓  version={info.get('redis_version', '?')}")
        rdb = client
        return rdb
    except redis.ConnectionError as e:
        log.error(f"Redis connection FAILED: {e}")
        return None
    except Exception as e:
        log.error(f"Redis unexpected error: {e}")
        return None


def redis_ok() -> bool:
    if not rdb:
        return False
    try:
        rdb.ping()
        return True
    except Exception:
        return False


def redis_set(key: str, data, ttl: int = TAPS_CACHE_TTL):
    if not rdb:
        return
    t0 = time.monotonic()
    try:
        payload = json.dumps(data, default=str)
        rdb.setex(key, ttl, payload)
        dt = (time.monotonic() - t0) * 1000
        log.info(f"redis SET {key} ({len(payload)/1024:.1f}KB, ttl={ttl}s) {dt:.0f}ms")
    except Exception as e:
        log.error(f"redis SET {key} failed: {e}")


def redis_get(key: str):
    if not rdb:
        return None
    try:
        val = rdb.get(key)
        if val:
            return json.loads(val)
    except Exception as e:
        log.error(f"redis GET {key} failed: {e}")
    return None


# ─── DISTRIBUTED LOCK ────────────────────────────────────────────────────────

LOCK_KEY = "cache:rebuild:lock"


@contextmanager
def rebuild_lock(ttl: int = REBUILD_LOCK_TTL):
    if not rdb:
        log.warning("No Redis — proceeding without lock")
        yield True
        return
    lock_id = f"{os.getpid()}-{time.monotonic_ns()}"
    acquired = rdb.set(LOCK_KEY, lock_id, nx=True, ex=ttl)
    if not acquired:
        log.info("Rebuild lock NOT acquired (held by another process)")
        yield False
        return
    log.info(f"Rebuild lock acquired (id={lock_id}, ttl={ttl}s)")
    try:
        yield True
    finally:
        try:
            pipe = rdb.pipeline()
            pipe.watch(LOCK_KEY)
            if pipe.get(LOCK_KEY) == lock_id:
                pipe.multi()
                pipe.delete(LOCK_KEY)
                pipe.execute()
                log.info("Rebuild lock released ✓")
            else:
                log.warning("Lock expired before release")
        except Exception as e:
            log.error(f"Lock release error: {e}")


# ─── FLOWHUB API ─────────────────────────────────────────────────────────────

def fh_headers():
    cid = os.environ.get("FLOWHUB_CLIENT_ID", "")
    key = os.environ.get("FLOWHUB_API_KEY", "")
    if not cid or not key:
        raise RuntimeError("Flowhub credentials not configured")
    return {"clientId": cid, "key": key}


def fh_get(path: str, params=None, timeout=120):
    url = f"{FLOWHUB_BASE}{path}"
    headers = fh_headers()
    for attempt in range(3):
        try:
            r = requests.get(url, headers=headers, params=params, timeout=timeout)
            if r.status_code == 200:
                return r.json()
            log.warning(f"FH {path} → {r.status_code}")
            if r.status_code in (429, 500, 502, 503) and attempt < 2:
                wait = 2 ** (attempt + 1)
                log.info(f"  retrying in {wait}s...")
                time.sleep(wait)
                continue
            return None
        except requests.exceptions.Timeout:
            log.warning(f"FH timeout attempt {attempt+1} for {path}")
            if attempt < 2:
                time.sleep(2 ** attempt)
        except Exception as e:
            log.error(f"FH error: {e}")
            return None
    return None


# ─── LOCATIONS (Redis-cached) ────────────────────────────────────────────────

def get_locations() -> list:
    cached = redis_get("taps:locations")
    if cached:
        return cached
    data = fh_get("/v0/clientsLocations")
    if not data:
        return []
    locs = data if isinstance(data, list) else data.get("data", data.get("locations", []))
    retail = []
    for loc in locs:
        name = loc.get("locationName", loc.get("name", ""))
        if any(ex.lower() in name.lower() for ex in EXCLUDE_STORES):
            continue
        lid = loc.get("locationId", loc.get("importId", ""))
        iid = loc.get("importId", "")
        store_clean = (name.replace("Thrive ", "").split(" - ")[0]
                       if " - RD" in name else name.replace("Thrive ", ""))
        retail.append({"_name": name, "_id": lid, "_iid": iid, "_clean": store_clean})
    redis_set("taps:locations", retail, ttl=86400)
    log.info(f"Loaded {len(retail)} retail locations from Flowhub")
    return retail


# ─── CATEGORY REFINEMENT ─────────────────────────────────────────────────────

def refine_category(cat: str, typ: str, product_name: str) -> str:
    cat_l = (cat or "").lower()
    typ_l = (typ or "").lower()
    pn_l = (product_name or "").lower()
    if "pre-roll" in typ_l or "pre roll" in typ_l or "preroll" in typ_l:
        return "Pre Rolls"
    if cat_l == "flower" and any(kw in pn_l for kw in ("pre-roll", "pre roll", "preroll", "joint", "blunt")):
        return "Pre Rolls"
    if "cartridge" in typ_l or "cart" in typ_l or "vape cart" in typ_l:
        return "Carts"
    if cat_l == "concentrates" and any(kw in pn_l for kw in ("cart", "cartridge", "pod", "510")):
        return "Carts"
    if "disposable" in typ_l:
        return "Disposables"
    if cat_l == "concentrates" and any(kw in pn_l for kw in ("disposable", "dispos", "all-in-one", "aio")):
        return "Disposables"
    return cat or ""


# ─── INVENTORY PULL ──────────────────────────────────────────────────────────

def pull_inventory() -> list:
    t0 = time.monotonic()
    log.info("Pulling inventory from Flowhub...")
    locations = get_locations()
    if not locations:
        log.error("No locations available")
        return []
    loc_lookup = {}
    for loc in locations:
        loc_lookup[loc["_id"]] = loc["_name"]
        if loc["_iid"] and loc["_iid"] != loc["_id"]:
            loc_lookup[loc["_iid"]] = loc["_name"]
    first_id = locations[0].get("_iid") or locations[0].get("_id")
    data = fh_get("/v0/inventory", params={"locationId": first_id})
    if not data:
        log.error("Inventory pull returned nothing")
        return []
    items = data.get("data", [])
    log.info(f"Raw inventory items: {len(items)}")

    sup_map = redis_get("taps:supplier_map") or {}
    sup_found = 0
    inventory = []
    for item in items:
        item_loc = item.get("locationId", "")
        store_name = loc_lookup.get(item_loc)
        if not store_name:
            continue
        if any(ex.lower() in store_name.lower() for ex in EXCLUDE_STORES):
            continue
        pname = item.get("productName", item.get("parentProductName", ""))
        if any(ex.lower() in pname.lower() for ex in EXCLUDE_PRODUCTS):
            continue
        qty = item.get("quantity", 0) or 0
        if qty <= 0:
            continue
        uc = (item.get("costInMinorUnits", 0) or 0) / 100
        up = (item.get("priceInMinorUnits", item.get("preTaxPriceInPennies", 0)) or 0) / 100
        sc = (store_name.replace("Thrive ", "").split(" - ")[0]
              if " - RD" in store_name else store_name.replace("Thrive ", ""))
        sup = (item.get("supplierName") or item.get("vendorName") or
               item.get("supplier") or item.get("vendor") or "")
        vid = item.get("variantId", "")
        if sup and vid:
            sup_map[vid] = sup
            sup_found += 1
        elif vid in sup_map:
            sup = sup_map[vid]
        inventory.append({
            "s": sc, "vid": vid, "p": pname,
            "cat": refine_category(item.get("category", item.get("customCategoryName", "")),
                                   item.get("type", ""), pname),
            "b": item.get("brand", ""), "sup": sup,
            "typ": item.get("type", ""), "str": item.get("strainName", "") or "",
            "oh": qty, "uc": round(uc, 2), "up": round(up, 2),
            "ic": round(qty * uc, 2), "ir": round(qty * up, 2),
        })

    # Apply COGS overrides to inventory
    overrides_applied = 0
    for inv_item in inventory:
        override = get_cogs_override(inv_item["b"], inv_item["cat"], inv_item["p"])
        if override is not None:
            inv_item["uc"] = override
            inv_item["ic"] = round(inv_item["oh"] * override, 2)
            overrides_applied += 1

    dt_fetch = time.monotonic() - t0
    t1 = time.monotonic()
    redis_set("taps:inventory", inventory, ttl=INVENTORY_CACHE_TTL)
    redis_set("taps:inventory_ts", datetime.now(timezone.utc).isoformat(), ttl=INVENTORY_CACHE_TTL)
    redis_set("taps:supplier_map", sup_map, ttl=SUPPLIER_CACHE_TTL)
    dt_cache = (time.monotonic() - t1) * 1000
    log.info(f"Inventory: {len(inventory)} items, {sum(i['oh'] for i in inventory):,}u, "
             f"${sum(i['ic'] for i in inventory):,.0f} "
             f"[fetch={dt_fetch:.1f}s cache={dt_cache:.0f}ms suppliers={len(sup_map)} "
             f"cogs_overrides={overrides_applied}]")
    return inventory


# ─── SALES: SINGLE STORE ────────────────────────────────────────────────────

def pull_store_sales(loc: dict, start_date: str, end_date: str,
                     cursor_after: Optional[str] = None) -> tuple:
    """Returns (store_clean, items, last_created_at)."""
    name = loc["_name"]
    store_clean = loc["_clean"]
    loc_id = loc.get("_iid") or loc.get("_id")
    effective_start = cursor_after or start_date

    data = fh_get(f"/v1/orders/findByLocationId/{loc_id}",
                  params={"created_after": effective_start, "created_before": end_date,
                          "page_size": 1, "page": 1})
    if not data:
        return store_clean, [], ""
    total = data.get("total", 0)
    if total == 0:
        return store_clean, [], cursor_after or ""

    log.info(f"  {store_clean}: {total:,} orders (since {effective_start})")
    items = []
    page = 1
    pulled = 0
    last_created = cursor_after or ""

    while pulled < total:
        data = fh_get(f"/v1/orders/findByLocationId/{loc_id}", params={
            "created_after": effective_start, "created_before": end_date,
            "page_size": PAGE_SIZE, "page": page, "order_by": "asc",
        })
        if not data:
            break
        orders = data.get("orders", [])
        if not orders:
            break
        for order in orders:
            status = (order.get("orderStatus") or "sold").lower()
            if status in ("cancelled", "voided") or order.get("voided"):
                continue
            created = order.get("createdAt") or order.get("created_at") or ""
            odate = (created or order.get("completedAt", ""))[:10]
            if created and created > last_created:
                last_created = created
            for it in order.get("itemsInCart", []):
                tp = it.get("totalPrice", 0) or 0
                td_val = it.get("totalDiscounts", 0) or 0
                items.append({
                    "s": store_clean, "vid": it.get("variantId", ""),
                    "q": it.get("quantity", 0) or 0,
                    "tp": round(tp, 2), "td": round(td_val, 2),
                    "nr": round(tp - td_val, 2),
                    "tc": round(it.get("totalCost", 0) or 0, 2),
                    "dt": odate,
                })
        pulled += len(orders)
        log.info(f"    {store_clean}: p{page} ({pulled:,}/{total:,})")
        if len(orders) < PAGE_SIZE:
            break
        page += 1
        time.sleep(RATE_LIMIT_S)

    return store_clean, items, last_created


# ─── SALES AGGREGATION ──────────────────────────────────────────────────────

def aggregate_sales(raw_items: list, days: int = DAYS_DEFAULT) -> tuple:
    """Returns (aggregated_list, store_totals_dict)."""
    t0 = time.monotonic()
    weeks = days / 7
    today = datetime.now(timezone.utc).date()
    w1_start = today - timedelta(days=7)
    w2_start = today - timedelta(days=14)
    w3_start = today - timedelta(days=21)
    w4_start = today - timedelta(days=28)

    agg = {}
    store_totals = {}
    for item in raw_items:
        key = (item["s"], item["vid"])
        if key not in agg:
            agg[key] = {"s": item["s"], "vid": item["vid"],
                        "q": 0, "tp": 0, "td": 0, "nr": 0, "tc": 0,
                        "w1": 0, "w2": 0, "w3": 0, "w4": 0}
        a = agg[key]
        a["q"] += item["q"]; a["tp"] += item["tp"]; a["td"] += item["td"]
        a["nr"] += item["nr"]; a["tc"] += item["tc"]
        dt = item.get("dt", "")
        if dt:
            try:
                d = datetime.strptime(dt, "%Y-%m-%d").date()
                if d >= w1_start:    a["w1"] += item["q"]
                elif d >= w2_start:  a["w2"] += item["q"]
                elif d >= w3_start:  a["w3"] += item["q"]
                elif d >= w4_start:  a["w4"] += item["q"]
            except ValueError:
                pass
        sn = item["s"]
        if sn not in store_totals:
            store_totals[sn] = {"nr": 0, "tp": 0, "td": 0, "tc": 0, "q": 0}
        store_totals[sn]["nr"] += item["nr"]; store_totals[sn]["tp"] += item["tp"]
        store_totals[sn]["td"] += item["td"]; store_totals[sn]["tc"] += item["tc"]
        store_totals[sn]["q"] += item["q"]

    result = []
    for d in agg.values():
        d["wv"] = d["q"] / weeks
        result.append(d)

    dt_agg = (time.monotonic() - t0) * 1000
    log.info(f"Aggregation: {len(raw_items):,} raw → {len(result):,} entries [{dt_agg:.0f}ms]")
    return result, store_totals


# ─── CURSOR MANAGEMENT ──────────────────────────────────────────────────────

def get_cursor(loc_id: str) -> Optional[str]:
    return redis_get(f"taps:cursor:{loc_id}")

def set_cursor(loc_id: str, value: str):
    redis_set(f"taps:cursor:{loc_id}", value, ttl=CURSOR_TTL)


# ─── FULL SALES PULL (bounded concurrency) ──────────────────────────────────

def pull_sales_all(days: int = DAYS_DEFAULT, incremental: bool = True) -> tuple:
    """Pull sales across all locations with MAX_FETCH_WORKERS concurrency."""
    t0 = time.monotonic()
    locations = get_locations()
    if not locations:
        log.error("No locations for sales pull")
        return [], {}

    end_date = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
    start_date = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")

    def pull_one(loc):
        loc_id = loc.get("_iid") or loc.get("_id")
        cursor = get_cursor(loc_id) if incremental else None

        use_cursor = None
        if cursor and incremental:
            try:
                cursor_date = cursor[:10]
                cursor_dt = datetime.strptime(cursor_date, "%Y-%m-%d").date()
                age_days = (datetime.now(timezone.utc).date() - cursor_dt).days
                if age_days <= 3:
                    use_cursor = cursor
                    log.info(f"  {loc['_clean']}: cursor {age_days}d old → incremental")
                else:
                    log.info(f"  {loc['_clean']}: cursor {age_days}d old → full window")
            except (ValueError, TypeError):
                pass

        store_clean, items, last_created = pull_store_sales(
            loc, start_date, end_date, cursor_after=use_cursor)
        if last_created:
            set_cursor(loc_id, last_created)
        return store_clean, items

    # Track progress in Redis
    total_locs = len(locations)
    if rdb:
        rdb.set("taps:rebuild:progress", json.dumps({
            "phase": "sales", "done": 0, "total": total_locs,
            "stores_done": [], "started": time.time(),
        }), ex=600)

    all_items = []
    done_count = 0
    with ThreadPoolExecutor(max_workers=MAX_FETCH_WORKERS) as executor:
        futures = {executor.submit(pull_one, loc): loc for loc in locations}
        for future in as_completed(futures):
            try:
                store_clean, items = future.result()
                all_items.extend(items)
                done_count += 1
                log.info(f"  ✓ {store_clean}: {len(items):,} line items ({done_count}/{total_locs})")
                # Update progress
                if rdb:
                    prog = redis_get("taps:rebuild:progress") or {}
                    stores_done = prog.get("stores_done", [])
                    stores_done.append(store_clean)
                    rdb.set("taps:rebuild:progress", json.dumps({
                        "phase": "sales", "done": done_count, "total": total_locs,
                        "stores_done": stores_done, "started": prog.get("started", time.time()),
                        "elapsed": round(time.time() - prog.get("started", time.time())),
                    }), ex=600)
            except Exception as e:
                loc = futures[future]
                done_count += 1
                log.error(f"  ✗ {loc.get('_clean', '?')}: {e}")

    dt_fetch = time.monotonic() - t0
    log.info(f"Sales fetch: {len(all_items):,} items, {len(locations)} locations "
             f"[{dt_fetch:.1f}s, workers={MAX_FETCH_WORKERS}]")

    if not all_items:
        existing = redis_get("taps:sales")
        existing_st = redis_get("taps:sales_store_totals")
        if existing:
            log.info("No new items — returning existing cached sales")
            return existing, existing_st or {}
        return [], {}

    sales_agg, store_totals = aggregate_sales(all_items, days)
    return sales_agg, store_totals


# ─── TAPS ENGINE (pure computation) ─────────────────────────────────────────

def run_taps(inventory: list, sales: list, store_totals: dict,
             wos_target: float = WOS_DEFAULT, days: int = DAYS_DEFAULT) -> dict:
    t0 = time.monotonic()
    if not inventory:
        return {"st": {}, "ss": [], "pd": []}

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

    sales_map = {(e["s"], e["vid"]): e for e in sales} if sales else {}
    vid_vels = {}
    if sales:
        vv = {}
        for e in sales:
            vv.setdefault(e["vid"], []).append(e["wv"])
        vid_vels = {vid: sum(vels) / len(vels) for vid, vels in vv.items()}
    stores_with_sales = set(e["s"] for e in sales) if sales else set()

    products = []
    cogs_overrides_applied = 0
    for (store, vid), inv in inv_map.items():
        sd = sales_map.get((store, vid))
        sold = sd["q"] if sd else 0
        vel = sd["wv"] if sd else 0
        nr = sd["nr"] if sd else 0
        tc_sold = sd["tc"] if sd else 0
        if sold == 0 and store not in stores_with_sales and vid in vid_vels:
            vel = vid_vels[vid]
            sold = vel * weeks

        # Apply COGS override: recalculate tc_sold using corrected unit cost
        override_uc = get_cogs_override(inv.get("b", ""), inv.get("cat", ""), inv.get("p", ""))
        if override_uc is not None and sold > 0:
            tc_sold = round(sold * override_uc, 2)
            cogs_overrides_applied += 1

        w1 = sd.get("w1", 0) if sd else 0
        w2 = sd.get("w2", 0) if sd else 0
        w3 = sd.get("w3", 0) if sd else 0
        w4 = sd.get("w4", 0) if sd else 0
        prior_avg = (w2 + w3 + w4) / 3 if (w2 + w3 + w4) > 0 else 0
        trend_pct = round((w1 - prior_avg) / prior_avg * 100, 0) if prior_avg > 0 else (100 if w1 > 0 else 0)

        wos_val = inv["oh"] / vel if vel > 0 else None
        par = max(round(vel * wos_target), 0) if vel > 0 else 0
        oq = max(par - inv["oh"], 0)
        mgn = round((nr - tc_sold) / nr * 100, 1) if nr > 0 else 0
        cls = "A" if vel >= 20 else "B" if vel >= 10 else "C" if vel >= 3 else "D"

        products.append({
            "s": store, "p": inv["p"][:55], "b": (inv["b"] or "")[:20],
            "cat": inv["cat"], "cls": cls, "wv": round(vel, 2), "oh": inv["oh"],
            "wos": round(wos_val, 1) if wos_val and wos_val < 999 else None,
            "par": par, "oq": oq, "nr": round(nr, 2), "cogs": round(tc_sold, 2),
            "mgn": mgn, "ic": round(inv["ic"], 2), "uc": inv["uc"], "up": inv["up"],
            "sup": (inv["sup"] or "")[:30],
            "w1": w1, "w2": w2, "w3": w3, "w4": w4, "tr": int(trend_pct),
            "vid": vid,
        })

    # Recalculate store totals using overridden COGS
    store_totals_adj = {}
    for p in products:
        sn = p["s"]
        if sn not in store_totals_adj:
            st_orig = store_totals.get(sn, {})
            store_totals_adj[sn] = {"nr": st_orig.get("nr", 0), "tp": st_orig.get("tp", 0),
                                     "td": st_orig.get("td", 0), "tc": 0, "q": st_orig.get("q", 0)}
        store_totals_adj[sn]["tc"] += p["cogs"]

    tnr = sum(s.get("nr", 0) for s in store_totals_adj.values())
    ttp = sum(s.get("tp", 0) for s in store_totals_adj.values())
    ttd = sum(s.get("td", 0) for s in store_totals_adj.values())
    ttc = sum(s.get("tc", 0) for s in store_totals_adj.values())
    tq = sum(s.get("q", 0) for s in store_totals_adj.values())
    inv_ts = redis_get("taps:inventory_ts")
    sales_meta = redis_get("taps:sales_meta")

    stats = {
        "period": f"{(datetime.now(timezone.utc) - timedelta(days=days)).strftime('%b %d')} - "
                  f"{(datetime.now(timezone.utc) - timedelta(days=1)).strftime('%b %d %Y')}",
        "source": "Flowhub API (Live)",
        "stores": len(set(p["s"] for p in products)),
        "net_revenue": round(tnr, 2), "gross_sales": round(ttp, 2),
        "discounts": round(ttd, 2), "cogs": round(ttc, 2),
        "gross_profit": round(tnr - ttc, 2),
        "margin": round((tnr - ttc) / tnr * 100, 1) if tnr > 0 else 0,
        "discount_rate": round(ttd / ttp * 100, 1) if ttp > 0 else 0,
        "units_sold": int(tq), "total_products": len(products),
        "total_inv_cost": round(sum(p["ic"] for p in products), 2),
        "total_inv_units": sum(p["oh"] for p in products),
        "inventory_ts": inv_ts,
        "sales_ts": sales_meta.get("ts") if sales_meta else None,
    }

    ss = []
    for sn in sorted(set(p["s"] for p in products)):
        sp = [p for p in products if p["s"] == sn]
        dead = [p for p in sp if p["wv"] == 0]
        over = [p for p in sp if p["wos"] and p["wos"] > 8 and p["wv"] > 0]
        st = store_totals_adj.get(sn, {})
        ic = sum(p["ic"] for p in sp)
        ss.append({
            "s": sn, "rev": round(st.get("nr", 0)), "cogs": round(st.get("tc", 0)),
            "margin": round((st.get("nr", 0) - st.get("tc", 0)) / st["nr"] * 100, 1) if st.get("nr", 0) > 0 else 0,
            "disc": round(st.get("td", 0)), "units": int(st.get("q", 0)),
            "products": len(sp), "inv_cost": round(ic), "inv_units": sum(p["oh"] for p in sp),
            "dead_cost": round(sum(p["ic"] for p in dead)),
            "dead_pct": round(sum(p["ic"] for p in dead) / ic * 100, 1) if ic > 0 else 0,
            "over_cost": round(sum(p["ic"] for p in over)),
        })

    result = {"st": stats, "ss": ss, "pd": products}
    dt = (time.monotonic() - t0) * 1000
    log.info(f"TAPS engine: {len(products)} products, {len(ss)} stores, "
             f"{cogs_overrides_applied} COGS overrides [{dt:.0f}ms]")
    return result


# ─── REBUILD ORCHESTRATOR ────────────────────────────────────────────────────

def do_rebuild(days: int = DAYS_DEFAULT, incremental: bool = True):
    t0 = time.monotonic()
    log.info(f"=== REBUILD START (days={days}, incremental={incremental}) ===")

    # Phase 1: Inventory
    if rdb:
        rdb.set("taps:rebuild:progress", json.dumps({
            "phase": "inventory", "done": 0, "total": 0,
            "stores_done": [], "started": time.time(), "elapsed": 0,
        }), ex=600)

    inventory = pull_inventory()
    if not inventory:
        log.error("Rebuild aborted: no inventory")
        _clear_progress()
        return False

    # Phase 2: Sales (progress tracked inside pull_sales_all)
    sales_agg, store_totals = pull_sales_all(days=days, incremental=incremental)
    if not sales_agg:
        log.warning("Rebuild: no sales data")

    t_p = time.monotonic()
    ts = datetime.now(timezone.utc).isoformat()

    # Phase 3: Finalizing
    if rdb:
        rdb.set("taps:rebuild:progress", json.dumps({
            "phase": "finalizing", "done": 0, "total": 0,
            "stores_done": [], "started": time.time(), "elapsed": round(time.time() - t0),
        }), ex=600)

    redis_set("taps:sales", sales_agg, ttl=SALES_CACHE_TTL)
    redis_set("taps:sales_store_totals", store_totals, ttl=SALES_CACHE_TTL)
    redis_set("taps:sales_meta", {"ts": ts, "last_date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                                   "count": len(sales_agg)}, ttl=SALES_CACHE_TTL)
    dt_p = (time.monotonic() - t_p) * 1000
    log.info(f"Raw data persisted [{dt_p:.0f}ms]")

    result = run_taps(inventory, sales_agg, store_totals, WOS_DEFAULT, days)
    redis_set("taps:dashboard", result, ttl=TAPS_CACHE_TTL)

    dt_total = time.monotonic() - t0
    log.info(f"=== REBUILD COMPLETE [{dt_total:.1f}s total] ===")
    _clear_progress()
    return True


def _clear_progress():
    if rdb:
        try:
            rdb.delete("taps:rebuild:progress")
        except Exception:
            pass


def _bg_rebuild_locked(days=DAYS_DEFAULT, incremental=True):
    with rebuild_lock(REBUILD_LOCK_TTL) as acquired:
        if not acquired:
            return
        try:
            do_rebuild(days=days, incremental=incremental)
        except Exception as e:
            log.error(f"Rebuild failed: {e}", exc_info=True)


def _bg_rebuild_safe(days=DAYS_DEFAULT, incremental=True):
    try:
        do_rebuild(days=days, incremental=incremental)
    except Exception as e:
        log.error(f"Background rebuild failed: {e}", exc_info=True)


# ─── ENDPOINTS ───────────────────────────────────────────────────────────────

@app.get("/")
def root():
    return {"app": "TAPS", "version": "3.0", "status": "ok",
            "redis": redis_ok(), "has_creds": bool(os.environ.get("FLOWHUB_CLIENT_ID"))}


@app.get("/health")
def health():
    rc = redis_ok()
    info = {}
    if rc:
        try:
            mem = rdb.info("memory")
            info["used_memory_human"] = mem.get("used_memory_human", "?")
            for key in ["taps:dashboard", "taps:sales", "taps:inventory"]:
                info[f"ttl:{key}"] = rdb.ttl(key)
        except Exception:
            pass
    return {"status": "healthy" if rc else "degraded", "redis_connected": rc,
            "redis": info, "timestamp": datetime.now(timezone.utc).isoformat()}


@app.get("/api/taps")
def get_taps(wos: float = WOS_DEFAULT, days: int = DAYS_DEFAULT,
             refresh_inventory: bool = False, background_tasks: BackgroundTasks = None):
    """Stale-while-revalidate: Redis-only reads. Background rebuild on miss."""
    cached = redis_get("taps:dashboard")
    if cached:
        if wos != WOS_DEFAULT:
            inventory = redis_get("taps:inventory")
            sales = redis_get("taps:sales")
            store_totals = redis_get("taps:sales_store_totals")
            if inventory and sales is not None:
                return run_taps(inventory, sales, store_totals or {}, wos, days)
        return cached

    inventory = redis_get("taps:inventory")
    sales = redis_get("taps:sales")
    store_totals = redis_get("taps:sales_store_totals")
    if inventory and sales:
        result = run_taps(inventory, sales, store_totals or {}, wos, days)
        redis_set("taps:dashboard", result, ttl=TAPS_CACHE_TTL)
        return result

    if background_tasks:
        background_tasks.add_task(_bg_rebuild_safe, days)
    return {"status": "warming",
            "message": "Cache empty. Background rebuild triggered. Retry in 2-3 minutes.",
            "st": {}, "ss": [], "pd": []}


@app.get("/api/taps/cached")
def get_taps_cached():
    cached = redis_get("taps:dashboard")
    if cached:
        return cached
    raise HTTPException(404, "No cached data. Trigger /internal/rebuild-cache")


@app.get("/api/status")
def status():
    meta = redis_get("taps:sales_meta")
    inv_ts = redis_get("taps:inventory_ts")
    dash_ttl = rdb.ttl("taps:dashboard") if rdb else -2
    sales_ttl = rdb.ttl("taps:sales") if rdb else -2
    return {"redis": redis_ok(), "inventory_ts": inv_ts,
            "sales_ts": meta.get("ts") if meta else None,
            "sales_count": meta.get("count", 0) if meta else 0,
            "dashboard_ttl": dash_ttl, "sales_ttl": sales_ttl,
            "lock_held": bool(rdb.get(LOCK_KEY)) if rdb else False}


@app.get("/api/inventory")
def get_inventory(background_tasks: BackgroundTasks):
    cached = redis_get("taps:inventory")
    if cached:
        return {"items": len(cached), "units": sum(i["oh"] for i in cached),
                "cost": round(sum(i["ic"] for i in cached), 2),
                "ts": redis_get("taps:inventory_ts")}
    background_tasks.add_task(pull_inventory)
    return {"status": "warming", "message": "Inventory pull triggered in background."}


@app.get("/api/sales-status")
def sales_status():
    meta = redis_get("taps:sales_meta")
    lock_held = bool(rdb.get(LOCK_KEY)) if rdb else False
    progress = redis_get("taps:rebuild:progress")
    return {"ts": meta.get("ts") if meta else None,
            "items": meta.get("count", 0) if meta else 0,
            "last_date": meta.get("last_date") if meta else None,
            "rebuild_running": lock_held or progress is not None,
            "progress": progress}


@app.post("/api/refresh-sales")
def refresh_sales(background_tasks: BackgroundTasks, days: int = DAYS_DEFAULT):
    background_tasks.add_task(_bg_rebuild_safe, days, False)
    return {"status": "started_full", "days": days}


@app.post("/api/refresh-sales-quick")
def refresh_sales_quick(background_tasks: BackgroundTasks):
    background_tasks.add_task(_bg_rebuild_safe, DAYS_DEFAULT, True)
    return {"status": "started_incremental"}


@app.post("/internal/rebuild-cache")
def rebuild_cache(background_tasks: BackgroundTasks,
                  days: int = DAYS_DEFAULT, incremental: bool = True):
    """Cron endpoint. Railway cron: curl -X POST https://app/internal/rebuild-cache"""
    if not redis_ok():
        raise HTTPException(503, "Redis not available")
    if rdb.get(LOCK_KEY):
        return {"status": "skipped", "reason": "rebuild already in progress"}
    background_tasks.add_task(_bg_rebuild_locked, days, incremental)
    return {"status": "started", "days": days, "incremental": incremental}


@app.get("/api/debug-categories")
def debug_categories():
    inv = redis_get("taps:inventory")
    if not inv:
        return {"error": "No inventory cached. Run rebuild first."}
    combos = {}
    for item in inv:
        key = f"{item['cat']} | {item.get('typ', '')}"
        if key not in combos:
            combos[key] = {"count": 0, "examples": []}
        combos[key]["count"] += item["oh"]
        if len(combos[key]["examples"]) < 3:
            combos[key]["examples"].append(item["p"][:50])
    return dict(sorted(combos.items(), key=lambda x: -x[1]["count"]))


@app.get("/api/debug-brand/{brand}")
def debug_brand(brand: str):
    """Show all products for a brand with their cat, type, uc, and COGS override status."""
    inv = redis_get("taps:inventory")
    if not inv:
        return {"error": "No inventory cached"}
    brand_l = brand.lower()
    matches = []
    for item in inv:
        if brand_l in (item.get("b") or "").lower():
            override = get_cogs_override(item.get("b", ""), item.get("cat", ""), item.get("p", ""))
            matches.append({
                "store": item["s"], "product": item["p"], "brand": item.get("b"),
                "cat": item["cat"], "type": item.get("typ"), "qty": item["oh"],
                "uc_flowhub": item["uc"], "uc_override": override,
                "uc_active": override if override else item["uc"],
            })
    matches.sort(key=lambda x: (-x["qty"]))
    return {"brand": brand, "products": len(matches), "items": matches}


@app.get("/api/debug-cursors")
def debug_cursors():
    locations = get_locations()
    cursors = {}
    for loc in locations:
        loc_id = loc.get("_iid") or loc.get("_id")
        cursor = get_cursor(loc_id)
        age = None
        if cursor:
            try:
                cdt = datetime.fromisoformat(cursor.replace("Z", "+00:00"))
                age = f"{(datetime.now(timezone.utc) - cdt).total_seconds() / 3600:.1f}h"
            except (ValueError, TypeError):
                age = cursor[:10] if cursor else None
        cursors[loc["_clean"]] = {"loc_id": loc_id, "cursor": cursor, "age": age}
    return cursors


# ─── STARTUP ─────────────────────────────────────────────────────────────────

@app.on_event("startup")
async def startup():
    log.info("=" * 60)
    log.info("TAPS API v3.0 — Redis-first, non-blocking architecture")
    log.info("=" * 60)
    client = connect_redis()
    if client:
        for key in ["taps:dashboard", "taps:sales", "taps:inventory"]:
            ttl = client.ttl(key)
            exists = "✓" if ttl >= 0 else "✗"
            log.info(f"  cache {key}: {exists} (ttl={ttl}s)")
    else:
        log.warning("Running without Redis — all data ephemeral!")
    log.info("Startup complete — no Flowhub calls.")
    log.info("POST /internal/rebuild-cache to populate.")
    log.info("=" * 60)
