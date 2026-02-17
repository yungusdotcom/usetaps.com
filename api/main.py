"""
TAPS API — Backend Server
FastAPI service that connects to Flowhub, caches data, and runs TAPS engine.
"""

import os
import json
import time
import logging
from datetime import datetime, timedelta
from typing import Optional

import requests
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# ─── CONFIG ───────────────────────────────────────────────────────────────────
FLOWHUB_BASE = "https://api.flowhub.co"
FLOWHUB_CLIENT_ID = os.environ.get("FLOWHUB_CLIENT_ID", "")
FLOWHUB_API_KEY = os.environ.get("FLOWHUB_API_KEY", "")
TAPS_AUTH_TOKEN = os.environ.get("TAPS_AUTH_TOKEN", "taps-default-token")

PAGE_SIZE = 500
RATE_LIMIT = 0.3
SALES_CACHE_HOURS = 12
DAYS_DEFAULT = 31
WOS_DEFAULT = 2.5

EXCLUDE_PRODUCTS = ['EXIT BAG']
EXCLUDE_STORES = ['MBNV', 'Smoke & Mirrors', 'Cultivation']
CANNABIS_CATS = ['FLOWER', 'Concentrates', 'Edibles', 'Infused Flower', 'Capsules',
                 'CBD', 'Topicals', 'Tinctures']

# ─── LOGGING ──────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("taps")

# ─── APP ──────────────────────────────────────────────────────────────────────
app = FastAPI(title="TAPS API", version="1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Lock down to runtaps.com in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─── IN-MEMORY CACHE ─────────────────────────────────────────────────────────
cache = {
    "locations": None,
    "inventory": None,
    "inventory_ts": None,
    "sales": None,
    "sales_ts": None,
    "sales_pulling": False,
    "taps": None,
    "taps_ts": None,
}

# ─── FLOWHUB API HELPERS ─────────────────────────────────────────────────────
def fh_headers():
    cid = os.environ.get("FLOWHUB_CLIENT_ID", "")
    key = os.environ.get("FLOWHUB_API_KEY", "")
    if not cid or not key:
        raise HTTPException(500, "Flowhub credentials not configured")
    return {"clientId": cid, "key": key}

def fh_get(path, params=None, timeout=90):
    url = f"{FLOWHUB_BASE}{path}"
    for attempt in range(3):
        try:
            r = requests.get(url, headers=fh_headers(), params=params, timeout=timeout)
            if r.status_code == 200:
                return r.json()
            log.warning(f"{path} returned {r.status_code}")
            if r.status_code == 500 and attempt < 2:
                time.sleep(2 ** attempt)
                continue
            return None
        except requests.exceptions.Timeout:
            log.warning(f"Timeout attempt {attempt+1} for {path}")
            if attempt < 2:
                time.sleep(2 ** attempt)
        except Exception as e:
            log.error(f"Error: {e}")
            return None
    return None

# ─── LOCATIONS ────────────────────────────────────────────────────────────────
def get_locations():
    if cache["locations"]:
        return cache["locations"]
    data = fh_get("/v0/clientsLocations")
    if not data:
        return []
    locs = data if isinstance(data, list) else data.get("data", data.get("locations", []))
    retail = []
    loc_lookup = {}
    for loc in locs:
        name = loc.get("locationName", loc.get("name", ""))
        if any(ex.lower() in name.lower() for ex in EXCLUDE_STORES):
            continue
        lid = loc.get("locationId", loc.get("importId", ""))
        loc["_name"] = name
        loc["_id"] = lid
        retail.append(loc)
        loc_lookup[lid] = name
        # Also map importId if different
        iid = loc.get("importId", "")
        if iid and iid != lid:
            loc_lookup[iid] = name
    cache["locations"] = retail
    cache["loc_lookup"] = loc_lookup
    log.info(f"Loaded {len(retail)} locations")
    return retail

# ─── INVENTORY (LIVE) ────────────────────────────────────────────────────────
def pull_inventory():
    log.info("Pulling inventory from Flowhub API...")
    locations = get_locations()
    if not locations:
        return None

    loc_lookup = cache.get("loc_lookup", {})
    first_id = locations[0].get("importId", locations[0].get("id", ""))
    data = fh_get("/v0/inventory", params={"locationId": first_id})
    if not data:
        return None

    items = data.get("data", [])
    log.info(f"Raw API items: {len(items)}")

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
        cost_pennies = item.get("costInMinorUnits", 0) or 0
        price_pennies = item.get("priceInMinorUnits", item.get("preTaxPriceInPennies", 0)) or 0
        uc = cost_pennies / 100
        up = price_pennies / 100

        store_clean = store_name.replace("Thrive ", "")
        store_clean = store_clean.split(" - ")[0] if " - RD" in store_clean else store_clean

        inventory.append({
            "s": store_clean,
            "vid": item.get("variantId", ""),
            "p": pname,
            "cat": item.get("category", item.get("customCategoryName", "")),
            "b": item.get("brand", ""),
            "sup": item.get("supplierName", "") or "",
            "typ": item.get("type", ""),
            "str": item.get("strainName", "") or "",
            "oh": qty,
            "uc": round(uc, 2),
            "up": round(up, 2),
            "ic": round(qty * uc, 2),
            "ir": round(qty * up, 2),
        })

    cache["inventory"] = inventory
    cache["inventory_ts"] = datetime.now().isoformat()
    log.info(f"Inventory: {len(inventory)} items, {sum(i['oh'] for i in inventory):,} units, ${sum(i['ic'] for i in inventory):,.2f}")
    return inventory

# ─── SALES (CACHED) ──────────────────────────────────────────────────────────
def pull_sales(days=DAYS_DEFAULT):
    if cache["sales_pulling"]:
        log.info("Sales pull already in progress")
        return cache.get("sales")

    cache["sales_pulling"] = True
    log.info(f"Pulling sales (last {days} days)...")

    try:
        locations = get_locations()
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

        all_items = []
        for loc in locations:
            name = loc["_name"]
            loc_id = loc.get("importId", loc.get("id", ""))

            store_clean = name.replace("Thrive ", "")
            store_clean = store_clean.split(" - ")[0] if " - RD" in store_clean else store_clean

            data = fh_get(f"/v1/orders/findByLocationId/{loc_id}",
                          params={"created_after": start_date, "created_before": end_date,
                                  "page_size": 1, "page": 1})
            if not data:
                continue
            total = data.get("total", 0)
            log.info(f"  {name}: {total:,} orders")
            if total == 0:
                continue

            page = 1
            pulled = 0
            while pulled < total:
                data = fh_get(f"/v1/orders/findByLocationId/{loc_id}",
                              params={"created_after": start_date, "created_before": end_date,
                                      "page_size": PAGE_SIZE, "page": page, "order_by": "asc"})
                if not data:
                    break
                orders = data.get("orders", [])
                if not orders:
                    break

                for order in orders:
                    status = order.get("orderStatus", "sold")
                    if status.lower() in ("cancelled", "voided") or order.get("voided"):
                        continue
                    for item in order.get("itemsInCart", []):
                        tp = item.get("totalPrice", 0) or 0
                        td = item.get("totalDiscounts", 0) or 0
                        all_items.append({
                            "s": store_clean,
                            "vid": item.get("variantId", ""),
                            "q": item.get("quantity", 0) or 0,
                            "tp": round(tp, 2),
                            "td": round(td, 2),
                            "nr": round(tp - td, 2),
                            "tc": round(item.get("totalCost", 0) or 0, 2),
                        })

                pulled += len(orders)
                if len(orders) < PAGE_SIZE:
                    break
                page += 1
                time.sleep(RATE_LIMIT)
            time.sleep(RATE_LIMIT)

        cache["sales"] = all_items
        cache["sales_ts"] = datetime.now().isoformat()
        log.info(f"Sales: {len(all_items):,} line items")
        return all_items
    finally:
        cache["sales_pulling"] = False

# ─── TAPS ENGINE ──────────────────────────────────────────────────────────────
def run_taps(wos_target=WOS_DEFAULT, days=DAYS_DEFAULT):
    inventory = cache.get("inventory") or pull_inventory()
    sales = cache.get("sales")

    if not inventory:
        raise HTTPException(500, "No inventory data")

    weeks = days / 7

    # Aggregate inventory by store + variant
    inv_map = {}
    for item in inventory:
        key = (item["s"], item["vid"])
        if key not in inv_map:
            inv_map[key] = {**item}
        else:
            inv_map[key]["oh"] += item["oh"]
            inv_map[key]["ic"] += item["ic"]
            inv_map[key]["ir"] += item["ir"]

    # Aggregate sales by store + variant
    sales_map = {}
    sales_store_totals = {}
    if sales:
        for item in sales:
            key = (item["s"], item["vid"])
            if key not in sales_map:
                sales_map[key] = {"q": 0, "tp": 0, "td": 0, "nr": 0, "tc": 0}
            sales_map[key]["q"] += item["q"]
            sales_map[key]["tp"] += item["tp"]
            sales_map[key]["td"] += item["td"]
            sales_map[key]["nr"] += item["nr"]
            sales_map[key]["tc"] += item["tc"]

            if item["s"] not in sales_store_totals:
                sales_store_totals[item["s"]] = {"nr": 0, "tp": 0, "td": 0, "tc": 0, "q": 0}
            sales_store_totals[item["s"]]["nr"] += item["nr"]
            sales_store_totals[item["s"]]["tp"] += item["tp"]
            sales_store_totals[item["s"]]["td"] += item["td"]
            sales_store_totals[item["s"]]["tc"] += item["tc"]
            sales_store_totals[item["s"]]["q"] += item["q"]

    # Cross-store velocity for stores without sales
    vid_velocities = {}
    if sales:
        vid_sales = {}
        for (s, vid), sd in sales_map.items():
            if vid not in vid_sales:
                vid_sales[vid] = []
            vid_sales[vid].append(sd["q"] / weeks)
        for vid, vels in vid_sales.items():
            vid_velocities[vid] = sum(vels) / len(vels)

    stores_with_sales = set(s for (s, _) in sales_map.keys())

    # Build product rows
    products = []
    for (store, vid), inv in inv_map.items():
        sd = sales_map.get((store, vid), {})
        sold = sd.get("q", 0)
        vel = sold / weeks if sold > 0 else 0

        # Cross-store estimate for stores without sales
        if sold == 0 and store not in stores_with_sales and vid in vid_velocities:
            vel = vid_velocities[vid]
            sold = vel * weeks

        nr = sd.get("nr", 0)
        tc_sold = sd.get("tc", 0)
        wos = inv["oh"] / vel if vel > 0 else None
        par = max(round(vel * wos_target), 0) if vel > 0 else 0
        oq = max(par - inv["oh"], 0)
        mgn = round((nr - tc_sold) / nr * 100, 1) if nr > 0 else 0

        cls = "A" if vel >= 20 else "B" if vel >= 10 else "C" if vel >= 3 else "D"

        products.append({
            "s": store, "p": inv["p"][:55], "b": (inv["b"] or "")[:20],
            "cat": inv["cat"], "cls": cls,
            "wv": round(vel, 2), "oh": inv["oh"],
            "wos": round(wos, 1) if wos and wos < 999 else None,
            "par": par, "oq": oq,
            "nr": round(nr, 2), "cogs": round(tc_sold, 2),
            "mgn": mgn, "ic": round(inv["ic"], 2),
            "uc": inv["uc"], "up": inv["up"],
            "sup": (inv["sup"] or "")[:30],
        })

    # Stats
    total_nr = sum(s["nr"] for s in sales_store_totals.values()) if sales_store_totals else 0
    total_tp = sum(s["tp"] for s in sales_store_totals.values()) if sales_store_totals else 0
    total_td = sum(s["td"] for s in sales_store_totals.values()) if sales_store_totals else 0
    total_tc = sum(s["tc"] for s in sales_store_totals.values()) if sales_store_totals else 0
    total_q = sum(s["q"] for s in sales_store_totals.values()) if sales_store_totals else 0

    stats = {
        "period": f"{(datetime.now()-timedelta(days=days)).strftime('%b %d')} - {datetime.now().strftime('%b %d %Y')}",
        "source": "Flowhub API (Live)",
        "stores": len(set(p["s"] for p in products)),
        "net_revenue": round(total_nr, 2),
        "gross_sales": round(total_tp, 2),
        "discounts": round(total_td, 2),
        "cogs": round(total_tc, 2),
        "gross_profit": round(total_nr - total_tc, 2),
        "margin": round((total_nr - total_tc) / total_nr * 100, 1) if total_nr > 0 else 0,
        "discount_rate": round(total_td / total_tp * 100, 1) if total_tp > 0 else 0,
        "units_sold": int(total_q),
        "total_products": len(products),
        "total_inv_cost": round(sum(p["ic"] for p in products), 2),
        "total_inv_units": sum(p["oh"] for p in products),
        "inventory_ts": cache.get("inventory_ts"),
        "sales_ts": cache.get("sales_ts"),
    }

    # Store stats
    store_names = sorted(set(p["s"] for p in products))
    store_stats = []
    for sn in store_names:
        sp = [p for p in products if p["s"] == sn]
        dead = [p for p in sp if p["wv"] == 0]
        over = [p for p in sp if p["wos"] and p["wos"] > 8 and p["wv"] > 0]
        st = sales_store_totals.get(sn, {})
        rev = st.get("nr", 0)
        cs = st.get("tc", 0)
        ds = st.get("td", 0)
        store_stats.append({
            "s": sn, "rev": round(rev), "cogs": round(cs),
            "margin": round((rev - cs) / rev * 100, 1) if rev > 0 else 0,
            "disc": round(ds),
            "units": int(st.get("q", 0)),
            "products": len(sp),
            "inv_cost": round(sum(p["ic"] for p in sp)),
            "inv_units": sum(p["oh"] for p in sp),
            "dead_cost": round(sum(p["ic"] for p in dead)),
            "dead_pct": round(sum(p["ic"] for p in dead) / sum(p["ic"] for p in sp) * 100, 1) if sum(p["ic"] for p in sp) > 0 else 0,
            "over_cost": round(sum(p["ic"] for p in over)),
        })

    result = {"st": stats, "ss": store_stats, "pd": products}
    cache["taps"] = result
    cache["taps_ts"] = datetime.now().isoformat()
    return result

# ─── ENDPOINTS ────────────────────────────────────────────────────────────────

@app.get("/")
def root():
    return {"status": "ok", "app": "TAPS", "version": "1.0"}

@app.get("/api/debug-env")
def debug_env():
    return {"has_cid": bool(os.environ.get("FLOWHUB_CLIENT_ID")), "has_key": bool(os.environ.get("FLOWHUB_API_KEY")), "env_keys": [k for k in os.environ.keys() if "FLOW" in k.upper()]}

@app.get("/api/status")
def status():
    return {
        "inventory_ts": cache.get("inventory_ts"),
        "sales_ts": cache.get("sales_ts"),
        "sales_pulling": cache.get("sales_pulling", False),
        "products": len(cache["taps"]["pd"]) if cache.get("taps") else 0,
        "locations": len(cache.get("locations") or []),
    }

@app.get("/api/inventory")
def get_inventory():
    inv = pull_inventory()
    if not inv:
        raise HTTPException(500, "Failed to pull inventory")
    return {
        "items": len(inv),
        "units": sum(i["oh"] for i in inv),
        "cost": round(sum(i["ic"] for i in inv), 2),
        "ts": cache["inventory_ts"],
    }

@app.post("/api/refresh-sales")
def refresh_sales(background_tasks: BackgroundTasks, days: int = DAYS_DEFAULT):
    if cache.get("sales_pulling"):
        return {"status": "already_pulling", "sales_ts": cache.get("sales_ts")}
    background_tasks.add_task(pull_sales, days)
    return {"status": "started", "days": days}

@app.get("/api/sales-status")
def sales_status():
    return {
        "pulling": cache.get("sales_pulling", False),
        "ts": cache.get("sales_ts"),
        "items": len(cache.get("sales") or []),
    }

@app.get("/api/taps")
def get_taps(wos: float = WOS_DEFAULT, days: int = DAYS_DEFAULT, refresh_inventory: bool = False):
    if refresh_inventory or not cache.get("inventory"):
        pull_inventory()
    if not cache.get("sales"):
        # No sales cached, try to pull
        pull_sales(days)
    result = run_taps(wos, days)
    return result

@app.get("/api/taps/cached")
def get_taps_cached():
    if cache.get("taps"):
        return cache["taps"]
    raise HTTPException(404, "No cached TAPS data. Call /api/taps first.")

@app.on_event("startup")
async def startup():
    log.info("TAPS API starting...")
    log.info("Ready. Inventory and sales will load on first request.")
