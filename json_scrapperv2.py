# %%
# Cell 1: imports and config
import requests, time, math
import pandas as pd
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
from collections import deque
import numpy as np

# ========== CONFIG ==========
# The user will input a PLP URL (site-facing). Leave URLS empty; it will be populated later.
URLS = []
TARGET_COUNT = 2500  # get top N per URL
SLEEP_BETWEEN = 0.25  # polite delay between page calls
TIMEOUT = 20  # request timeout
RETRIES = 3  # retries per request
OUT_CSV = "test2.csv"
FULL_URLS = False
BASE_HOST = "https://www.digikala.com"
# ============================


# %%
# Cell 2: helper functions (URL conversion, safe access, HTTP)
def plp_to_api(plp_url):
    """
    Convert a Digikala PLP URL (site-facing) to the API search URL pattern.
    Example:
      https://www.digikala.com/search/category-mobile-phone/
    -> https://api.digikala.com/v1/categories/mobile-phone/search/?sort=7&page=1
    This function is conservative: it tries to extract the category slug from the path.
    """
    p = urlparse(plp_url)
    path = p.path.strip("/")
    # common patterns: "search/category-<slug>" or "category-<slug>" or "search/<slug>"
    parts = path.split("/")
    slug = None
    # look for a part that starts with "category-"
    for part in parts:
        if part.startswith("category-"):
            slug = part.replace("category-", "")
            break
    # fallback: if path contains "search" and next segment exists, take it
    if not slug:
        if "search" in parts:
            idx = parts.index("search")
            if idx + 1 < len(parts):
                # remove possible "category-" prefix
                slug = parts[idx + 1].replace("category-", "")
    # final fallback: take last non-empty segment and sanitize
    if not slug and parts:
        slug = parts[-1].replace("category-", "")
    if not slug:
        raise ValueError("Could not determine category slug from PLP URL.")
    # ensure slug contains only allowed characters (letters, digits, hyphen)
    slug = "".join(ch for ch in slug if ch.isalnum() or ch == "-")
    api = f"https://api.digikala.com/v1/categories/{slug}/search/?sort=7&page=1"
    return api

def _update_url_page(url, new_page):
    parts = urlparse(url)
    qs = parse_qs(parts.query)
    qs["page"] = [str(new_page)]
    new_query = urlencode({k: v[0] if len(v) == 1 else v for k, v in qs.items()}, doseq=True)
    return urlunparse((parts.scheme, parts.netloc, parts.path, parts.params, new_query, parts.fragment))

def _safe_get(d, *path, default=None):
    cur = d
    for p in path:
        if isinstance(cur, dict) and p in cur:
            cur = cur[p]
        elif isinstance(cur, list) and isinstance(p, int) and 0 <= p < len(cur):
            cur = cur[p]
        else:
            return default
    return cur

def _get_json(url):
    headers = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
    last_err = None
    for _ in range(RETRIES):
        try:
            r = requests.get(url, headers=headers, timeout=TIMEOUT)
            if r.status_code == 200:
                return r.json()
            last_err = f"HTTP {r.status_code}"
        except Exception as e:
            last_err = str(e)
        time.sleep(0.7)
    raise RuntimeError(f"Failed to fetch {url}. {last_err}")


# %%
# Cell 3: product-finding and deep helpers
def _find_products(payload):
    prods = _safe_get(payload, "data", "products")
    if isinstance(prods, list) and prods:
        return prods
    q = deque([payload])
    while q:
        node = q.popleft()
        if isinstance(node, dict):
            for v in node.values():
                q.append(v)
        elif isinstance(node, list) and node and isinstance(node[0], dict):
            if "id" in node[0] and any(k in node[0] for k in ("title_fa","title","name","product_title_fa")):
                return node
    return []

def _deep_find_first(prod, candidate_keys, want_type=(str, int, float, dict)):
    seen = set()
    q = deque([prod])
    while q:
        node = q.popleft()
        if id(node) in seen:
            continue
        seen.add(id(node))
        if isinstance(node, dict):
            for k, v in node.items():
                if k in candidate_keys and isinstance(v, want_type):
                    return v
                if isinstance(v, (dict, list)):
                    q.append(v)
        elif isinstance(node, list):
            for v in node:
                if isinstance(v, (dict, list)):
                    q.append(v)
    return None


# %%
# Cell 4: extraction helpers
def _extract_brand(prod):
    v = prod.get("brand")
    if isinstance(v, str) and v.strip():
        return v.strip()
    if isinstance(v, dict):
        for kk in ("title_fa","title_en","title","name_fa","name"):
            vv = v.get(kk)
            if isinstance(vv, str) and vv.strip():
                return vv.strip()
    val = _deep_find_first(prod, {"brand","brand_title","brand_name","brand_fa","brand_en"})
    if isinstance(val, str) and val.strip():
        return val.strip()
    if isinstance(val, dict):
        for kk in ("title_fa","title_en","title","name_fa","name"):
            vv = val.get(kk)
            if isinstance(vv, str) and vv.strip():
                return vv.strip()
    return ""

def _extract_category(prod):
    v = prod.get("category")
    if isinstance(v, str) and v.strip():
        return v.strip()
    code = None; name_fa = None
    if isinstance(v, dict):
        code = v.get("code")
        name_fa = v.get("title_fa") or v.get("title")
    if not (code and name_fa):
        cats = prod.get("categories")
        if isinstance(cats, list) and cats:
            c0 = cats[0]
            if isinstance(c0, dict):
                code = code or c0.get("code")
                name_fa = name_fa or c0.get("title_fa") or c0.get("title")
    if code or name_fa:
        return f"[{code or ''},{(name_fa or '').strip()}]"
    val = _deep_find_first(prod, {"category","category_title","cat_title_fa","cat_name"})
    if isinstance(val, str) and val.strip():
        return val.strip()
    return ""

def _extract_uri(prod):
    pid = prod.get("id")
    if pid and str(pid).strip():
        clean_id = str(pid).strip()
        return f"https://www.digikala.com/product/dkp-{clean_id}"
    return ""

def _extract_selling_price(prod):
    for path in [
            ("price","selling_price"),
            ("default_variant","price","selling_price"),
            ("variants",0,"price","selling_price"),
            ("summary","price","selling_price"),
        ]:
        val = _safe_get(prod, *path)
        if val is not None:
            return val
    return _safe_get(prod, "min_price")

def _extract_item_categories(prod):
    result = {"item_category2": "", "item_category3": "", "item_category4": "", "item_category5": ""}
    if not isinstance(prod, dict):
        return result
    for key in result.keys():
        if isinstance(prod.get(key), str):
            result[key] = prod[key]
    if all(result.values()):
        return result
    q = deque([prod])
    seen = set()
    while q:
        node = q.popleft()
        if id(node) in seen:
            continue
        seen.add(id(node))
        if isinstance(node, dict):
            for k, v in node.items():
                if k in result and not result[k] and isinstance(v, str):
                    result[k] = v
                if isinstance(v, (dict, list)):
                    q.append(v)
        elif isinstance(node, list):
            for v in node:
                if isinstance(v, (dict, list)):
                    q.append(v)
    return result


# %%
# Cell 5: new fields (rrp, is_promotion, discount_percent)
def _extract_rrp_price(prod):
    for path in [
        ("price", "rrp_price"),
        ("default_variant", "price", "rrp_price"),
        ("summary", "price", "rrp_price"),
        ("price_history", "min_last_30d"),
    ]:
        val = _safe_get(prod, *path)
        if val is not None:
            return val
    return None

def _extract_is_promotion(prod):
    if prod.get("is_incredible") is True:
        return True
    if prod.get("is_promotion") is True:
        return True
    if _safe_get(prod, "price", "is_promotion") is True:
        return True
    if _safe_get(prod, "default_variant", "price", "is_promotion") is True:
        return True
    badges = _safe_get(prod, "badges") or []
    if isinstance(badges, list):
        for b in badges:
            title = _safe_get(b, "title") or ""
            if "شگفت" in title or "تخفیف" in title or "incredible" in title.lower():
                return True
    return False

def _extract_discount_percent(prod):
    for path in [
        ("price", "discount_percent"),
        ("default_variant", "price", "discount_percent"),
        ("summary", "price", "discount_percent"),
    ]:
        val = _safe_get(prod, *path)
        if val is not None and val > 0:
            return val
    original = None
    selling = _extract_selling_price(prod)
    if selling is None:
        return None
    for path in [
        ("price", "rrp_price"),
        ("price", "original_price"),
        ("default_variant", "price", "rrp_price"),
        ("default_variant", "price", "original_price"),
        ("summary", "price", "rrp_price"),
    ]:
        val = _safe_get(prod, *path)
        if val is not None and val > selling:
            original = val
            break
    if original and original > selling:
        discount = ((original - selling) / original) * 100
        return round(discount, 1)
    return None


# %%
# Cell 6: row builder and main scraping loop (converts PLP to API)
def _row(prod):
    cats = _extract_item_categories(prod)
    return {
        "uri": _extract_uri(prod),
        "title_fa": prod.get("title_fa") or prod.get("title") or "",
        "id": prod.get("id"),
        "brand": _extract_brand(prod),
        "category": _extract_category(prod),
        "item_category2": cats["item_category2"],
        "item_category3": cats["item_category3"],
        "item_category4": cats["item_category4"],
        "item_category5": cats["item_category5"],
        "test_title_fa": prod.get("test_title_fa") or "",
        "rating.rate": _safe_get(prod, "rating", "rate"),
        "rating.count": _safe_get(prod, "rating", "count"),
        "selling_price": _extract_selling_price(prod),
        "rrp_price": _extract_rrp_price(prod),
        "is_promotion": _extract_is_promotion(prod),
        "discount_percent": _extract_discount_percent(prod),
    }

def scrape_from_plp(plp_url, target_count=TARGET_COUNT):
    api_pattern = plp_to_api(plp_url)
    print("Using API pattern:", api_pattern)
    all_rows = []
    try:
        page = int(parse_qs(urlparse(api_pattern).query).get("page", ["1"])[0])
    except:
        page = 1
    collected, seen_ids = 0, set()
    start_url = api_pattern
    while collected < target_count:
        page_url = _update_url_page(start_url, page)
        data = _get_json(page_url)
        products = _find_products(data)
        if not products:
            break
        for p in products:
            pid = p.get("id")
            if pid in seen_ids:
                continue
            all_rows.append(_row(p))
            seen_ids.add(pid)
            collected += 1
            if collected >= target_count:
                break
        page += 1
        time.sleep(SLEEP_BETWEEN)
    df = pd.DataFrame(all_rows)
    cols = [
        "uri", "title_fa", "id", "brand", "category",
        "item_category2", "item_category3", "item_category4", "item_category5",
        "test_title_fa", "rating.rate", "rating.count", "selling_price",
        "rrp_price", "is_promotion", "discount_percent"
    ]
    df = df.reindex(columns=cols)
    df.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
    print(f"Saved {len(df)} rows to {OUT_CSV}")
    return df


# %%
# Interactive usage: paste PLP URL at runtime
plp = input("Paste Digikala PLP URL (e.g. https://www.digikala.com/search/category-mobile-phone/): ").strip()
if not plp:
    raise SystemExit("No URL provided.")
api_pattern = plp_to_api(plp)
print("Derived API pattern:", api_pattern)
df = scrape_from_plp(plp, TARGET_COUNT)
display(df.head(12).loc[:, ["uri","brand","category","item_category3","title_fa",
                           "selling_price","rrp_price","is_promotion","discount_percent"]])




