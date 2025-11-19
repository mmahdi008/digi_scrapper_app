import streamlit as st
import requests
import time
import pandas as pd
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
from collections import deque

# Page config
st.set_page_config(
    page_title="Digikala Product Scraper",
    page_icon="🛍️",
    layout="wide"
)

# ========== CONFIG ==========
SLEEP_BETWEEN = 0.25
TIMEOUT = 15  # Reduced timeout for Streamlit Cloud
RETRIES = 2  # Reduced retries to fail faster

# ScrapingBee API Key - Put your API key here
SCRAPINGBEE_API_KEY = "DOFLGDGT60O6F63AS517BUX1IZ07LD2ZW5GKXYMLFQIV9VOE633RC5CRHF627Z77M8ESW5U6MYX4FUFI"  # Replace with your actual ScrapingBee API key
# ============================

# Helper functions
def plp_to_api(plp_url):
    """
    Convert a Digikala PLP URL (site-facing) to the API search URL pattern.
    Handles six patterns:
    1. Category URLs: https://www.digikala.com/search/category-mobile-phone/
       -> https://api.digikala.com/v1/categories/mobile-phone/search/?sort=7&page=1
    2. Search query URLs: https://www.digikala.com/search/?q=query
       -> https://api.digikala.com/v1/search/?q=query&page=1
    3. Facet URLs: https://www.digikala.com/search/facet/category-mobile-phone/up-to-29000000/
       -> https://api.digikala.com/v1/facet/search/category-mobile-phone/up-to-29000000/?facetURL[0]=category-mobile-phone&facetURL[1]=up-to-29000000&page=1
    4. Tag URLs: https://www.digikala.com/tags/spongebob/
       -> https://api.digikala.com/v1/tags/spongebob/?page=1
    5. Category-brand URLs: https://www.digikala.com/search/category-cell-phone-pouch-cover/abnabat-rangi/
       -> https://api.digikala.com/v1/categories/cell-phone-pouch-cover/brands/abnabat-rangi/search/?page=1
    6. Brand URLs: https://www.digikala.com/brand/abnabat-rangi/
       -> https://api.digikala.com/v1/brands/abnabat-rangi/?page=1
    """
    p = urlparse(plp_url)
    qs = parse_qs(p.query)
    path = p.path.strip("/")
    parts = path.split("/")
    
    # Check if this is a search query URL (has 'q' parameter)
    if "q" in qs and qs["q"]:
        query = qs["q"][0]
        # Build search API URL with the query parameter (properly URL-encoded)
        api = f"https://api.digikala.com/v1/search/?{urlencode({'q': query, 'page': '1'})}"
        return api
    
    # Check if this is a facet URL (contains /search/facet/)
    if "search" in parts and "facet" in parts:
        facet_idx = parts.index("facet")
        if facet_idx + 1 < len(parts):
            # Extract all segments after "facet"
            facet_segments = [part for part in parts[facet_idx + 1:] if part]
            if facet_segments:
                # Build facetURL parameters as array
                facet_params = {"sort": "7", "page": "1"}
                for i, segment in enumerate(facet_segments):
                    facet_params[f"facetURL[{i}]"] = segment
                
                # Build the full API URL
                api = f"https://api.digikala.com/v1/facet/search/{'/'.join(facet_segments)}/?{urlencode(facet_params)}"
                return api
    
    # Check if this is a tag URL (starts with /tags/)
    if parts and parts[0] == "tags" and len(parts) > 1:
        tag_slug = parts[1]
        # Ensure slug contains only allowed characters (letters, digits, hyphen)
        tag_slug = "".join(ch for ch in tag_slug if ch.isalnum() or ch == "-")
        api = f"https://api.digikala.com/v1/tags/{tag_slug}/?page=1"
        return api
    
    # Check if this is a brand URL (starts with /brand/)
    if parts and parts[0] == "brand" and len(parts) > 1:
        brand_slug = parts[1]
        # Ensure slug contains only allowed characters (letters, digits, hyphen)
        brand_slug = "".join(ch for ch in brand_slug if ch.isalnum() or ch == "-")
        api = f"https://api.digikala.com/v1/brands/{brand_slug}/?page=1"
        return api
    
    # Check if this is a category-brand URL (search/category-{slug}/{brand-slug}/)
    if "search" in parts:
        search_idx = parts.index("search")
        if search_idx + 1 < len(parts):
            category_part = parts[search_idx + 1]
            if category_part.startswith("category-") and search_idx + 2 < len(parts):
                category_slug = category_part.replace("category-", "")
                brand_slug = parts[search_idx + 2]
                # Sanitize both slugs
                category_slug = "".join(ch for ch in category_slug if ch.isalnum() or ch == "-")
                brand_slug = "".join(ch for ch in brand_slug if ch.isalnum() or ch == "-")
                api = f"https://api.digikala.com/v1/categories/{category_slug}/brands/{brand_slug}/search/?page=1"
                return api
    
    # Otherwise, treat as category URL
    # common patterns: "search/category-<slug>" or "category-<slug>" or "search/<slug>"
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

def _get_json(url, use_scrapingbee=False, scrapingbee_api_key=None):
    """
    Fetch JSON data from URL.
    If use_scrapingbee=True and api_key provided, uses ScrapingBee proxy service.
    """
    if use_scrapingbee and scrapingbee_api_key:
        # Use ScrapingBee API
        scrapingbee_url = "https://app.scrapingbee.com/api/v1/"
        
        params = {
            'api_key': scrapingbee_api_key,
            'url': url,
            'render_js': 'false',  # Set to 'true' if you need JavaScript rendering
            # Note: 'premium_proxy' requires a paid plan. Remove if you have a free plan.
            # 'premium_proxy': 'true',  # Uncomment if you have a premium plan
            # 'country_code': 'us'  # Optional: specify country
        }
        
        headers = {"Accept": "application/json"}
        last_err = None
        
        print(f"Fetching URL via ScrapingBee: {url}")  # Debug log
        for attempt in range(RETRIES):
            try:
                print(f"Attempt {attempt + 1}/{RETRIES} (via ScrapingBee)")  # Debug log
                r = requests.get(scrapingbee_url, params=params, headers=headers, timeout=TIMEOUT)
                print(f"Response status: {r.status_code}")  # Debug log
                
                if r.status_code == 200:
                    # ScrapingBee returns the scraped content in response.text
                    # The content is the actual response from the target URL
                    try:
                        # Try to parse as JSON directly
                        return r.json()
                    except ValueError:
                        # If not JSON, the response might be text/HTML
                        # Try to parse the text content as JSON
                        try:
                            import json
                            return json.loads(r.text)
                        except:
                            # If still not JSON, return None and let the caller handle it
                            print(f"⚠️ ScrapingBee returned non-JSON content. Content type: {r.headers.get('content-type', 'unknown')}")
                            print(f"First 200 chars: {r.text[:200]}")
                            raise RuntimeError("ScrapingBee returned non-JSON content. The target URL might not be returning JSON.")
                elif r.status_code == 403:
                    # Try to get more details from the response
                    try:
                        error_details = r.json() if r.text else {}
                        error_msg = error_details.get('message', r.text[:200] if r.text else 'No details')
                        last_err = f"HTTP 403 - ScrapingBee Error: {error_msg}. Possible causes: Invalid API key, quota exceeded, or premium features used without premium plan."
                    except:
                        last_err = f"HTTP 403 - ScrapingBee API key may be invalid, quota exceeded, or you're using premium features without a premium plan. Response: {r.text[:200] if r.text else 'No response'}"
                    print(f"⚠️ ScrapingBee Error: {last_err}")  # Debug log
                    print(f"Full response: {r.text[:500]}")  # Debug log
                else:
                    last_err = f"HTTP {r.status_code}"
                    print(f"⚠️ ScrapingBee Error: {last_err}")  # Debug log
            except requests.exceptions.Timeout as e:
                last_err = f"Timeout: {str(e)}"
                print(f"Timeout error: {e}")  # Debug log
            except requests.exceptions.RequestException as e:
                last_err = f"Request error: {str(e)}"
                print(f"Request error: {e}")  # Debug log
            except Exception as e:
                last_err = str(e)
                print(f"Unexpected error: {e}")  # Debug log
            if attempt < RETRIES - 1:
                time.sleep(0.7)
        error_msg = f"Failed to fetch {url} via ScrapingBee. {last_err}"
        print(f"ERROR: {error_msg}")  # Debug log
        raise RuntimeError(error_msg)
    
    else:
        # Original direct method
        headers = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
        last_err = None
        print(f"Fetching URL: {url}")  # Debug log
        for attempt in range(RETRIES):
            try:
                print(f"Attempt {attempt + 1}/{RETRIES}")  # Debug log
                r = requests.get(url, headers=headers, timeout=TIMEOUT)
                print(f"Response status: {r.status_code}")  # Debug log
                if r.status_code == 200:
                    print(f"Successfully fetched data")  # Debug log
                    return r.json()
                last_err = f"HTTP {r.status_code}"
            except requests.exceptions.Timeout as e:
                last_err = f"Timeout: {str(e)}"
                print(f"Timeout error: {e}")  # Debug log
            except requests.exceptions.RequestException as e:
                last_err = f"Request error: {str(e)}"
                print(f"Request error: {e}")  # Debug log
            except Exception as e:
                last_err = str(e)
                print(f"Unexpected error: {e}")  # Debug log
            if attempt < RETRIES - 1:
                time.sleep(0.7)
        error_msg = f"Failed to fetch {url}. {last_err}"
        print(f"ERROR: {error_msg}")  # Debug log
        raise RuntimeError(error_msg)

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
        "rating.rate": _safe_get(prod, "rating", "rate"),
        "rating.count": _safe_get(prod, "rating", "count"),
        "selling_price": _extract_selling_price(prod),
        "rrp_price": _extract_rrp_price(prod),
        "is_promotion": _extract_is_promotion(prod),
        "discount_percent": _extract_discount_percent(prod),
    }

def scrape_from_plp(plp_url, target_count, progress_bar, status_text, use_scrapingbee=False, scrapingbee_api_key=None):
    print(f"Starting scrape for URL: {plp_url}, target: {target_count}")  # Debug log
    if use_scrapingbee:
        print(f"Using ScrapingBee proxy")  # Debug log
    api_pattern = plp_to_api(plp_url)
    print(f"API pattern: {api_pattern}")  # Debug log
    all_rows = []
    try:
        page = int(parse_qs(urlparse(api_pattern).query).get("page", ["1"])[0])
    except:
        page = 1
    collected, seen_ids = 0, set()
    start_url = api_pattern
    consecutive_empty_pages = 0
    max_pages = 100  # Safety limit to prevent infinite loops
    
    print(f"Starting from page {page}")  # Debug log
    
    while collected < target_count and page <= max_pages:
        page_url = _update_url_page(start_url, page)
        print(f"Processing page {page}: {page_url}")  # Debug log
        status_text.markdown(f"**📄 Fetching page {page}...** ({collected}/{target_count} products collected)")
        
        try:
            data = _get_json(page_url, use_scrapingbee=use_scrapingbee, scrapingbee_api_key=scrapingbee_api_key)
            print(f"Got data, looking for products...")  # Debug log
            
            # Check pagination metadata if available
            pager = _safe_get(data, "data", "pager")
            if pager:
                current_page = _safe_get(pager, "current_page", default=page)
                total_pages = _safe_get(pager, "total_pages")
                total_items = _safe_get(pager, "total_items", default=0)
                
                if total_pages and current_page > total_pages:
                    status_text.markdown(f"**⚠️ No more pages available.** Total pages: {total_pages}")
                    break
                
                if total_items == 0:
                    status_text.markdown("**⚠️ No products found in this category/search.**")
                    break
            
            products = _find_products(data)
            print(f"Found {len(products) if products else 0} products on page {page}")  # Debug log
            
            if not products:
                consecutive_empty_pages += 1
                if consecutive_empty_pages >= 2:
                    status_text.markdown("**⚠️ No products found on multiple consecutive pages. Stopping.**")
                    break
                status_text.markdown(f"**📄 No products on page {page}.** Trying next page...")
                page += 1
                time.sleep(SLEEP_BETWEEN)
                continue
            
            # Reset consecutive empty pages counter if we found products
            consecutive_empty_pages = 0
            
            # Process products
            products_added_this_page = 0
            for p in products:
                pid = p.get("id")
                if pid in seen_ids:
                    continue
                try:
                    all_rows.append(_row(p))
                    seen_ids.add(pid)
                    collected += 1
                    products_added_this_page += 1
                    if collected >= target_count:
                        break
                except Exception as e:
                    # Log error but continue processing other products
                    print(f"Warning: Error processing product {pid}: {e}")
                    continue
            
            print(f"Page {page}: Added {products_added_this_page} products (Total: {collected})")  # Debug log
            status_text.markdown(f"**✅ Page {page}:** Added {products_added_this_page} products (Total: {collected})")
            
            # Update progress
            progress = min(collected / target_count, 1.0)
            progress_bar.progress(progress)
            
            if collected >= target_count:
                break
            
            # Check if we should continue paginating
            if products_added_this_page == 0:
                status_text.markdown("**⚠️ No new products added. Stopping pagination.**")
                break
                
            page += 1
            time.sleep(SLEEP_BETWEEN)
            
        except RuntimeError as e:
            if page == 1:
                # If first page fails, raise the error
                status_text.markdown(f"**❌ Error fetching first page:** {str(e)}")
                raise
            # Otherwise, stop pagination
            status_text.markdown(f"**⚠️ Error on page {page}:** {str(e)}. Stopping.")
            break
        except Exception as e:
            if page == 1:
                status_text.markdown(f"**❌ Unexpected error on first page:** {str(e)}")
                raise
            status_text.markdown(f"**⚠️ Unexpected error on page {page}:** {str(e)}. Stopping.")
            break
    
    if page > max_pages:
        status_text.markdown(f"**⚠️ Reached maximum page limit ({max_pages}). Stopping.**")
    
    print(f"Scraping complete. Collected {len(all_rows)} products total.")  # Debug log
    
    # Create DataFrame with proper handling for empty results
    if not all_rows:
        status_text.markdown("**⚠️ No products were collected.**")
        print("WARNING: No products collected")  # Debug log
        # Create empty DataFrame with correct columns
        df = pd.DataFrame(columns=[
            "uri", "title_fa", "id", "brand", "category",
            "item_category2", "item_category3", "item_category4", "item_category5",
            "rating.rate", "rating.count", "selling_price",
            "rrp_price", "is_promotion", "discount_percent"
        ])
    else:
        df = pd.DataFrame(all_rows)
        cols = [
            "uri", "title_fa", "id", "brand", "category",
            "item_category2", "item_category3", "item_category4", "item_category5",
            "rating.rate", "rating.count", "selling_price",
            "rrp_price", "is_promotion", "discount_percent"
        ]
        df = df.reindex(columns=cols)
        print(f"Created DataFrame with {len(df)} rows and {len(df.columns)} columns")  # Debug log
    
    return df

# Main UI
st.title("🛍️ Digikala Product Scraper")
st.markdown("---")

# Sidebar for input
with st.sidebar:
    st.header("⚙️ Settings")
    plp_url = st.text_input(
        "Digikala URL",
        placeholder="https://www.digikala.com/search/category-mobile-phone/",
        help="Enter a category URL or search query URL"
    )
    target_count = st.number_input(
        "Target Product Count",
        min_value=1,
        max_value=10000,
        value=50,
        step=10,
        help="Number of products to scrape"
    )
    test_mode = st.checkbox("🧪 Test Mode (scrape only 10 products)", value=False, help="Use this to test if scraping works")
    
    st.markdown("---")
    st.subheader("🔄 Proxy Settings")
    use_scrapingbee = st.checkbox("Use ScrapingBee Proxy", value=False, help="Enable to use ScrapingBee proxy service (helps avoid IP blocking)")
    
    scrapingbee_api_key = None
    if use_scrapingbee:
        # Priority order: 1) Hardcoded key, 2) Secrets, 3) Manual input
        if SCRAPINGBEE_API_KEY and SCRAPINGBEE_API_KEY != "YOUR_API_KEY_HERE":
            scrapingbee_api_key = SCRAPINGBEE_API_KEY
            st.success("✅ Using ScrapingBee API key from config")
        elif 'scrapingbee' in st.secrets and 'api_key' in st.secrets.scrapingbee:
            scrapingbee_api_key = st.secrets.scrapingbee.api_key
            st.success("✅ Using ScrapingBee API key from secrets")
        else:
            # Allow manual input
            scrapingbee_api_key = st.text_input(
                "ScrapingBee API Key",
                type="password",
                placeholder="Enter your ScrapingBee API key",
                help="Get your API key from https://www.scrapingbee.com/"
            )
            if not scrapingbee_api_key:
                st.warning("⚠️ Please enter your ScrapingBee API key to use proxy")
    
    scrape_button = st.button("🚀 Start Scraping", type="primary", width='stretch')

# Initialize session state
if 'scraping' not in st.session_state:
    st.session_state.scraping = False
if 'scrape_results' not in st.session_state:
    st.session_state.scrape_results = None
if 'scrape_error' not in st.session_state:
    st.session_state.scrape_error = None

# Main content area
if scrape_button and not st.session_state.scraping:
    if not plp_url:
        st.error("❌ Please enter a Digikala URL")
    else:
        # Reset previous results
        st.session_state.scrape_results = None
        st.session_state.scrape_error = None
        st.session_state.scraping = True
        
        try:
            # Show API pattern
            api_pattern = plp_to_api(plp_url)
            st.info(f"🔗 API Pattern: `{api_pattern}`")
            
            # Show proxy status
            if use_scrapingbee and scrapingbee_api_key:
                st.success("🔄 Using ScrapingBee Proxy")
            else:
                st.info("🌐 Using Direct Connection")
            
            # Use test mode if enabled
            actual_target = 10 if test_mode else target_count
            if test_mode:
                st.warning("🧪 Test Mode: Scraping only 10 products")
            
            # Progress tracking
            progress_placeholder = st.empty()
            status_placeholder = st.empty()
            
            # Show initial message
            status_placeholder.info("🚀 Starting scraping... Please wait, this may take a minute or two.")
            
            try:
                # Create progress bar and status
                progress_bar = progress_placeholder.progress(0)
                status_text = status_placeholder.empty()
                
                # Scrape
                df = scrape_from_plp(
                    plp_url, 
                    actual_target, 
                    progress_bar, 
                    status_text,
                    use_scrapingbee=use_scrapingbee if use_scrapingbee and scrapingbee_api_key else False,
                    scrapingbee_api_key=scrapingbee_api_key if use_scrapingbee and scrapingbee_api_key else None
                )
                
                # Store results in session state
                st.session_state.scrape_results = df
                st.session_state.scraping = False
                
                # Clear placeholders
                progress_placeholder.empty()
                status_placeholder.empty()
                
                # Rerun to display results from session state
                st.rerun()
                
            except Exception as scrape_error:
                st.session_state.scrape_error = str(scrape_error)
                st.session_state.scraping = False
                progress_placeholder.empty()
                status_placeholder.empty()
                # Don't rerun here, let the outer exception handler display the error
                
        except Exception as e:
            st.session_state.scrape_error = str(e)
            st.session_state.scraping = False
            progress_placeholder.empty()
            status_placeholder.empty()

# Display results if available
if st.session_state.scrape_results is not None:
    df = st.session_state.scrape_results
    
    if len(df) > 0:
        st.success(f"✅ Successfully scraped {len(df)} products!")
        st.markdown("---")
        
        # Display results
        st.subheader("📊 Results")
        st.dataframe(df, width='stretch', height=400)
        
        # Download button - CSV is only generated when button is clicked
        def generate_csv():
            return df.to_csv(index=False, encoding="utf-8-sig")
        
        csv_data = generate_csv()
        st.download_button(
            label="📥 Download CSV (Click to download)",
            data=csv_data,
            file_name=f"digikala_products_{len(df)}.csv",
            mime="text/csv",
            width='stretch',
            key=f"download_session_{len(df)}",
            help="Click this button to download the results as CSV"
        )
        
        # Statistics
        st.markdown("---")
        st.subheader("📈 Statistics")
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Products", len(df))
        with col2:
            st.metric("Unique Brands", df["brand"].nunique())
        with col3:
            avg_price = df["selling_price"].mean()
            st.metric("Avg Price", f"{avg_price:,.0f}" if pd.notna(avg_price) else "N/A")
        with col4:
            promotions = df["is_promotion"].sum()
            st.metric("Promotions", promotions)
    else:
        st.warning("⚠️ No products were found. Please check the URL and try again.")
    
    # Reset button
    if st.button("🔄 Scrape Again", width='stretch', key="scrape_again_btn"):
        st.session_state.scrape_results = None
        st.session_state.scrape_error = None
        st.rerun()

elif st.session_state.scrape_error:
    st.error(f"❌ Error occurred: {st.session_state.scrape_error}")
    if st.button("🔄 Try Again", width='stretch', key="try_again_btn"):
        st.session_state.scrape_error = None
        st.session_state.scraping = False
        st.rerun()
else:
    # Instructions
    st.info("""
    ### 📖 How to use:
    1. Enter a Digikala URL in the sidebar
       - Category URL: `https://www.digikala.com/search/category-mobile-phone/`
       - Search URL: `https://www.digikala.com/search/?q=شال قرمز`
       - Facet URL: `https://www.digikala.com/search/facet/category-mobile-phone/up-to-29000000/`
       - Tag URL: `https://www.digikala.com/tags/spongebob/`
       - Category-Brand URL: `https://www.digikala.com/search/category-cell-phone-pouch-cover/abnabat-rangi/`
       - Brand URL: `https://www.digikala.com/brand/abnabat-rangi/`
    2. Set the target number of products
    3. Click "Start Scraping"
    4. View results and download as CSV
    """)
    
    # Example URLs
    with st.expander("📝 Example URLs"):
        st.code("""
Category URL:
https://www.digikala.com/search/category-mobile-phone/

Search Query URL:
https://www.digikala.com/search/?q=شال قرمز

Facet URL (with filters):
https://www.digikala.com/search/facet/category-mobile-phone/up-to-29000000/

Tag URL:
https://www.digikala.com/tags/spongebob/

Category-Brand URL:
https://www.digikala.com/search/category-cell-phone-pouch-cover/abnabat-rangi/

Brand URL:
https://www.digikala.com/brand/abnabat-rangi/
        """)