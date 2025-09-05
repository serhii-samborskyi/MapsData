from __future__ import annotations

# === BEGIN: user's scraper (Playwright + DOM/XPath), emails disabled ===

import asyncio
import re
import unicodedata
from playwright.async_api import async_playwright
import logging
import random
from datetime import datetime
from lxml import html
from urllib.parse import urlparse
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Utility functions
def safeString(value):
    if value is None or value == "":
        return ""
    return str(value).replace('"', '""')

def cleanUrl(url):
    if not url or not isinstance(url, str):
        return ""
    url = re.sub(r'^"|"$', '', url).strip()
    if url == "":
        return ""
    if not url.startswith("http://") and not url.startswith("https://"):
        url = "https://" + url
    try:
        return urlparse(url).geturl()
    except ValueError:
        logger.warning(f"Invalid URL format: {url}")
        return ""

def getDomain(url):
    tlds = {
        'ac', 'ad', 'ae', 'af', 'ag', 'ai', 'al', 'am', 'an', 'ao', 'aq', 'ar', 'as', 'at', 'au',
        'aw', 'ax', 'az', 'ba', 'bb', 'bd', 'be', 'bf', 'bg', 'bh', 'bi', 'bj', 'bm', 'bn', 'bo',
        'br', 'bs', 'bt', 'bv', 'bw', 'by', 'bz', 'ca', 'cc', 'cd', 'cf', 'cg', 'ch', 'ci', 'ck',
        'cl', 'cm', 'cn', 'co', 'cr', 'cu', 'cv', 'cw', 'cx', 'cy', 'cz', 'de', 'dj', 'dk', 'dm',
        'do', 'dz', 'ec', 'ee', 'eg', 'eh', 'er', 'es', 'et', 'eu', 'fi', 'fj', 'fk', 'fm', 'fo',
        'fr', 'ga', 'gb', 'gd', 'ge', 'gf', 'gg', 'gh', 'gi', 'gl', 'gm', 'gn', 'gp', 'gq', 'gr',
        'gs', 'gt', 'gu', 'gw', 'gy', 'hk', 'hm', 'hn', 'hr', 'ht', 'hu', 'id', 'ie', 'il', 'im',
        'in', 'io', 'iq', 'ir', 'is', 'it', 'je', 'jm', 'jo', 'jp', 'ke', 'kg', 'kh', 'ki', 'km',
        'kn', 'kp', 'kr', 'kw', 'ky', 'kz', 'la', 'lb', 'lc', 'li', 'lk', 'lr', 'ls', 'lt', 'lu',
        'lv', 'ly', 'ma', 'mc', 'md', 'me', 'mf', 'mg', 'mh', 'mk', 'ml', 'mm', 'mn', 'mo', 'mp',
        'mq', 'mr', 'ms', 'mt', 'mu', 'mv', 'mw', 'mx', 'my', 'mz', 'na', 'nc', 'ne', 'nf', 'ng',
        'ni', 'nl', 'no', 'np', 'nr', 'nu', 'nz', 'om', 'pa', 'pe', 'pf', 'pg', 'ph', 'pk', 'pl',
        'pm', 'pn', 'pr', 'ps', 'pt', 'pw', 'py', 'qa', 're', 'ro', 'rs', 'ru', 'rw', 'sa', 'sb',
        'sc', 'sd', 'se', 'sg', 'sh', 'si', 'sj', 'sk', 'sl', 'sm', 'sn', 'so', 'sr', 'ss', 'st',
        'su', 'sv', 'sx', 'sy', 'sz', 'tc', 'td', 'tf', 'tg', 'th', 'tj', 'tk', 'tl', 'tm', 'tn',
        'to', 'tr', 'tt', 'tv', 'tw', 'tz', 'ua', 'ug', 'uk', 'us', 'uy', 'uz', 'va', 'vc', 've',
        'vg', 'vi', 'vn', 'vu', 'wf', 'ws', 'xk', 'ye', 'yt', 'za', 'zm', 'zw',
    }
    try:
        urlObj = urlparse(url)
        hostParts = urlObj.hostname.lower().split('.')
        return (tlds & set(hostParts)).pop() if tlds & set(hostParts) else hostParts[-2]
    except Exception:
        return ""

def decodeCfEmail(encoded):
    if not encoded or len(encoded) < 2:
        return ""
    key = int(encoded[:2], 16)
    decoded = ""
    for i in range(2, len(encoded), 2):
        charCode = int(encoded[i:i+2], 16) ^ key
        decoded += chr(charCode)
    return decoded

async def extractEmail(url: str, name: str, browser, recursive: bool = True, max_concurrent: int = 5) -> dict:
    logger.setLevel(logging.CRITICAL)
    try:
        url = cleanUrl(url)
        if not url:
            return {"email": set()}

        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
        )
        page = await context.new_page()
        try:
            response = await asyncio.wait_for(page.goto(url, wait_until="domcontentloaded"), timeout=5000)
            content = await page.content() if response and response.status == 200 else ""
        except Exception:
            await context.close()
            return {"email": set()}
        await context.close()

        content = unicodedata.normalize('NFKC', content.encode().decode('utf-8', errors='ignore'))
        result = {"email": set()}
        linkedPages = set()

        # Multiple email patterns to catch different formats
        patterns = [
            r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}(?![^<]*>)",
            r"mailto:([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})",
            r"href=['\"]mailto:([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})['\"]"
        ]

        soup = BeautifulSoup(content, 'html.parser')

        # Search in all text content
        for pattern in patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            for match in matches:
                email = match if isinstance(match, str) else match[0] if match else ""
                if email and not (re.search(r'\{.*\}', email) or len(email.split()) > 1):
                    result["email"].add(email.strip().lower())

        # Also search in specific HTML elements
        for element in soup.find_all(['a', 'span', 'div', 'p', 'td', 'li']):
            text = element.get_text() if element else ""
            href = element.get('href', '') if element.name == 'a' else ""

            for pattern in patterns:
                if text:
                    matches = re.findall(pattern, text, re.IGNORECASE)
                    for match in matches:
                        email = match if isinstance(match, str) else match[0] if match else ""
                        if email:
                            result["email"].add(email.strip().lower())

                if href and href.startswith('mailto:'):
                    email = href.replace('mailto:', '').strip().lower()
                    if '@' in email and '.' in email.split('@')[-1]:
                        result["email"].add(email)

        cfEmail = soup.select_one('.__cf_email__')
        if cfEmail and cfEmail.get('data-cfemail'):
            result["email"].add(decodeCfEmail(cfEmail['data-cfemail']))

        if recursive:
            baseUrl = urlparse(url).geturl()
            links = [a.get('href') for a in soup.find_all('a', href=True)]
            targetPaths = [
                '/contact', '/contact-us', '/contact-me', '/contacts', '/contact-info', '/contact-information',
                '/about', '/about-me', '/about-us', '/team', '/our-team', '/meet-the-team',
                '/support', '/customer-service', '/feedback', '/help', '/sales', '/reach-us',
                'privacy', 'return', 'location', 'policy', 'faq', '/get-in-touch', '/reach-out',
                '/info', '/information', '/details', '/services', '/quote', '/estimate'
            ]
            for link in links:
                try:
                    fullUrl = urlparse(link, baseUrl).geturl()
                    for path in targetPaths:
                        if path in fullUrl:
                            linkedPages.add(fullUrl)
                            break
                except Exception:
                    continue

            if linkedPages:
                async def fetchPage(pageUrl: str) -> dict:
                    return await extractEmail(pageUrl, "", browser, False)
                tasks = [fetchPage(pageUrl) for pageUrl in list(linkedPages)[:max_concurrent]]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for res in results:
                    if isinstance(res, dict):
                        result["email"].update(res["email"])

        allEmails = set()
        domainEmails = set()
        invalidPatterns = [
            '.png', '.jpg', '.jpeg', '.gif', '.webp', 'wixpress.com', 'sentry.io',
            'noreply', 'abuse', 'no-reply', 'subscribe', 'mailer-daemon', 'domain.com',
            'email.com', 'yourname', 'wix.com',
        ]
        domain = getDomain(url)

        for email in result["email"]:
            cleanedEmail = email.replace('u003e', '').replace('&lt;', '').replace('&gt;', '').lower().strip()
            # Skip obviously invalid emails
            if (any(pattern in cleanedEmail for pattern in invalidPatterns) or
                not re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', cleanedEmail) or
                cleanedEmail.count('@') != 1):
                continue
            allEmails.add(cleanedEmail)
            if domain and (domain in cleanedEmail or cleanedEmail.split('@')[1].endswith('.' + domain)):
                domainEmails.add(cleanedEmail)

        result["email"] = domainEmails if domainEmails else allEmails
        return result

    except Exception:
        return {"email": set()}
    finally:
        logger.setLevel(logging.INFO)

async def process_business(business, browser, extract_emails: bool = False) -> dict:
    try:
        name = business.xpath(".//div[contains(@class, 'fontHeadlineSmall')]/text()")[0] if business.xpath(".//div[contains(@class, 'fontHeadlineSmall')]/text()") else "N/A"
        logger.info(f"🔍 Processing: {name}")
        rating = business.xpath(".//span[@role='img' and contains(@aria-label, 'stars')]/span[1]/text()")[0] if business.xpath(".//span[@role='img' and contains(@aria-label, 'stars')]/span[1]/text()") else "N/A"
        reviews = business.xpath(".//span[@role='img' and contains(@aria-label, 'stars')]//span[contains(text(), '(')]/text()")[0] if business.xpath(".//span[@role='img' and contains(@aria-label, 'stars')]//span[contains(text(), '(')]/text()") else "N/A"
        if reviews != "N/A":
            reviews = reviews.strip('()')
        business_type = business.xpath(".//div[.//span[@aria-hidden='true' and text()='·']][1]//span[not(@aria-hidden='true') and not(span[@aria-hidden='true']) and not(contains(text(), 'Closed')) and not(contains(text(), 'Open')) and not(starts-with(text(), '('))][1]/span/text()")[0] if business.xpath(".//div[.//span[@aria-hidden='true' and text()='·']][1]//span[not(@aria-hidden='true') and not(span[@aria-hidden='true']) and not(contains(text(), 'Closed')) and not(contains(text(), 'Open')) and not(starts-with(text(), '('))][1]/span/text()") else "N/A"
        address = business.xpath(".//div[.//span[@aria-hidden='true' and text()='·']]//span[preceding-sibling::span[@aria-hidden='true' and text()='·'] and not(.//a[@href])]/text()")[0] if business.xpath(".//div[.//span[@aria-hidden='true' and text()='·']]//span[preceding-sibling::span[@aria-hidden='true' and text()='·'] and not(.//a[@href])]/text()") else "N/A"
        hours = business.xpath(".//div[.//span[@aria-hidden='true' and text()='·']]//span[contains(text(), 'Open') or contains(text(), 'Closed')]/text()")[0] if business.xpath(".//div[.//span[@aria-hidden='true' and text()='·']]//span[contains(text(), 'Open') or contains(text(), 'Closed')]/text()") else "N/A"
        phone = business.xpath(".//div[.//span[@aria-hidden='true' and text()='·']]//span[starts-with(text(), '(') and contains(text(), ')') and contains(text(), '-') and string-length(normalize-space(text())) >= 10]/text()")[0] if business.xpath(".//div[.//span[@aria-hidden='true' and text()='·']]//span[starts-with(text(), '(') and contains(text(), ')') and contains(text(), '-') and string-length(normalize-space(text())) >= 10]/text()") else "N/A"
        website = business.xpath(".//a[@data-value='Website']/@href")[0] if business.xpath(".//a[@data-value='Website']/@href") else "N/A"
        if website != "N/A":
            parsed_url = urlparse(website)
            website = f"{parsed_url.scheme}://{parsed_url.netloc}"
        if extract_emails and website != "N/A":
            email_result = await extractEmail(website, name, browser)
            email = ",".join(email_result["email"]) if email_result["email"] else "N/A"
        else:
            email = "N/A"
        sponsored = business.xpath(".//h1[.//span[text()='Sponsored']]//span[text()='Sponsored']/text()")[0] if business.xpath(".//h1[.//span[text()='Sponsored']]//span[text()='Sponsored']/text()") else "Not Sponsored"

        return {
            "Name": name,
            "Rating": rating,
            "Reviews": reviews,
            "Business Type": business_type,
            "Address": address,
            "Hours": hours,
            "Phone": phone,
            "Website": website,
            "Email": email,
            "Sponsored": sponsored
        }
    except Exception as e:
        logger.error(f"Error processing business: {e}")
        return None

async def scrape_business_info(page, browser, max_concurrent: int = 5):
    html_content = await page.content()
    tree = html.fromstring(html_content)
    businesses = tree.xpath("//div[@role='feed']//a[starts-with(@href, 'https://www.google.com/maps/place/')]/parent::div")

    logger.info(f"Found {len(businesses)} businesses to process with max {max_concurrent} concurrent")

    # Create semaphore to control concurrency
    semaphore = asyncio.Semaphore(max_concurrent)

    async def process_with_semaphore(business, idx):
        async with semaphore:
            logger.info(f"🔄 Starting business {idx + 1}/{len(businesses)}")
            result = await process_business(business, browser, extract_emails=False)
            if result:
                logger.info(f"✅ COMPLETED {idx + 1}/{len(businesses)}: {result['Name']} | {result['Rating']} ⭐ ({result['Reviews']} reviews) | {result['Business Type']} | {result['Address']} | {result['Phone']} | {result['Website']} | {result['Email']}")
            else:
                logger.info(f"❌ FAILED {idx + 1}/{len(businesses)}: Could not process business")
            return result

    # Process all businesses with controlled concurrency
    tasks = [process_with_semaphore(business, idx) for idx, business in enumerate(businesses)]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Filter successful results
    all_results = []
    for result in results:
        if isinstance(result, dict) and result is not None:
            all_results.append(result)
        elif isinstance(result, Exception):
            logger.error(f"Exception occurred: {result}")

    logger.info(f"✅ TOTAL COMPLETED: {len(all_results)}/{len(businesses)} businesses processed successfully")
    return all_results

async def scroll_google_maps(search_query: str, max_concurrent: int = 3):
    async with async_playwright() as p:
        try:
            browser = await p.chromium.launch(
                headless=False,
                args=[
                    '--disable-blink-features=AutomationControlled',
                    '--disable-extensions',
                    '--disable-plugins',
                    '--disable-images',
                    '--disable-background-timer-throttling',
                    '--disable-backgrounding-occluded-windows',
                    '--disable-renderer-backgrounding'
                ]
            )
            page = await browser.new_page(viewport={'width': 1280, 'height': 720})

            search_url = f"https://www.google.com/maps/search/{search_query.replace(' ', '+')}"
            logger.info(f"Navigating to {search_url}")
            await page.goto(search_url)
            logger.info("Page loaded")

            await asyncio.sleep(5)
            feed_container = page.locator('div[aria-label*="Results"][role="feed"]')
            if await feed_container.count() == 0:
                logger.error("Feed container not found")
                await page.screenshot(path="screenshot.png")
                await browser.close()
                return

            logger.info("Found feed container")
            await feed_container.scroll_into_view_if_needed()

            max_scroll_time = 600
            start_time = asyncio.get_event_loop().time()
            logger.info("Starting scroll loop")

            while (asyncio.get_event_loop().time() - start_time) < max_scroll_time:
                logger.info("Scrolling...")
                await feed_container.evaluate("""
                    (element) => {
                        return new Promise((resolve) => {
                            let totalHeight = 0;
                            const distance = 300;
                            let scrollTimeout;

                            const scroll = () => {
                                const scrollHeight = element.scrollHeight;
                                element.scrollBy(0, distance);
                                totalHeight += distance;

                                if (totalHeight >= scrollHeight) {
                                    clearTimeout(scrollTimeout);
                                    resolve();
                                } else {
                                    scrollTimeout = setTimeout(scroll, 500);
                                }
                            };

                            scroll();
                        });
                    }
                """)

                timestamp = datetime.now().strftime("%H:%M:%S")
                delay = random.uniform(2.0, 3.0)
                logger.info(f"Scrolled down at {timestamp} with delay {delay:.1f} seconds")
                await asyncio.sleep(delay)

                page_end = await page.locator('//span[contains(text(), "reached the end of the list")]').count()
                if page_end > 0:
                    logger.info("Reached the end of the list")
                    break

            logger.info(f"Scraping business info from feed with {max_concurrent} concurrent processes")
            await scrape_business_info(page, browser, max_concurrent)

            await page.screenshot(path="screenshot.png")
            logger.info("Screenshot saved as screenshot.png")
            await browser.close()
            logger.info("Browser closed")

        except Exception as e:
            logger.error(f"Error occurred: {e}")
            await page.screenshot(path="screenshot.png")
            await browser.close()

async def main():
    search_query = "locksmith kenosha"
    max_concurrent = 3  # Adjust this number to control concurrency (1-10 recommended)
    await scroll_google_maps(search_query, max_concurrent)

# (removed old __main__ runner)

# === END: user's scraper ===


# === BEGIN: API Orchestration (ported from extension) ===
import os
import json
import time
import random
import logging
import urllib.request
import urllib.error
import urllib.parse
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Callable, Set, Tuple

logger = logging.getLogger(__name__)

DEFAULT_BASE_URL = os.environ.get(
    "LEADS_API_BASE_URL",
    "https://fac10661-ee9f-4358-b944-f137d7d73a1a-00-30y5jt13c33qm.riker.replit.dev/api",
)
BATCH_SIZE = int(os.environ.get("LEADS_BATCH_SIZE", "20"))
REQUEST_TIMEOUT_S = float(os.environ.get("HTTP_TIMEOUT", "20"))
MAX_RETRIES = int(os.environ.get("HTTP_MAX_RETRIES", "5"))
RETRY_BACKOFF_S = float(os.environ.get("HTTP_RETRY_BACKOFF", "2.0"))
MAX_CONCURRENT = int(os.environ.get("MAX_CONCURRENT", "3"))

try:
    import requests  # type: ignore
except Exception:
    requests = None  # type: ignore


class HttpClient:
    def __init__(self, timeout: float = REQUEST_TIMEOUT_S, max_retries: int = MAX_RETRIES, backoff: float = RETRY_BACKOFF_S):
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff = backoff

    def _sleep(self, attempt: int):
        delay = self.backoff * (1 + attempt)
        delay *= (0.75 + random.random() * 0.5)
        time.sleep(delay)

    def get_text(self, url: str, headers: Optional[Dict[str,str]] = None) -> str:
        for attempt in range(self.max_retries):
            try:
                if requests is not None:
                    r = requests.get(url, headers=headers, timeout=self.timeout, allow_redirects=True)
                    if r.status_code >= 400:
                        raise RuntimeError(f"HTTP {r.status_code}: {r.text[:200]}")
                    return r.text or ""
                else:
                    req = urllib.request.Request(url, headers=headers or {})
                    with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                        return resp.read().decode("utf-8", errors="ignore")
            except Exception as e:
                if attempt == self.max_retries - 1:
                    logger.error(f"GET failed for {url}: {e}")
                    return ""
                logger.warning(f"GET retry {attempt+1}/{self.max_retries} for {url}: {e}")
                self._sleep(attempt)
        return ""

    def get_json(self, url: str, headers: Optional[Dict[str,str]] = None) -> Dict[str,Any]:
        text = self.get_text(url, headers=headers)
        if not text:
            return {}
        try:
            return json.loads(text)
        except Exception as e:
            logger.error(f"JSON parse error for {url}: {e}; raw={text[:300]}")
            return {}

    def post_json(self, url: str, payload: Any, headers: Optional[Dict[str,str]] = None) -> Tuple[int,str]:
        body = json.dumps(payload).encode("utf-8")
        for attempt in range(self.max_retries):
            try:
                if requests is not None:
                    r = requests.post(url, data=json.dumps(payload), headers={"Content-Type":"application/json", **(headers or {})}, timeout=self.timeout, allow_redirects=True)
                    return r.status_code, r.text or ""
                else:
                    req = urllib.request.Request(url, data=body, headers={"Content-Type":"application/json", **(headers or {})})
                    with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                        code = getattr(resp, "status", 200) or 200
                        text = resp.read().decode("utf-8", errors="ignore")
                        return code, text
            except Exception as e:
                if attempt == self.max_retries - 1:
                    logger.error(f"POST failed for {url}: {e}")
                    return 599, ""
                logger.warning(f"POST retry {attempt+1}/{self.max_retries} for {url}: {e}")
                self._sleep(attempt)
        return 599, ""


@dataclass
class Campaign:
    id: str
    name: str


@dataclass
class RequestItem:
    id: str
    req_text: str


class LeadsApiClient:
    def __init__(self, base_url: str, http: Optional[HttpClient] = None):
        self.base_url = base_url.rstrip("/")
        self.http = http or HttpClient()

    def get_active_campaigns(self) -> List[Campaign]:
        url = f"{self.base_url}/campaigns/active"
        data = self.http.get_json(url)
        out: List[Campaign] = []
        for c in (data.get("campaigns") or []):
            cid = str(c.get("id","")).strip()
            name = str(c.get("name","")).strip()
            if cid and name:
                out.append(Campaign(id=cid, name=name))
        return out

    def get_requests_for_campaign_name(self, campaign_name: str) -> List[RequestItem]:
        enc = urllib.parse.quote(campaign_name)
        url = f"{self.base_url}/campaign/{enc}/requests"
        data = self.http.get_json(url)
        out: List[RequestItem] = []
        for r in (data.get("requests") or []):
            rid = str(r.get("id","")).strip()
            txt = str(r.get("req_text","")).strip()
            if rid:
                out.append(RequestItem(id=rid, req_text=txt))
        return out

    def set_request_status(self, request_id: str, state: str) -> None:
        url = f"{self.base_url}/request/{request_id}/status/{state}"
        _ = self.http.get_text(url)

    def complete_campaign(self, campaign_id: str) -> None:
        url = f"{self.base_url}/campaign/{campaign_id}/complete"
        _ = self.http.get_text(url)

    def send_contacts(self, contacts: List[Dict[str,Any]]) -> bool:
        url = f"{self.base_url}/contacts"
        code, text = self.http.post_json(url, contacts)
        if code >= 400 or not text:
            logger.error(f"/contacts failed: code={code} text={text[:200]}")
            return False
        return True


PLACE_ID_RE = re.compile(r"place_id:([^\\\"/]+)")

def _strip_quotes(s: Optional[str]) -> str:
    if not s:
        return ""
    return re.sub(r'(^\\\"|\\\"$)', "", str(s)).strip()

def format_contacts_for_api(batch: List[Dict[str, Any]], campaign_id: str, request_id: str) -> List[Dict[str, Any]]:
    formatted: List[Dict[str, Any]] = []
    for contact in batch:
        url = _strip_quotes(contact.get("url", ""))
        m = PLACE_ID_RE.search(url or "")
        place_id = m.group(1) if m else ""

        def _g(key: str, default: str = "") -> str:
            v = contact.get(key, default)
            if isinstance(v, (list, dict)):
                v = json.dumps(v, ensure_ascii=False)
            return _strip_quotes(str(v))

        formatted.append(
            {
                "campaign_id": str(campaign_id),
                "request_id": str(request_id),
                "business_name": _g("companyName") or "Unknown Business",
                "address": _g("address"),
                "category": _g("category"),
                "rating": _g("rating"),
                "review_count": int(_g("reviews") or "0"),
                "phone": _g("phone"),
                "domain": _g("website"),
                "email": "",
                "facebook": "",
                "instagram": "",
                "twitter": "",
                "yelp": "",
                "place_id": place_id,
            }
        )
    return formatted


def write_batch_csv(batch: List[Dict[str, Any]], out_path: str, write_header: bool = False) -> None:
    fieldnames = ["companyName","address","phone","rating","reviews","category","website","url","email","facebook","instagram","twitter","yelp"]
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    import csv
    with open(out_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        for row in batch:
            writer.writerow({k: row.get(k, "") for k in fieldnames})


class CampaignProcessor:
    def __init__(self, api: LeadsApiClient, batch_size: int = BATCH_SIZE, csv_dir: Optional[str] = None):
        self.api = api
        self.batch_size = batch_size
        self.csv_dir = csv_dir
        self._stop = False

    def stop(self):
        self._stop = True

    def _on_batch(self, campaign_id: str, request_id: str, batch: List[Dict[str, Any]], total_seen: int) -> None:
        if not batch:
            return
        if self.csv_dir:
            out = os.path.join(self.csv_dir, f"leads_{campaign_id}_{request_id}.csv")
            write_batch_csv(batch, out, write_header=not os.path.exists(out))
        payload = format_contacts_for_api(batch, campaign_id, request_id)
        ok = self.api.send_contacts(payload)
        if ok:
            logger.info(f"Sent batch of {len(batch)} leads (total_seen={total_seen}) for request {request_id}")
        else:
            logger.error(f"Failed sending batch for request {request_id}")

    def process_campaign(self, campaign: Campaign) -> None:
        logger.info(f"Starting campaign: {campaign.name} (ID: {campaign.id})")
        while not self._stop:
            reqs = self.api.get_requests_for_campaign_name(campaign.name)
            if not reqs:
                logger.info("No more requests; completing campaign.")
                self.api.complete_campaign(campaign.id)
                break
            request = reqs[0]
            if not request.req_text:
                logger.warning(f"Empty req_text for request {request.id}; marking completed.")
                self.api.set_request_status(request.id, "completed")
                continue
            logger.info(f"Processing request {request.id} | query='{request.req_text}'")
            self.api.set_request_status(request.id, "inuse")
            total_seen = 0
            dedupe_keys: Set[str] = set()

            def batch_callback(batch: List[Dict[str, Any]]):
                nonlocal total_seen, dedupe_keys
                if not batch:
                    return
                cleaned: List[Dict[str, Any]] = []
                for lead in batch:
                    key = (lead.get("companyName",""), lead.get("address",""))
                    if key in dedupe_keys:
                        continue
                    dedupe_keys.add(key)
                    cleaned.append(lead)
                if not cleaned:
                    return
                total_seen += len(cleaned)
                for i in range(0, len(cleaned), self.batch_size):
                    sub = cleaned[i : i + self.batch_size]
                    self._on_batch(campaign.id, request.id, sub, total_seen)

            try:
                ok = run_scrape_and_yield_batches(request.req_text, self.batch_size, batch_callback)
                if not ok:
                    logger.warning(f"Scrape returned no data for request {request.id}")
            except Exception as e:
                logger.exception(f"Scrape error for request {request.id}: {e}")
            finally:
                self.api.set_request_status(request.id, "completed")
                logger.info(f"Completed request {request.id} (total leads seen: {total_seen})")
                time.sleep(1.5)


def run_all(csv_dir: Optional[str] = None, campaign_id: Optional[str] = None, campaign_name: Optional[str] = None) -> None:
    api = LeadsApiClient(DEFAULT_BASE_URL, HttpClient())
    processor = CampaignProcessor(api, batch_size=BATCH_SIZE, csv_dir=csv_dir)
    if campaign_id and campaign_name:
        processor.process_campaign(Campaign(id=campaign_id, name=campaign_name))
        return
    active = api.get_active_campaigns()
    if not active:
        logger.info("No active campaigns found. Exiting.")
        return
    for camp in active:
        if processor._stop:
            break
        processor.process_campaign(camp)


def run_scrape_and_yield_batches(query: str, batch_size: int, on_batch: Callable[[List[Dict[str, Any]]], None]) -> bool:
    import asyncio
    from playwright.async_api import async_playwright
    import urllib.parse

    async def _run() -> List[Dict[str, Any]]:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=[
                '--disable-blink-features=AutomationControlled','--disable-extensions','--disable-plugins','--disable-images','--disable-background-timer-throttling','--disable-backgrounding-occluded-windows','--disable-renderer-backgrounding'
            ])
            context = await browser.new_context(user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/114.0.0.0 Safari/537.36"
            ))
            page = await context.new_page()
            try:
                url = f"https://www.google.com/maps/search/{urllib.parse.quote_plus(query)}"
                await page.goto(url, wait_until="domcontentloaded", timeout=60000)
                await page.wait_for_selector('//div[@role="feed"]', timeout=60000)
                last = -1; stable = 0
                for _ in range(60):
                    await page.evaluate("""(sel) => {
                        const el = document.evaluate(sel, document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue;
                        if (!el) return;
                        el.scrollBy(0, el.scrollHeight);
                    }""", '//div[@role="feed"]')
                    await page.wait_for_timeout(800)
                    count = await page.evaluate("""(sel) => {
                        const el = document.evaluate(sel, document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue;
                        if (!el) return 0;
                        return el.querySelectorAll('a[href^="https://www.google.com/maps/place/"]').length;
                    }""", '//div[@role="feed"]')
                    if count == last: stable += 1
                    else: stable = 0
                    last = count
                    if stable >= 3: break
                results = await scrape_business_info(page, browser, max_concurrent=MAX_CONCURRENT)
                try: await page.screenshot(path="screenshot.png")
                except Exception: pass
                await browser.close()
                return results or []
            except Exception as e:
                logger.error(f"[Adapter] Playwright error: {e}")
                try: await browser.close()
                except Exception: pass
                return []

    results: List[Dict[str, Any]] = asyncio.run(_run())
    if not results:
        return False

    import re as _re
    def _pick(d: Dict[str, Any], *keys, default=""):
        for k in keys:
            if k in d and d[k] not in (None, "", "N/A"):
                return d[k]
        return default
    def _rev_to_str(v: Any) -> str:
        s = str(v)
        m = _re.search(r"([\\d,]+)", s)
        return m.group(1).replace(",", "") if m else ""

    batch: List[Dict[str, Any]] = []; sent_any = False
    for r in results:
        contact = {
            "companyName": _pick(r, "Name", "Business Name", "Company"),
            "address": _pick(r, "Address"),
            "phone": _pick(r, "Phone", "Telephone"),
            "rating": str(_pick(r, "Rating", default="")),
            "reviews": _rev_to_str(_pick(r, "Reviews", default="")),
            "category": _pick(r, "Business Type", "Category"),
            "website": _pick(r, "Website", "Site", "URL", "Url"),
            "url": _pick(r, "Url", "URL", "Maps URL", "MapUrl", default=""),
            "email": "","facebook": "","instagram": "","twitter": "","yelp": "",
        }
        batch.append(contact)
        if len(batch) >= batch_size:
            on_batch(batch); sent_any = True; batch = []
    if batch: on_batch(batch); sent_any = True
    return sent_any

# === END: API Orchestration ===

if __name__ == "__main__":
    run_all(csv_dir=None, campaign_id=None, campaign_name=None)
