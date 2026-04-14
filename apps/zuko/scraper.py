"""
Playwright-based job scrapers for Seek.com.au and LinkedIn.

Sessions are saved to data/sessions/ so login only happens once.
Call setup_sessions.py once to set up persistent SEEK and LinkedIn sessions.
"""
import asyncio
import logging
import os
import random
import re
from pathlib import Path
from urllib.parse import quote

log = logging.getLogger(__name__)

SESSION_DIR = Path("data/sessions")
SESSION_DIR.mkdir(parents=True, exist_ok=True)

SEEK_SESSION     = str(SESSION_DIR / "seek_session.json")
LINKEDIN_SESSION = str(SESSION_DIR / "linkedin_session.json")

HEADLESS = os.environ.get("HEADLESS", "true").lower() != "false"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


async def _delay(lo=0.8, hi=2.5):
    await asyncio.sleep(random.uniform(lo, hi))


async def _make_context(p, session_file: str | None = None, headless: bool | None = None):
    """Create a stealth browser context, optionally restoring saved session."""
    use_headless = headless if headless is not None else HEADLESS
    browser = await p.chromium.launch(
        headless=use_headless,
        args=[
            "--no-sandbox",
            "--disable-blink-features=AutomationControlled",
            "--disable-infobars",
            "--start-maximized",
        ],
        slow_mo=30,
    )
    storage = session_file if (session_file and Path(session_file).exists()) else None
    ctx = await browser.new_context(
        user_agent=USER_AGENT,
        viewport={"width": 1366, "height": 768},
        locale="en-AU",
        timezone_id="UTC",
        storage_state=storage,
    )
    await ctx.add_init_script(
        "Object.defineProperty(navigator,'webdriver',{get:()=>undefined});"
    )
    return browser, ctx


# ── SEEK ──────────────────────────────────────────────────────────────────────

async def seek_login_interactive():
    """Open headed browser for Seek login. Run once via setup_sessions.py."""
    from playwright.async_api import async_playwright
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=False,
            ignore_default_args=["--enable-automation"],
            args=["--no-sandbox", "--disable-infobars", "--start-maximized"],
        )
        ctx = await browser.new_context(
            user_agent=USER_AGENT,
            viewport={"width": 1366, "height": 768},
            locale="en-AU",
            timezone_id="UTC",
        )
        page = await ctx.new_page()
        try:
            await page.goto("https://www.seek.com.au/sign-in", timeout=30000)
        except Exception:
            pass
        log.info("[Seek] Browser opened. Please log in, then press ENTER here.")
        input(">>> Press ENTER after you have logged in to Seek <<<")
        await ctx.storage_state(path=SEEK_SESSION)
        log.info("[Seek] Session saved to %s", SEEK_SESSION)
        await browser.close()


async def scrape_seek(role: str, location: str = "Your City") -> list[dict]:
    """Scrape seek.com.au for a given role and location. Returns list of job dicts."""
    from playwright.async_api import async_playwright
    jobs: list[dict] = []
    try:
        async with async_playwright() as p:
            browser, ctx = await _make_context(p, SEEK_SESSION)
            page = await ctx.new_page()

            url = (
                f"https://www.seek.com.au/jobs"
                f"?keywords={quote(role)}"
                f"&where={quote(location)}"
                f"&sortmode=ListedDate"
            )
            log.info("[Seek] Scraping: %s", url)
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await _delay(2, 4)

            try:
                await page.wait_for_selector(
                    'article[data-card-type="JobCard"], [data-testid="job-card"]',
                    timeout=15000,
                )
            except Exception:
                log.warning("[Seek] No job cards found for '%s'", role)
                await browser.close()
                return []

            cards = await page.query_selector_all(
                'article[data-card-type="JobCard"], [data-testid="job-card"]'
            )
            log.info("[Seek] %d cards for '%s'", len(cards), role)

            for card in cards[:10]:
                try:
                    job = await _parse_seek_card(card, page)
                    if job:
                        jobs.append(job)
                except Exception as exc:
                    log.debug("[Seek] card error: %s", exc)

            await ctx.storage_state(path=SEEK_SESSION)
            await browser.close()

    except Exception as exc:
        log.error("[Seek] scrape_seek('%s') failed: %s", role, exc)

    return jobs


async def _parse_seek_card(card, page) -> dict | None:
    try:
        title_el = (
            await card.query_selector('[data-automation="jobTitle"]') or
            await card.query_selector('a[data-testid="job-title"]') or
            await card.query_selector('h3 a')
        )
        company_el = (
            await card.query_selector('[data-automation="jobCompany"]') or
            await card.query_selector('[data-testid="job-card-company"]') or
            await card.query_selector('a[data-automation="jobListingCompany"]')
        )
        location_el = (
            await card.query_selector('[data-automation="jobLocation"]') or
            await card.query_selector('[data-testid="job-card-location"]') or
            await card.query_selector('[data-automation="jobArea"]')
        )
        salary_el = (
            await card.query_selector('[data-automation="jobSalary"]') or
            await card.query_selector('[data-testid="job-card-pay"]')
        )

        title    = (await title_el.inner_text()).strip()    if title_el    else ""
        company  = (await company_el.inner_text()).strip()  if company_el  else ""
        location = (await location_el.inner_text()).strip() if location_el else ""
        salary   = (await salary_el.inner_text()).strip()   if salary_el   else ""

        href = await title_el.get_attribute("href") if title_el else None
        if not href:
            href = await card.evaluate("el => el.querySelector('a')?.href || ''")

        if not title or not href:
            return None

        job_url = f"https://www.seek.com.au{href}" if href.startswith("/") else href
        m = re.search(r'/job/(\d+)', job_url)
        job_id = m.group(1) if m else re.sub(r'\W+', '_', title)[:20]

        return {
            "job_id":      f"seek_{job_id}",
            "title":       title,
            "company":     company,
            "location":    location,
            "url":         job_url.split("?")[0],
            "salary":      salary,
            "description": "",
            "source":      "seek",
            "apply_type":  "unknown",
        }
    except Exception as exc:
        log.debug("[Seek] parse_card error: %s", exc)
        return None


async def seek_get_job_detail(url: str) -> tuple[str, str]:
    """
    Fetch full description + detect apply type.
    Returns (description, apply_type) where apply_type is:
      'seek_quick'  — Quick Apply (stays on Seek)
      'company'     — redirects to company ATS
      'manual'      — fallback
    """
    from playwright.async_api import async_playwright
    desc, apply_type = "", "manual"
    try:
        async with async_playwright() as p:
            browser, ctx = await _make_context(p, SEEK_SESSION)
            page = await ctx.new_page()
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await _delay(2, 3)

            desc_el = (
                await page.query_selector('[data-automation="jobDescription"]') or
                await page.query_selector('[data-automation="job-detail-description"]') or
                await page.query_selector('.yvsb870')
            )
            if desc_el:
                desc = (await desc_el.inner_text())[:3000]

            apply_btn = (
                await page.query_selector('[data-automation="job-detail-apply"]') or
                await page.query_selector('a[data-automation="job-detail-apply"]')
            )
            if apply_btn:
                btn_text = (await apply_btn.inner_text()).lower()
                if "quick apply" in btn_text or "easy apply" in btn_text:
                    apply_type = "seek_quick"
                elif "company" in btn_text or "external" in btn_text:
                    apply_type = "company"
                else:
                    apply_type = "seek_quick"

            await ctx.storage_state(path=SEEK_SESSION)
            await browser.close()
    except Exception as exc:
        log.error("[Seek] get_detail failed for %s: %s", url, exc)

    return desc, apply_type


# ── LINKEDIN ──────────────────────────────────────────────────────────────────

async def linkedin_login_interactive():
    """Open headed browser for LinkedIn login. Run once via setup_sessions.py."""
    from playwright.async_api import async_playwright
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=False,
            ignore_default_args=["--enable-automation"],
            args=["--no-sandbox", "--disable-infobars", "--start-maximized"],
        )
        ctx = await browser.new_context(
            user_agent=USER_AGENT,
            viewport={"width": 1366, "height": 768},
            locale="en-AU",
            timezone_id="UTC",
        )
        page = await ctx.new_page()
        try:
            await page.goto("https://www.linkedin.com", timeout=30000)
        except Exception:
            pass
        log.info("[LinkedIn] Browser opened. Log in, then press ENTER here.")
        input(">>> Press ENTER after you have logged in to LinkedIn <<<")
        await ctx.storage_state(path=LINKEDIN_SESSION)
        log.info("[LinkedIn] Session saved to %s", LINKEDIN_SESSION)
        await browser.close()


async def scrape_linkedin(role: str, location: str = "Your City, Your Country") -> list[dict]:
    """Scrape LinkedIn Jobs using JS extraction — resilient to class name changes."""
    from playwright.async_api import async_playwright
    jobs: list[dict] = []
    try:
        async with async_playwright() as p:
            browser, ctx = await _make_context(p, LINKEDIN_SESSION)
            page = await ctx.new_page()

            url = (
                f"https://www.linkedin.com/jobs/search/"
                f"?keywords={quote(role)}"
                f"&location={quote(location)}"
                f"&f_TPR=r604800"
                f"&sortBy=DD"
            )
            log.info("[LinkedIn] Scraping: %s", url)
            await page.goto(url, wait_until="domcontentloaded", timeout=35000)
            await _delay(3, 5)

            if "authwall" in page.url or "/login" in page.url or "/uas/login" in page.url:
                log.warning("[LinkedIn] Session expired — run setup_sessions.py")
                await browser.close()
                return []

            for dismiss_sel in [
                'button[aria-label="Dismiss"]',
                'button:has-text("Dismiss")',
                '[data-test-modal-close-btn]',
            ]:
                try:
                    btn = await page.query_selector(dismiss_sel)
                    if btn:
                        await btn.click()
                        await _delay(0.5, 1)
                        break
                except Exception:
                    pass

            for _ in range(3):
                await page.evaluate("window.scrollBy(0, 600)")
                await _delay(0.8, 1.5)

            raw_cards = await page.evaluate("""
                () => {
                    const results = [];
                    const seen = new Set();
                    document.querySelectorAll('a[href*="/jobs/view/"]').forEach(a => {
                        const href = a.href.split('?')[0];
                        if (seen.has(href)) return;
                        seen.add(href);
                        let container = a;
                        for (let i = 0; i < 8; i++) {
                            if (!container.parentElement) break;
                            container = container.parentElement;
                            if (container.tagName === 'LI') break;
                        }
                        const titleText = a.innerText.trim();
                        if (!titleText) return;
                        const allText = container.innerText || '';
                        const lines = allText.split('\\n').map(l => l.trim()).filter(l => l && l !== titleText);
                        const company  = lines[0] || '';
                        const location = lines[1] || '';
                        const easyApply = container.innerText.includes('Easy Apply');
                        results.push({ href, title: titleText, company, location, easyApply });
                    });
                    return results.slice(0, 15);
                }
            """)

            log.info("[LinkedIn] JS extracted %d cards for '%s'", len(raw_cards), role)

            for card in raw_cards:
                try:
                    href  = card.get("href", "")
                    title = card.get("title", "")
                    if not href or not title:
                        continue
                    m = re.search(r'/jobs/view/(\d+)', href)
                    job_id = m.group(1) if m else re.sub(r'\W+', '_', title)[:20]
                    jobs.append({
                        "job_id":      f"linkedin_{job_id}",
                        "title":       title,
                        "company":     card.get("company", "").strip(),
                        "location":    card.get("location", "").strip(),
                        "url":         href,
                        "salary":      "",
                        "description": "",
                        "source":      "linkedin",
                        "apply_type":  "linkedin_easy" if card.get("easyApply") else "company",
                    })
                except Exception as exc:
                    log.debug("[LinkedIn] card parse error: %s", exc)

            await ctx.storage_state(path=LINKEDIN_SESSION)
            await browser.close()

    except Exception as exc:
        log.error("[LinkedIn] scrape_linkedin('%s') failed: %s", role, exc)

    return jobs


async def linkedin_get_job_detail(url: str) -> tuple[str, str]:
    """Fetch LinkedIn job description and confirm apply type."""
    from playwright.async_api import async_playwright
    desc, apply_type = "", "company"
    try:
        async with async_playwright() as p:
            browser, ctx = await _make_context(p, LINKEDIN_SESSION)
            page = await ctx.new_page()
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await _delay(2, 3)

            await page.evaluate("""
                () => {
                    document.querySelectorAll('p span button, button').forEach(btn => {
                        const txt = btn.innerText || btn.textContent || '';
                        if (txt.includes('more') && btn.offsetParent !== null) {
                            try { btn.click(); } catch(e) {}
                        }
                    });
                }
            """)
            await _delay(1, 2)

            desc = await page.evaluate("""
                () => {
                    const candidates = [
                        document.querySelector('.show-more-less-html__markup'),
                        document.querySelector('[class*="description__text"]'),
                        document.querySelector('[class*="job-description"]'),
                        document.querySelector('section[class*="description"] div'),
                    ];
                    for (const el of candidates) {
                        if (el && el.innerText.trim().length > 50) {
                            return el.innerText.trim().slice(0, 3000);
                        }
                    }
                    return '';
                }
            """)

            easy_btn = (
                await page.query_selector('[aria-label*="Easy Apply"]') or
                await page.query_selector('button:has-text("Easy Apply")')
            )
            if easy_btn:
                apply_type = "linkedin_easy"

            await ctx.storage_state(path=LINKEDIN_SESSION)
            await browser.close()
    except Exception as exc:
        log.error("[LinkedIn] get_detail failed for %s: %s", url, exc)

    return desc, apply_type


# ── LINKEDIN FEED ─────────────────────────────────────────────────────────────

import re as _re

_JOB_POST_KEYWORDS = [
    "hiring", "we're hiring", "we are hiring", "now hiring",
    "looking for", "looking to hire", "seeking", "we're looking",
    "open role", "open position", "job opportunity", "career opportunity",
    "join our team", "join us", "come work with us",
    "reach out", "dm me", "send your cv", "send your resume",
    "applications open", "apply now", "interested candidates",
    "we need a", "exciting opportunity", "immediate start",
    "urgently seeking", "we have a vacancy", "vacancy",
]

_EMAIL_RE = _re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b')
_PHONE_RE = _re.compile(r'\b(?:\+?61[-\s]?)?0[0-9]{9}\b|\b04[0-9]{2}[-\s]?[0-9]{3}[-\s]?[0-9]{3}\b')


def _is_job_post(text: str) -> bool:
    t = text.lower()
    return any(kw in t for kw in _JOB_POST_KEYWORDS)


async def _expand_feed_posts(page) -> int:
    clicked = await page.evaluate("""
        () => {
            let count = 0;
            const seen = new Set();
            document.querySelectorAll('p span button').forEach(btn => {
                if (seen.has(btn) || btn.offsetParent === null) return;
                const txt = btn.innerText || btn.textContent || '';
                if (!txt.includes('more')) return;
                seen.add(btn);
                try { btn.click(); count++; } catch(e) {}
            });
            return count;
        }
    """)
    return clicked


async def scrape_linkedin_feed(max_posts: int = 50) -> list[dict]:
    """Read LinkedIn feed posts and return job advertisement dicts."""
    from playwright.async_api import async_playwright
    posts: list[dict] = []
    try:
        async with async_playwright() as p:
            browser, ctx = await _make_context(p, LINKEDIN_SESSION)
            page = await ctx.new_page()

            log.info("[LinkedIn Feed] Loading feed...")
            await page.goto("https://www.linkedin.com/feed/", wait_until="domcontentloaded", timeout=30000)
            await _delay(3, 5)

            if "authwall" in page.url or "/login" in page.url:
                log.warning("[LinkedIn Feed] Not logged in")
                await browser.close()
                return []

            scroll_rounds = 15
            for i in range(scroll_rounds):
                await page.evaluate("window.scrollBy(0, window.innerHeight * 0.75)")
                await _delay(1.5, 3)
                if (i + 1) % 3 == 0:
                    n = await _expand_feed_posts(page)
                    log.info("[LinkedIn Feed] Scroll %d/%d — expanded %d posts", i + 1, scroll_rounds, n)
                    await _delay(1, 2)

            n = await _expand_feed_posts(page)
            log.info("[LinkedIn Feed] Final expand — %d additional posts", n)
            await _delay(2, 3)

            raw_posts = await page.evaluate("""
                () => {
                    const results = [];
                    const seen = new Set();
                    const postSelectors = [
                        '[data-urn*="urn:li:activity"]',
                        '[data-id*="urn:li:activity"]',
                        '.feed-shared-update-v2',
                        '.occludable-update',
                    ];
                    let containers = [];
                    for (const sel of postSelectors) {
                        containers = Array.from(document.querySelectorAll(sel));
                        if (containers.length > 0) break;
                    }
                    containers.forEach(post => {
                        try {
                            const textEl = post.querySelector(
                                '[class*="commentary"], [class*="update-components-text"], ' +
                                '[class*="feed-shared-text"], .break-words'
                            );
                            const text = (textEl ? textEl.innerText : post.innerText).trim();
                            if (!text || text.length < 30) return;
                            if (seen.has(text.slice(0, 100))) return;
                            seen.add(text.slice(0, 100));
                            const authorEl = post.querySelector(
                                '[class*="actor__title"], [class*="actor__name"], ' +
                                '.update-components-actor__title, .feed-shared-actor__title'
                            );
                            const companyEl = post.querySelector(
                                '[class*="actor__description"], [class*="actor__subtitle"], ' +
                                '.update-components-actor__description, .feed-shared-actor__description'
                            );
                            const author  = authorEl  ? authorEl.innerText.trim()  : '';
                            const company = companyEl ? companyEl.innerText.trim() : '';
                            const linkEl  = post.querySelector('a[href*="/posts/"], a[href*="/feed/update/"]');
                            const postUrl = linkEl ? linkEl.href.split('?')[0] : '';
                            results.push({ text, author, company, postUrl });
                        } catch(e) {}
                    });
                    return results;
                }
            """)

            log.info("[LinkedIn Feed] %d total posts extracted", len(raw_posts))

            for raw in raw_posts[:max_posts]:
                text = raw.get("text", "")
                if not _is_job_post(text):
                    continue
                emails = _EMAIL_RE.findall(text)
                phones = _PHONE_RE.findall(text)
                post_id = f"feed_{abs(hash(text[:120])) % 10**9}"
                posts.append({
                    "post_id":  post_id,
                    "job_id":   post_id,
                    "title":    raw.get("author", "LinkedIn Post"),
                    "text":     text[:3000],
                    "author":   raw.get("author", ""),
                    "company":  raw.get("company", ""),
                    "location": "",
                    "url":      raw.get("postUrl", ""),
                    "salary":   "",
                    "description": text[:3000],
                    "source":   "linkedin_feed",
                    "apply_type": "email" if emails else "manual",
                    "emails":   list(set(emails)),
                    "phones":   list(set(phones)),
                })
                log.info("[LinkedIn Feed] Job post: %s — emails=%s", raw.get("author"), emails)

            await ctx.storage_state(path=LINKEDIN_SESSION)
            await browser.close()

    except Exception as exc:
        log.error("[LinkedIn Feed] scrape_linkedin_feed failed: %s", exc)

    return posts
