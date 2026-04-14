"""
Auto-apply engine for Zuko — Seek Quick Apply, LinkedIn Easy Apply, generic ATS.

Pre-submission human approval is always required before any form is submitted.
"""
import asyncio
import json
import logging
import os
import random
import re
from dataclasses import dataclass, field
from pathlib import Path

log = logging.getLogger(__name__)

BASE_DIR        = Path(__file__).parent
SESSION_DIR     = Path("data/sessions")
SCREENSHOTS_DIR = Path("data/screenshots")
SCREENSHOTS_DIR.mkdir(parents=True, exist_ok=True)

SEEK_SESSION     = str(SESSION_DIR / "seek_session.json")
LINKEDIN_SESSION = str(SESSION_DIR / "linkedin_session.json")
PROFILE_PATH     = BASE_DIR / "config" / "candidate_profile.json"

HEADLESS_APPLY = os.environ.get("HEADLESS_APPLY", "true").lower() != "false"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

ATS_MARKERS = {
    "Workday":         "myworkdayjobs.com",
    "Greenhouse":      "greenhouse.io",
    "Lever":           "lever.co",
    "SmartRecruiters": "smartrecruiters.com",
    "Jobvite":         "jobvite.com",
    "Taleo":           "taleo.net",
    "iCIMS":           "icims.com",
    "SuccessFactors":  "successfactors",
    "BambooHR":        "bamboohr.com",
    "Recruitee":       "recruitee.com",
}


def _load_profile() -> dict:
    try:
        return json.loads(PROFILE_PATH.read_text(encoding="utf-8"))
    except Exception as exc:
        log.warning("[Profile] Could not load candidate_profile.json: %s", exc)
        return {}


PROFILE = _load_profile()

FIELD_MAP: dict[str, str] = {
    "first name":    PROFILE.get("first_name", ""),
    "first":         PROFILE.get("first_name", ""),
    "given name":    PROFILE.get("first_name", ""),
    "last name":     PROFILE.get("last_name", ""),
    "last":          PROFILE.get("last_name", ""),
    "surname":       PROFILE.get("last_name", ""),
    "family name":   PROFILE.get("last_name", ""),
    "full name":     PROFILE.get("full_name", ""),
    "name":          PROFILE.get("full_name", ""),
    "email":         PROFILE.get("email", ""),
    "phone":         PROFILE.get("phone_formatted", ""),
    "mobile":        PROFILE.get("phone_formatted", ""),
    "telephone":     PROFILE.get("phone_formatted", ""),
    "city":          PROFILE.get("city", ""),
    "suburb":        PROFILE.get("city", ""),
    "location":      PROFILE.get("location", ""),
    "postcode":      PROFILE.get("postcode", ""),
    "zip":           PROFILE.get("postcode", ""),
    "state":         PROFILE.get("state", ""),
    "country":       PROFILE.get("country", ""),
    "address":       f"{PROFILE.get('city', '')} {PROFILE.get('state', '')}",
    "linkedin":      PROFILE.get("linkedin_url", ""),
    "github":        PROFILE.get("github_url", ""),
    "portfolio":     PROFILE.get("portfolio_url", ""),
    "website":       PROFILE.get("linkedin_url", ""),
    "salary":        str(PROFILE.get("salary_expectation_aud", "")),
    "notice":        PROFILE.get("notice_period_text", ""),
    "years of experience": str(PROFILE.get("years_experience", "")),
    "years experience":    str(PROFILE.get("years_experience", "")),
}


@dataclass
class ApplyResult:
    success: bool
    message: str
    screenshot_path: str | None = None
    fields_filled: list[str] = field(default_factory=list)
    fields_flagged: list[str] = field(default_factory=list)


async def _delay(lo=0.6, hi=2.0):
    await asyncio.sleep(random.uniform(lo, hi))


async def _type_human(page, selector: str, text: str):
    try:
        await page.click(selector)
        await page.fill(selector, "")
        for ch in text:
            await page.type(selector, ch)
            await asyncio.sleep(random.uniform(0.03, 0.09))
    except Exception:
        try:
            await page.fill(selector, text)
        except Exception:
            pass


async def _make_ctx(p, session_file: str | None = None):
    browser = await p.chromium.launch(
        headless=HEADLESS_APPLY,
        ignore_default_args=["--enable-automation"],
        args=["--no-sandbox", "--disable-infobars", "--start-maximized"],
        slow_mo=60,
    )
    storage = session_file if (session_file and Path(session_file).exists()) else None
    ctx = await browser.new_context(
        user_agent=USER_AGENT,
        viewport={"width": 1366, "height": 768},
        locale="en-AU",
        timezone_id="UTC",
        storage_state=storage,
        accept_downloads=True,
    )
    await ctx.add_init_script(
        "Object.defineProperty(navigator,'webdriver',{get:()=>undefined});"
    )
    return browser, ctx


async def _screenshot(page, name: str) -> str | None:
    try:
        path = str(SCREENSHOTS_DIR / f"{name}.png")
        await page.screenshot(path=path, full_page=False)
        return path
    except Exception as exc:
        log.debug("Screenshot failed: %s", exc)
        return None


def _detect_ats(url: str) -> str:
    url = url.lower()
    for name, marker in ATS_MARKERS.items():
        if marker in url:
            return name
    return "Custom ATS"


async def _get_field_label(page, inp) -> str:
    try:
        inp_id = await inp.get_attribute("id")
        if inp_id:
            lbl = await page.query_selector(f'label[for="{inp_id}"]')
            if lbl:
                return (await lbl.inner_text()).strip()
        placeholder = await inp.get_attribute("placeholder") or ""
        name        = await inp.get_attribute("name") or ""
        aria_label  = await inp.get_attribute("aria-label") or ""
        return (aria_label or placeholder or name).strip()
    except Exception:
        return ""


async def apply_to_job(job: dict, cover_letter: str, bot=None, chat_id: int = 0) -> ApplyResult:
    """Route to correct apply method with pre-submission review."""
    from playwright.async_api import async_playwright
    apply_type = job.get("apply_type", "unknown")
    source     = job.get("source", "")
    log.info("[Apply] %s @ %s — type=%s", job.get("title", job.get("role", "")), job.get("company", ""), apply_type)

    if apply_type == "linkedin_easy" or (source == "linkedin" and apply_type != "company"):
        return await _apply_linkedin_easy(job, cover_letter, bot, chat_id)
    elif apply_type in ("seek_quick", "seek") or source == "seek":
        return await _apply_seek(job, cover_letter, bot, chat_id)
    else:
        return await _apply_generic_ats(job, cover_letter, bot, chat_id)


async def _pre_submit_review(page, job: dict, safe_fields, confirm_fields, unknown_fields, bot, chat_id: int) -> str:
    """Take screenshot, send Telegram review, wait for APPROVE/REJECT."""
    if not bot or not chat_id:
        log.warning("[Review] No bot/chat_id — auto-approving")
        return "approve"

    shot = await _screenshot(page, job["job_id"] + "_review")
    from apps.zuko.modules.approval_gate import request_approval
    return await request_approval(
        bot=bot,
        chat_id=chat_id,
        job=job,
        screenshot_path=shot,
        safe_fields=safe_fields,
        confirm_fields=confirm_fields,
        unknown_fields=unknown_fields,
        timeout_seconds=300,
    )


async def _check_stop(page, job: dict, bot, chat_id: int) -> str | None:
    from apps.zuko.modules.stop_detector import check_page, check_for_assessment_page
    stop = await check_page(page) or await check_for_assessment_page(page)
    if stop:
        log.warning("[Stop] %s — %s", stop.reason, stop.detail)
        shot = await _screenshot(page, job["job_id"] + "_stop")
        if bot and chat_id:
            try:
                await bot.send_message(
                    chat_id,
                    f"🛑 <b>Stop condition detected</b>\n\n"
                    f"<b>Job:</b> {job.get('title', job.get('role', ''))} @ {job.get('company', '')}\n"
                    f"<b>Reason:</b> {stop.reason}\n"
                    f"<b>Detail:</b> {stop.detail}\n\n"
                    f"Apply manually: {job.get('url', '')}",
                    parse_mode="HTML",
                )
                if shot and Path(shot).exists():
                    with open(shot, "rb") as f:
                        await bot.send_photo(chat_id, f, caption="Stop condition screenshot")
            except Exception as exc:
                log.error("[Stop] Failed to send alert: %s", exc)
        return stop.reason
    return None


# ── SEEK APPLY ────────────────────────────────────────────────────────────────

async def _seek_ensure_login(page) -> bool:
    from playwright.async_api import async_playwright
    await page.goto("https://www.seek.com.au/dashboard", wait_until="domcontentloaded", timeout=20000)
    await _delay(1, 2)
    if "dashboard" in page.url or "profile" in page.url:
        return True

    email    = os.environ.get("SEEK_EMAIL", "")
    password = os.environ.get("SEEK_PASSWORD", "")
    if not email or not password:
        log.warning("[Seek] No credentials — relying on saved session")
        return False

    try:
        await page.goto("https://www.seek.com.au/sign-in", wait_until="domcontentloaded", timeout=20000)
        await _delay(1, 2)
        await _type_human(page, 'input[name="email"], input[type="email"]', email)
        await _delay(0.5, 1)
        await _type_human(page, 'input[name="password"], input[type="password"]', password)
        await _delay(0.5, 1)
        await page.click('button[data-automation="login"], button[type="submit"]')
        await page.wait_for_load_state("networkidle", timeout=15000)
        return "dashboard" in page.url or "profile" in page.url
    except Exception as exc:
        log.warning("[Seek] Login attempt failed: %s", exc)
        return False


async def _apply_seek(job: dict, cover_letter: str, bot, chat_id: int) -> ApplyResult:
    from playwright.async_api import async_playwright
    from apps.zuko.modules.field_classifier import FieldClassifier
    classifier  = FieldClassifier()
    safe_fields, confirm_fields, unknown_fields = [], [], []
    cv_path = PROFILE.get("cv_path", os.environ.get("ZUKO_CV_PATH", ""))

    try:
        async with async_playwright() as p:
            browser, ctx = await _make_ctx(p, SEEK_SESSION)
            page = await ctx.new_page()

            # Ensure we're logged in
            await _seek_ensure_login(page)

            # Navigate to job
            await page.goto(job["url"], wait_until="domcontentloaded", timeout=30000)
            await _delay(2, 3)

            stop = await _check_stop(page, job, bot, chat_id)
            if stop:
                await browser.close()
                return ApplyResult(success=False, message=f"Stop: {stop}")

            # Click apply
            apply_btn = (
                await page.query_selector('[data-automation="job-detail-apply"]') or
                await page.query_selector('a[data-automation="job-detail-apply"]')
            )
            if not apply_btn:
                await browser.close()
                return ApplyResult(
                    success=False,
                    message="No apply button found — apply manually.",
                    screenshot_path=await _screenshot(page, job["job_id"] + "_no_btn"),
                )

            await apply_btn.click()
            await page.wait_for_load_state("domcontentloaded", timeout=15000)
            await _delay(2, 3)

            stop = await _check_stop(page, job, bot, chat_id)
            if stop:
                await browser.close()
                return ApplyResult(success=False, message=f"Stop after clicking apply: {stop}")

            # Fill cover letter
            for cl_sel in [
                'textarea[data-automation="cover-letter"]',
                'textarea[name="coverLetter"]',
                'textarea[placeholder*="cover" i]',
                'textarea[placeholder*="letter" i]',
            ]:
                try:
                    cl_field = await page.query_selector(cl_sel)
                    if cl_field:
                        await cl_field.fill(cover_letter[:3000])
                        safe_fields.append("Cover letter")
                        break
                except Exception:
                    pass

            # Upload CV
            if cv_path and Path(cv_path).exists():
                file_inputs = await page.query_selector_all('input[type="file"]')
                for fi in file_inputs[:1]:
                    try:
                        await fi.set_input_files(cv_path)
                        safe_fields.append("Resume/CV uploaded")
                    except Exception as exc:
                        log.debug("[Seek] CV upload failed: %s", exc)

            # Fill standard fields
            inputs = await page.query_selector_all('input:not([type="hidden"]):not([type="file"]), textarea')
            for inp in inputs:
                label = await _get_field_label(page, inp)
                if not label:
                    continue
                tier = classifier.classify(label)
                if tier == "stop":
                    stop_reason = classifier.stop_reason(label)
                    unknown_fields.append(f"{label} — requires review ({stop_reason})")
                elif tier in ("safe", "confirm"):
                    answer = classifier.get_answer(label)
                    if answer:
                        try:
                            await inp.fill(answer)
                            if tier == "safe":
                                safe_fields.append(label)
                            else:
                                confirm_fields.append(f"{label} = {answer}")
                        except Exception:
                            unknown_fields.append(label)
                    else:
                        # Try FIELD_MAP fallback
                        matched = next((v for k, v in FIELD_MAP.items() if k in label.lower()), None)
                        if matched:
                            try:
                                await inp.fill(matched)
                                safe_fields.append(label)
                            except Exception:
                                unknown_fields.append(label)
                        else:
                            unknown_fields.append(label)
                else:
                    unknown_fields.append(label)

            # Pre-submission review
            decision = await _pre_submit_review(
                page, job, safe_fields, confirm_fields, unknown_fields, bot, chat_id
            )

            if decision != "approve":
                await browser.close()
                return ApplyResult(
                    success=False,
                    message=f"Application {'cancelled' if decision == 'reject' else 'timed out'}.",
                )

            # Submit
            submit_btn = (
                await page.query_selector('[data-automation="submit-button"]') or
                await page.query_selector('button[type="submit"]') or
                await page.query_selector('button:has-text("Submit")')
            )
            if submit_btn:
                await submit_btn.click()
                await page.wait_for_load_state("networkidle", timeout=15000)
                await _delay(2, 3)
                shot = await _screenshot(page, job["job_id"] + "_submitted")
                await ctx.storage_state(path=SEEK_SESSION)
                await browser.close()
                return ApplyResult(
                    success=True,
                    message="Application submitted via Seek Quick Apply.",
                    screenshot_path=shot,
                    fields_filled=safe_fields + confirm_fields,
                )
            else:
                shot = await _screenshot(page, job["job_id"] + "_no_submit")
                await browser.close()
                return ApplyResult(
                    success=False,
                    message="Could not find submit button — check screenshot.",
                    screenshot_path=shot,
                )

    except Exception as exc:
        log.error("[Seek Apply] %s: %s", job.get("job_id"), exc, exc_info=True)
        return ApplyResult(success=False, message=f"Apply error: {exc}")


async def _apply_linkedin_easy(job: dict, cover_letter: str, bot, chat_id: int) -> ApplyResult:
    from playwright.async_api import async_playwright
    from apps.zuko.modules.field_classifier import FieldClassifier
    classifier = FieldClassifier()
    safe_fields, confirm_fields, unknown_fields = [], [], []
    cv_path = PROFILE.get("cv_path", os.environ.get("ZUKO_CV_PATH", ""))

    try:
        async with async_playwright() as p:
            browser, ctx = await _make_ctx(p, LINKEDIN_SESSION)
            page = await ctx.new_page()

            await page.goto(job["url"], wait_until="domcontentloaded", timeout=30000)
            await _delay(2, 3)

            if "authwall" in page.url or "/login" in page.url:
                await browser.close()
                return ApplyResult(success=False, message="LinkedIn session expired — run setup_sessions.py")

            stop = await _check_stop(page, job, bot, chat_id)
            if stop:
                await browser.close()
                return ApplyResult(success=False, message=f"Stop: {stop}")

            easy_btn = (
                await page.query_selector('[aria-label*="Easy Apply"]') or
                await page.query_selector('button:has-text("Easy Apply")')
            )
            if not easy_btn:
                await browser.close()
                return ApplyResult(
                    success=False,
                    message="No Easy Apply button — apply manually.",
                    screenshot_path=await _screenshot(page, job["job_id"] + "_no_btn"),
                )

            await easy_btn.click()
            await _delay(2, 3)

            stop = await _check_stop(page, job, bot, chat_id)
            if stop:
                await browser.close()
                return ApplyResult(success=False, message=f"Stop after clicking Easy Apply: {stop}")

            # Multi-step form navigation
            max_steps = 10
            for step in range(max_steps):
                inputs = await page.query_selector_all(
                    'input:not([type="hidden"]):not([type="file"]):not([type="checkbox"]):not([type="radio"]), '
                    'textarea, select'
                )

                for inp in inputs:
                    label = await _get_field_label(page, inp)
                    if not label:
                        continue
                    tier = classifier.classify(label)
                    if tier == "stop":
                        unknown_fields.append(f"{label} — requires review")
                        continue
                    if "cover letter" in label.lower() or "message" in label.lower():
                        try:
                            tag = await inp.evaluate("el => el.tagName.toLowerCase()")
                            if tag == "textarea":
                                await inp.fill(cover_letter[:3000])
                                safe_fields.append("Cover letter")
                                continue
                        except Exception:
                            pass

                    answer = classifier.get_answer(label) if tier in ("safe", "confirm") else None
                    if not answer:
                        answer = next((v for k, v in FIELD_MAP.items() if k in label.lower()), None)
                    if answer:
                        try:
                            tag = await inp.evaluate("el => el.tagName.toLowerCase()")
                            if tag == "select":
                                await inp.select_option(label=answer)
                            else:
                                await inp.fill(answer)
                            if tier == "confirm":
                                confirm_fields.append(f"{label} = {answer}")
                            else:
                                safe_fields.append(label)
                        except Exception:
                            unknown_fields.append(label)
                    else:
                        unknown_fields.append(label)

                # Upload CV if file input present
                if cv_path and Path(cv_path).exists():
                    file_inputs = await page.query_selector_all('input[type="file"]')
                    for fi in file_inputs[:1]:
                        try:
                            await fi.set_input_files(cv_path)
                            safe_fields.append("Resume uploaded")
                        except Exception:
                            pass

                # Check for Next or Submit button
                next_btn = (
                    await page.query_selector('button[aria-label="Continue to next step"]') or
                    await page.query_selector('button:has-text("Next")') or
                    await page.query_selector('button:has-text("Review")')
                )
                submit_btn = (
                    await page.query_selector('button[aria-label="Submit application"]') or
                    await page.query_selector('button:has-text("Submit application")')
                )

                if submit_btn:
                    # At final review — request approval
                    decision = await _pre_submit_review(
                        page, job, safe_fields, confirm_fields, unknown_fields, bot, chat_id
                    )
                    if decision != "approve":
                        await browser.close()
                        return ApplyResult(success=False, message="Application cancelled.")
                    await submit_btn.click()
                    await page.wait_for_load_state("networkidle", timeout=15000)
                    shot = await _screenshot(page, job["job_id"] + "_submitted")
                    await ctx.storage_state(path=LINKEDIN_SESSION)
                    await browser.close()
                    return ApplyResult(
                        success=True,
                        message="Applied via LinkedIn Easy Apply.",
                        screenshot_path=shot,
                        fields_filled=safe_fields + confirm_fields,
                    )
                elif next_btn:
                    await next_btn.click()
                    await _delay(1.5, 3)
                    stop = await _check_stop(page, job, bot, chat_id)
                    if stop:
                        await browser.close()
                        return ApplyResult(success=False, message=f"Stop during form: {stop}")
                else:
                    break

            shot = await _screenshot(page, job["job_id"] + "_incomplete")
            await browser.close()
            return ApplyResult(
                success=False,
                message="Could not complete form — apply manually.",
                screenshot_path=shot,
                fields_filled=safe_fields,
            )

    except Exception as exc:
        log.error("[LinkedIn Apply] %s: %s", job.get("job_id"), exc, exc_info=True)
        return ApplyResult(success=False, message=f"Apply error: {exc}")


async def _apply_generic_ats(job: dict, cover_letter: str, bot, chat_id: int) -> ApplyResult:
    """Generic ATS fallback — opens page, fills what we can, asks for approval."""
    from playwright.async_api import async_playwright
    from apps.zuko.modules.field_classifier import FieldClassifier
    classifier = FieldClassifier()
    safe_fields, confirm_fields, unknown_fields = [], [], []
    cv_path = PROFILE.get("cv_path", os.environ.get("ZUKO_CV_PATH", ""))
    ats_name = _detect_ats(job.get("url", ""))

    try:
        async with async_playwright() as p:
            browser, ctx = await _make_ctx(p)
            page = await ctx.new_page()

            await page.goto(job["url"], wait_until="domcontentloaded", timeout=30000)
            await _delay(2, 3)

            stop = await _check_stop(page, job, bot, chat_id)
            if stop:
                await browser.close()
                return ApplyResult(success=False, message=f"Stop: {stop}")

            # Fill visible form fields
            inputs = await page.query_selector_all(
                'input:not([type="hidden"]):not([type="file"]):not([type="checkbox"]):not([type="radio"]), textarea'
            )
            for inp in inputs:
                label = await _get_field_label(page, inp)
                if not label:
                    continue
                tier = classifier.classify(label)
                if tier == "stop":
                    unknown_fields.append(f"{label} — requires review")
                    continue
                if "cover letter" in label.lower() or "message" in label.lower():
                    try:
                        await inp.fill(cover_letter[:3000])
                        safe_fields.append("Cover letter")
                        continue
                    except Exception:
                        pass
                answer = classifier.get_answer(label) if tier in ("safe", "confirm") else None
                if not answer:
                    answer = next((v for k, v in FIELD_MAP.items() if k in label.lower()), None)
                if answer:
                    try:
                        await inp.fill(answer)
                        if tier == "confirm":
                            confirm_fields.append(f"{label} = {answer}")
                        else:
                            safe_fields.append(label)
                    except Exception:
                        unknown_fields.append(label)
                else:
                    unknown_fields.append(label)

            # Upload CV
            if cv_path and Path(cv_path).exists():
                file_inputs = await page.query_selector_all('input[type="file"]')
                for fi in file_inputs[:1]:
                    try:
                        await fi.set_input_files(cv_path)
                        safe_fields.append("Resume uploaded")
                    except Exception:
                        pass

            # Pre-submission review before submitting
            decision = await _pre_submit_review(
                page, job, safe_fields, confirm_fields, unknown_fields, bot, chat_id
            )
            if decision != "approve":
                await browser.close()
                return ApplyResult(success=False, message="Application cancelled.")

            submit_btn = (
                await page.query_selector('button[type="submit"]') or
                await page.query_selector('input[type="submit"]') or
                await page.query_selector('button:has-text("Submit")')
            )
            if submit_btn:
                await submit_btn.click()
                await page.wait_for_load_state("networkidle", timeout=15000)
                shot = await _screenshot(page, job["job_id"] + "_submitted")
                await browser.close()
                return ApplyResult(
                    success=True,
                    message=f"Applied via {ats_name}.",
                    screenshot_path=shot,
                    fields_filled=safe_fields + confirm_fields,
                )
            else:
                shot = await _screenshot(page, job["job_id"] + "_no_submit")
                await browser.close()
                return ApplyResult(
                    success=False,
                    message=f"No submit button found on {ats_name} — apply manually.",
                    screenshot_path=shot,
                )

    except Exception as exc:
        log.error("[Generic ATS] %s: %s", job.get("job_id"), exc, exc_info=True)
        return ApplyResult(success=False, message=f"Apply error: {exc}")
