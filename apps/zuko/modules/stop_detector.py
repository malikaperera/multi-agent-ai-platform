"""
StopDetector — scans the active Playwright page for conditions requiring human intervention.
Returns a StopCondition or None if clear to proceed.
"""
import logging
from dataclasses import dataclass

from playwright.async_api import Page

log = logging.getLogger(__name__)


@dataclass
class StopCondition:
    reason: str
    detail: str = ""


STOP_PATTERNS: list[tuple[str, str]] = [
    ("captcha",              "CAPTCHA detected"),
    ("i'm not a robot",      "reCAPTCHA detected"),
    ("verify you are human", "Human verification required"),
    ("cloudflare",           "Cloudflare challenge detected"),
    ("hcaptcha",             "hCaptcha detected"),
    ("two-factor",           "MFA/2FA required"),
    ("two factor",           "MFA/2FA required"),
    ("verification code",    "Verification code required"),
    ("authenticator app",    "Authenticator app required"),
    ("enter the code",       "Verification code required"),
    ("complete the assessment", "Assessment required"),
    ("aptitude test",           "Aptitude test required"),
    ("coding challenge",        "Coding challenge required"),
    ("take home test",          "Take-home test required"),
    ("personality test",        "Personality test required"),
    ("video interview",         "Video interview required"),
    ("one-way video",           "One-way video interview required"),
    ("record a video",          "Video recording required"),
    ("hirevue",                 "HireVue video interview required"),
    ("pymetrics",               "Pymetrics assessment required"),
    ("create an account",       "Account creation required"),
    ("sign up to apply",        "Account creation required"),
    ("register to apply",       "Account registration required"),
    ("i declare that",          "Legal declaration requires review"),
    ("i certify that",          "Legal certification requires review"),
    ("police clearance",        "Police clearance required"),
    ("background check",        "Background check declaration"),
    ("security clearance",      "Security clearance required"),
    ("something went wrong",    "Page error detected"),
    ("session expired",         "Session has expired"),
    ("please log in",           "Login required"),
    ("sign in to continue",     "Login required"),
]

STOP_URL_PATTERNS: list[tuple[str, str]] = [
    ("captcha",         "CAPTCHA in URL"),
    ("challenge",       "Challenge page detected"),
    ("checkpoint",      "Account checkpoint"),
    ("verification",    "Verification page"),
    ("assessment",      "Assessment page"),
    ("hirevue",         "HireVue interview"),
]


async def check_page(page: Page) -> StopCondition | None:
    try:
        url = page.url.lower()
        for pattern, reason in STOP_URL_PATTERNS:
            if pattern in url:
                return StopCondition(reason=reason, detail=f"URL: {url}")

        body_text = await page.evaluate("() => document.body?.innerText?.toLowerCase() || ''")
        for pattern, reason in STOP_PATTERNS:
            if pattern in body_text:
                return StopCondition(reason=reason, detail=f"Pattern: '{pattern}'")

        captcha_frames = await page.query_selector_all(
            'iframe[src*="hcaptcha"], iframe[src*="recaptcha"], iframe[title*="captcha"]'
        )
        if captcha_frames:
            return StopCondition(reason="CAPTCHA iframe detected", detail="hCaptcha or reCAPTCHA iframe found")

        return None
    except Exception as exc:
        log.debug("[StopDetector] check_page error: %s", exc)
        return None


async def check_for_assessment_page(page: Page) -> StopCondition | None:
    try:
        url = page.url.lower()
        assessment_domains = [
            "hirevue.com", "pymetrics.ai", "codility.com", "hackerrank.com",
            "testgorilla.com", "sova.ai", "talentplus.com", "criteriacorp.com",
        ]
        for domain in assessment_domains:
            if domain in url:
                return StopCondition(reason=f"Redirected to assessment: {domain}", detail=url)
        return None
    except Exception as exc:
        log.debug("[StopDetector] check_for_assessment error: %s", exc)
        return None
