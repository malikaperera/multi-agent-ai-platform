"""
HTML message formatting helpers for Telegram (parse_mode=HTML).
All public functions return strings safe for Telegram HTML.
"""
import html as _html


def escape(text: str) -> str:
    return _html.escape(str(text))


def bold(text: str) -> str:
    return f"<b>{escape(text)}</b>"


def italic(text: str) -> str:
    return f"<i>{escape(text)}</i>"


def code(text: str) -> str:
    return f"<code>{escape(text)}</code>"


def pre(text: str) -> str:
    return f"<pre>{escape(text)}</pre>"


def section(title: str, body: str) -> str:
    """Bold title followed by body (body may contain HTML)."""
    return f"<b>{escape(title)}</b>\n{body}"


def bullet_list(items: list[str]) -> str:
    return "\n".join(f"• {escape(item)}" for item in items)


def kv(label: str, value: str) -> str:
    return f"<b>{escape(label)}:</b> {escape(value)}"


def divider() -> str:
    return "─" * 24


def split_message(text: str, max_length: int = 4000) -> list[str]:
    """Split long messages into Telegram-safe chunks."""
    if len(text) <= max_length:
        return [text]
    chunks = []
    while text:
        chunks.append(text[:max_length])
        text = text[max_length:]
    return chunks
