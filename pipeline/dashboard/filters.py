"""
Jinja2 custom formatting filters for the formal commodity dashboard.
"""

from __future__ import annotations

from datetime import date
import re
from typing import Any

from markupsafe import Markup


def currency_filter(val: float | int | None) -> str:
    """Formats numeric value as Philippine Peso currency (₱xx.xx)."""
    if val is None:
        return "—"
    try:
        num = float(val)
        return f"₱{num:,.2f}"
    except (ValueError, TypeError):
        return "—"


def percent_filter(val: float | int | None, show_sign: bool = True) -> str:
    """Formats numeric value as percentage (+xx.xx% or xx.xx%)."""
    if val is None:
        return "—"
    try:
        num = float(val)
        sign = "+" if show_sign and num > 0 else ""
        return f"{sign}{num:.2f}%"
    except (ValueError, TypeError):
        return "—"


def change_badge_filter(val: float | int | None, is_pct: bool = True) -> Markup:
    """
    Renders a formal financial badge with directional arrow and indicator colors.
    """
    if val is None:
        return Markup('<span class="badge badge-neutral">—</span>')
    try:
        num = float(val)
    except (ValueError, TypeError):
        return Markup('<span class="badge badge-neutral">—</span>')

    if num > 0.0001:
        arrow, cls, sign = "▲", "badge-increase", "+"
    elif num < -0.0001:
        arrow, cls, sign = "▼", "badge-decrease", ""
    else:
        arrow, cls, sign = "—", "badge-neutral", ""

    formatted = f"{sign}{num:.2f}%" if is_pct else f"{sign}₱{num:,.2f}"
    if abs(num) <= 0.0001:
        formatted = "0.00%" if is_pct else "₱0.00"

    html = f'<span class="badge {cls}"><span class="badge-arrow">{arrow}</span> {formatted}</span>'
    return Markup(html)


def format_date_filter(val: Any) -> str:
    """Formats ISO date string or date object into formal publication format."""
    if val is None:
        return "—"
    if hasattr(val, "strftime"):
        return val.strftime("%B %d, %Y")
    try:
        d = date.fromisoformat(str(val)[:10])
        return d.strftime("%B %d, %Y")
    except Exception:
        return str(val)


def format_num_filter(val: float | int | None, decimals: int = 0) -> str:
    """Formats numbers with comma thousand separators."""
    if val is None:
        return "—"
    try:
        num = float(val)
        return f"{num:,.{decimals}f}" if decimals > 0 else f"{int(round(num)):,}"
    except (ValueError, TypeError):
        return "—"


def slugify_filter(val: Any) -> str:
    """Converts a commodity or category name into a clean URL-safe slug."""
    if val is None:
        return ""
    text = str(val).lower().strip()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    return text.strip("-")
