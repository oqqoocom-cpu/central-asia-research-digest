"""Main-text and PDF extraction with bounded resource use."""

from __future__ import annotations

import io
import re

try:
    import trafilatura
except ImportError:  # pragma: no cover - fallback is exercised in the main script.
    trafilatura = None

try:
    from pypdf import PdfReader
except ImportError:  # pragma: no cover
    PdfReader = None


def extract_main_text(html: str, url: str = "") -> str:
    if not html or trafilatura is None:
        return ""
    try:
        text = trafilatura.extract(
            html,
            url=url or None,
            include_comments=False,
            include_tables=False,
            include_links=False,
            favor_precision=True,
            deduplicate=True,
            output_format="txt",
        )
    except Exception:
        return ""
    return re.sub(r"[ \t]+", " ", text or "").strip()


def extract_pdf_text(data: bytes, *, max_pages: int = 12, max_bytes: int = 20_000_000) -> str:
    if not data or len(data) > max_bytes or PdfReader is None:
        return ""
    try:
        reader = PdfReader(io.BytesIO(data), strict=False)
        pages = []
        for page in reader.pages[:max_pages]:
            text = page.extract_text() or ""
            if text:
                pages.append(text)
        return re.sub(r"[ \t]+", " ", "\n".join(pages)).strip()
    except Exception:
        return ""


def excerpt(text: str, max_chars: int = 500) -> str:
    text = re.sub(r"\s+", " ", text or "").strip()
    if len(text) <= max_chars:
        return text
    boundary = max(text.rfind(". ", 0, max_chars), text.rfind("。", 0, max_chars))
    if boundary >= max_chars // 2:
        return text[: boundary + 1].strip()
    return text[:max_chars].rstrip(" ,;，；") + "..."
