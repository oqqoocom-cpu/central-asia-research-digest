"""Runtime configuration shared by the digest CLI and library imports."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Sequence


def _bool(value: object, default: bool) -> bool:
    if value is None:
        return default
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return default


def _float(value: object, default: float, minimum: float = 0.0) -> float:
    try:
        return max(minimum, float(value))
    except (TypeError, ValueError):
        return default


def _date(value: str | None) -> dt.date:
    if not value:
        return dt.date.today()
    return dt.date.fromisoformat(value)


def _load_json(path: Path | None) -> dict:
    if not path or not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="digest_generator.py",
        description="Generate the Central Asia Research Daily Digest.",
    )
    parser.add_argument("--date", help="Run date in YYYY-MM-DD format.")
    parser.add_argument("--output-dir", help="Directory for reports and runtime state.")
    parser.add_argument("--config", help="Optional JSON runtime configuration file.")
    parser.add_argument(
        "--replay",
        action="store_true",
        help="Reproduce the saved render for the selected date without network access.",
    )
    parser.add_argument("--full", action="store_true", help="Disable stable-mode source skips.")
    parser.add_argument(
        "--no-candidates",
        action="store_true",
        help="Skip experimental candidate web sources.",
    )
    parser.add_argument("--no-translation", action="store_true", help="Disable machine translation.")
    parser.add_argument(
        "--allow-insecure-tls",
        action="store_true",
        help="Disable TLS certificate verification. Use only for diagnostics.",
    )
    parser.add_argument(
        "--enable-gdelt",
        action="store_true",
        help="Enable the optional rate-limited GDELT discovery adapter.",
    )
    return parser


@dataclass(frozen=True)
class RuntimeSettings:
    run_date: dt.date
    output_dir: Path | None
    replay: bool
    stable_mode: bool
    test_candidate_sources: bool
    translation_enabled: bool
    verify_tls: bool
    enable_gdelt: bool
    min_host_interval: float
    request_timeout: float
    user_agent: str
    openalex_api_key: str
    crossref_mailto: str

    @classmethod
    def from_process(
        cls,
        argv: Sequence[str] | None = None,
        environ: Mapping[str, str] | None = None,
    ) -> "RuntimeSettings":
        env = dict(os.environ if environ is None else environ)
        args, _ = _parser().parse_known_args(list(argv) if argv is not None else None)
        config_path_text = args.config or env.get("DIGEST_CONFIG", "")
        config = _load_json(Path(config_path_text).expanduser()) if config_path_text else {}

        date_text = args.date or env.get("DIGEST_DATE") or config.get("date")
        output_text = args.output_dir or env.get("DIGEST_OUTPUT_DIR") or config.get("output_dir")
        output_dir = Path(output_text).expanduser().resolve() if output_text else None

        stable_mode = not args.full and _bool(
            env.get("DIGEST_STABLE_MODE", config.get("stable_mode")), True
        )
        candidate_sources = not args.no_candidates and _bool(
            env.get("DIGEST_CANDIDATE_SOURCES", config.get("candidate_sources")), True
        )
        translation_enabled = not args.no_translation and _bool(
            env.get("DIGEST_TRANSLATION", config.get("translation")), True
        )
        verify_tls = not args.allow_insecure_tls and _bool(
            env.get("DIGEST_VERIFY_TLS", config.get("verify_tls")), True
        )
        enable_gdelt = args.enable_gdelt or _bool(
            env.get("DIGEST_ENABLE_GDELT", config.get("enable_gdelt")), False
        )

        crossref_mailto = str(
            env.get("CROSSREF_MAILTO", config.get("crossref_mailto", ""))
        ).strip()
        default_agent = (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 Chrome/127 Safari/537.36 "
            "CentralAsiaResearchDigest/2.0"
        )
        if crossref_mailto:
            default_agent += " (mailto:" + crossref_mailto + ")"

        return cls(
            run_date=_date(str(date_text) if date_text else None),
            output_dir=output_dir,
            replay=bool(args.replay or _bool(env.get("DIGEST_REPLAY"), False)),
            stable_mode=stable_mode,
            test_candidate_sources=candidate_sources,
            translation_enabled=translation_enabled,
            verify_tls=verify_tls,
            enable_gdelt=enable_gdelt,
            min_host_interval=_float(
                env.get("DIGEST_MIN_HOST_INTERVAL", config.get("min_host_interval")),
                0.15,
            ),
            request_timeout=_float(
                env.get("DIGEST_REQUEST_TIMEOUT", config.get("request_timeout")),
                12.0,
                minimum=1.0,
            ),
            user_agent=str(env.get("DIGEST_USER_AGENT", config.get("user_agent", default_agent))).strip(),
            openalex_api_key=str(
                env.get("OPENALEX_API_KEY", config.get("openalex_api_key", ""))
            ).strip(),
            crossref_mailto=crossref_mailto,
        )
