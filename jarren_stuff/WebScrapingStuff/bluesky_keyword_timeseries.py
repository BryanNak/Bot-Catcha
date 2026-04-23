#!/usr/bin/env python3
"""
Collect publicly accessible Bluesky posts matching a keyword over a time range.

Official API strategy
---------------------
- Public read-only mode uses the official Bluesky AppView endpoint:
  https://public.api.bsky.app/xrpc/app.bsky.feed.searchPosts
- Optional authenticated mode uses the official session endpoint:
  /xrpc/com.atproto.server.createSession
  and then sends search requests through the authenticated service host.

Important limitations
---------------------
- This script intentionally uses Bluesky's official API only.
- Official search is useful for practical collection, but it does not guarantee
  exhaustive historical recall for arbitrary long date ranges.
- The app.bsky.feed.searchPosts lexicon notes that cursor pagination "may not
  necessarily allow scrolling through entire result set."
- Server-side "since" and "until" search filters are based on a search/sort
  timestamp ("sortAt"), which may not match the post record's "createdAt".

To stay cautious, this script:
- uses only documented official parameters,
- paginates with official cursors,
- rate-limits and retries politely,
- chunks long date ranges into smaller official search windows,
- applies the final date-range filter client-side using record.createdAt, and
- deduplicates on the stable AT-URI before writing output.
"""

from __future__ import annotations

import argparse
import concurrent.futures as futures
import csv
import getpass
import json
import logging
import os
import re
import sys
import threading
import time
from dataclasses import asdict, dataclass
from datetime import date, datetime, time as dt_time, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple
from urllib.parse import quote

import requests


LOGGER = logging.getLogger("bluesky_keyword_timeseries")

PUBLIC_APPVIEW_BASE_URL = "https://public.api.bsky.app"
DEFAULT_AUTH_SERVICE = "https://bsky.social"
SEARCH_POSTS_PATH = "/xrpc/app.bsky.feed.searchPosts"
CREATE_SESSION_PATH = "/xrpc/com.atproto.server.createSession"
REFRESH_SESSION_PATH = "/xrpc/com.atproto.server.refreshSession"

REQUEST_HEADERS = {
    "Accept": "application/json",
    "User-Agent": (
        "bluesky-keyword-timeseries/1.0 "
        "(official-api-only; respectful-rate-limited research collection)"
    ),
}

RETRYABLE_STATUS_CODES = {408, 409, 425, 429, 500, 502, 503, 504}
OUTPUT_FIELD_ORDER = [
    "keyword",
    "post_uri",
    "cid",
    "author_did",
    "author_handle",
    "post_text",
    "created_at_raw",
    "created_at_iso",
    "indexed_at",
    "reply_parent_uri",
    "reply_root_uri",
    "like_count",
    "repost_count",
    "reply_count",
    "quote_count",
    "source_url",
]


class BlueskyRequestError(RuntimeError):
    """Raised when an HTTP request fails in a non-recoverable way."""


class BlueskyCursorPaginationForbidden(BlueskyRequestError):
    """Raised for the known public AppView search cursor 403 behavior."""


@dataclass(frozen=True)
class NormalizedPostRecord:
    keyword: str
    post_uri: str
    cid: str
    author_did: str
    author_handle: str
    post_text: str
    created_at_raw: str
    created_at_iso: str
    indexed_at: str
    reply_parent_uri: str
    reply_root_uri: str
    like_count: Optional[int]
    repost_count: Optional[int]
    reply_count: Optional[int]
    quote_count: Optional[int]
    source_url: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Collect Bluesky posts matching a keyword over a UTC date range using "
            "Bluesky's official API only."
        )
    )
    parser.add_argument("--keyword", required=True, help="Keyword or query string to search for.")
    parser.add_argument(
        "--start-date",
        required=True,
        help="Inclusive start date in YYYY-MM-DD format (interpreted as UTC).",
    )
    parser.add_argument(
        "--end-date",
        required=True,
        help="Inclusive end date in YYYY-MM-DD format (interpreted as UTC).",
    )
    parser.add_argument(
        "--output-path",
        required=True,
        help="Path to the output file to create.",
    )
    parser.add_argument(
        "--output-format",
        required=True,
        choices=("csv", "json"),
        help="Output format.",
    )
    parser.add_argument(
        "--max-results",
        type=int,
        default=None,
        help="Optional cap on unique kept records after deduplication.",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=100,
        help="Search page size per request. Official maximum is 100. Default: 100.",
    )
    parser.add_argument(
        "--lang",
        default=None,
        help="Optional single language code supported by the official search API, e.g. en.",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=1.0,
        help="Minimum sleep between HTTP requests. Default: 1.0.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=30.0,
        help="Per-request timeout in seconds. Default: 30.0.",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=5,
        help="Maximum retry attempts for retryable failures. Default: 5.",
    )
    parser.add_argument(
        "--backoff-base-seconds",
        type=float,
        default=2.0,
        help="Base exponential backoff in seconds. Default: 2.0.",
    )
    parser.add_argument(
        "--window-days",
        type=int,
        default=7,
        help=(
            "Chunk the requested date range into this many days per search window. "
            "Smaller windows can improve practical coverage on dense queries. Default: 7."
        ),
    )
    parser.add_argument(
        "--parallel-workers",
        type=int,
        default=1,
        help=(
            "Number of independent date windows to collect concurrently. Cursor pages "
            "inside each window remain sequential. Default: 1."
        ),
    )
    parser.add_argument(
        "--authenticated",
        action="store_true",
        help=(
            "Optional authenticated mode. Default behavior is public read-only mode. "
            "Authenticated mode can be useful when a service requires auth."
        ),
    )
    parser.add_argument(
        "--auth-identifier",
        default=None,
        help="Bluesky handle or identifier for authenticated mode.",
    )
    parser.add_argument(
        "--auth-password",
        default=None,
        help=(
            "Password or app password for authenticated mode. Prefer supplying via "
            "BLUESKY_APP_PASSWORD or interactive prompt instead of shell history."
        ),
    )
    parser.add_argument(
        "--auth-service",
        default=DEFAULT_AUTH_SERVICE,
        help=(
            "Authenticated service base URL. Default: https://bsky.social. "
            "If your account is hosted elsewhere, set the appropriate service/PDS host."
        ),
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose debug logging.",
    )
    return parser.parse_args(argv)


def setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(message)s")


def parse_iso_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"Invalid date '{value}'. Expected YYYY-MM-DD."
        ) from exc


def utc_date_bounds(start_date: date, end_date: date) -> Tuple[datetime, datetime]:
    start_dt = datetime.combine(start_date, dt_time.min, tzinfo=timezone.utc)
    end_exclusive = datetime.combine(
        end_date + timedelta(days=1),
        dt_time.min,
        tzinfo=timezone.utc,
    )
    return start_dt, end_exclusive


def validate_args(args: argparse.Namespace) -> None:
    if args.page_size < 1 or args.page_size > 100:
        raise ValueError("--page-size must be between 1 and 100.")
    if args.max_results is not None and args.max_results < 1:
        raise ValueError("--max-results must be a positive integer.")
    if args.sleep_seconds < 0:
        raise ValueError("--sleep-seconds must be zero or greater.")
    if args.timeout_seconds <= 0:
        raise ValueError("--timeout-seconds must be greater than zero.")
    if args.max_retries < 1:
        raise ValueError("--max-retries must be at least 1.")
    if args.backoff_base_seconds < 0:
        raise ValueError("--backoff-base-seconds must be zero or greater.")
    if args.window_days < 1:
        raise ValueError("--window-days must be at least 1.")
    if args.parallel_workers < 1:
        raise ValueError("--parallel-workers must be at least 1.")

    start_date = parse_iso_date(args.start_date)
    end_date = parse_iso_date(args.end_date)
    if end_date < start_date:
        raise ValueError(
            f"Invalid date range: end date {args.end_date} is before start date {args.start_date}."
        )

    if args.authenticated and not args.auth_identifier and not os.getenv("BLUESKY_HANDLE"):
        raise ValueError(
            "--authenticated requires --auth-identifier or BLUESKY_HANDLE."
        )


def normalize_whitespace(value: Optional[str]) -> str:
    if not value:
        return ""

    text = str(value).replace("\u00a0", " ")
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t\f\v]+", " ", text)
    text = "\n".join(line.strip() for line in text.splitlines())
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def safe_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def safe_int(value: Any) -> Optional[int]:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def parse_bsky_datetime(value: Any) -> Optional[datetime]:
    text = safe_str(value)
    if not text:
        return None

    candidate = text
    if candidate.endswith("Z"):
        candidate = candidate[:-1] + "+00:00"

    try:
        dt = datetime.fromisoformat(candidate)
    except ValueError:
        return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def to_iso8601_z(dt: Optional[datetime]) -> str:
    if dt is None:
        return ""
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def iter_windows(start_dt: datetime, end_exclusive: datetime, window_days: int) -> Iterator[Tuple[datetime, datetime]]:
    cursor = start_dt
    step = timedelta(days=window_days)
    while cursor < end_exclusive:
        window_end = min(cursor + step, end_exclusive)
        yield cursor, window_end
        cursor = window_end


def build_source_url(post_uri: str, author_did: str, author_handle: str) -> str:
    parts = post_uri.split("/")
    if len(parts) < 5 or parts[0] != "at:" or parts[3] != "app.bsky.feed.post":
        return ""

    # Prefer the handle for a more human-readable Bluesky web URL, but fall back
    # to DID/AT-URI components when the handle is missing.
    profile_id = author_handle or author_did or parts[2]
    record_key = parts[4]
    quoted_profile = quote(profile_id, safe=":@._-")
    quoted_record_key = quote(record_key, safe=":@._-")
    return f"https://bsky.app/profile/{quoted_profile}/post/{quoted_record_key}"


def extract_reply_uri(reply_obj: Any, which: str) -> str:
    if not isinstance(reply_obj, dict):
        return ""
    target = reply_obj.get(which)
    if not isinstance(target, dict):
        return ""
    return safe_str(target.get("uri"))


def normalize_post(keyword: str, post_view: Dict[str, Any]) -> Optional[NormalizedPostRecord]:
    post_uri = safe_str(post_view.get("uri"))
    if not post_uri:
        return None

    author = post_view.get("author")
    author_obj = author if isinstance(author, dict) else {}
    record = post_view.get("record")
    record_obj = record if isinstance(record, dict) else {}
    reply_obj = record_obj.get("reply")

    created_at_raw = safe_str(record_obj.get("createdAt"))
    created_at_iso = to_iso8601_z(parse_bsky_datetime(created_at_raw))
    indexed_at_raw = safe_str(post_view.get("indexedAt"))
    indexed_at_iso = to_iso8601_z(parse_bsky_datetime(indexed_at_raw)) or indexed_at_raw

    author_did = safe_str(author_obj.get("did"))
    author_handle = safe_str(author_obj.get("handle"))

    return NormalizedPostRecord(
        keyword=keyword,
        post_uri=post_uri,
        cid=safe_str(post_view.get("cid")),
        author_did=author_did,
        author_handle=author_handle,
        post_text=normalize_whitespace(record_obj.get("text")),
        created_at_raw=created_at_raw,
        created_at_iso=created_at_iso,
        indexed_at=indexed_at_iso,
        reply_parent_uri=extract_reply_uri(reply_obj, "parent"),
        reply_root_uri=extract_reply_uri(reply_obj, "root"),
        like_count=safe_int(post_view.get("likeCount")),
        repost_count=safe_int(post_view.get("repostCount")),
        reply_count=safe_int(post_view.get("replyCount")),
        quote_count=safe_int(post_view.get("quoteCount")),
        source_url=build_source_url(
            post_uri=post_uri,
            author_did=author_did,
            author_handle=author_handle,
        ),
    )


def count_populated_fields(record: NormalizedPostRecord) -> int:
    return sum(
        1
        for value in record.to_dict().values()
        if value not in ("", None)
    )


def choose_preferred_record(
    existing: NormalizedPostRecord,
    incoming: NormalizedPostRecord,
) -> NormalizedPostRecord:
    existing_indexed = parse_bsky_datetime(existing.indexed_at)
    incoming_indexed = parse_bsky_datetime(incoming.indexed_at)

    if existing_indexed and incoming_indexed:
        if incoming_indexed > existing_indexed:
            return incoming
        if existing_indexed > incoming_indexed:
            return existing
    elif incoming_indexed and not existing_indexed:
        return incoming
    elif existing_indexed and not incoming_indexed:
        return existing

    if len(incoming.post_text) > len(existing.post_text):
        return incoming
    if len(existing.post_text) > len(incoming.post_text):
        return existing

    if count_populated_fields(incoming) > count_populated_fields(existing):
        return incoming
    return existing


@dataclass(frozen=True)
class WindowCollectionResult:
    records: Dict[str, NormalizedPostRecord]
    pages: int
    received: int


class PoliteRateLimiter:
    """Thread-safe global request spacing across all worker clients."""

    def __init__(self, sleep_seconds: float) -> None:
        self.sleep_seconds = sleep_seconds
        self._last_request_monotonic = 0.0
        self._lock = threading.Lock()

    def wait(self) -> None:
        if self.sleep_seconds <= 0:
            return

        with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_request_monotonic
            if elapsed < self.sleep_seconds:
                sleep_for = self.sleep_seconds - elapsed
                LOGGER.debug("Global rate limiter sleeping %.2fs", sleep_for)
                time.sleep(sleep_for)
                now = time.monotonic()
            self._last_request_monotonic = now


class BlueskyAPIClient:
    """Small API client with polite rate limiting and retry/backoff."""

    def __init__(
        self,
        base_url: str,
        timeout_seconds: float,
        sleep_seconds: float,
        max_retries: int,
        backoff_base_seconds: float,
        auth_service: Optional[str] = None,
        rate_limiter: Optional[PoliteRateLimiter] = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.auth_service = auth_service.rstrip("/") if auth_service else None
        self.timeout_seconds = timeout_seconds
        self.sleep_seconds = sleep_seconds
        self.max_retries = max_retries
        self.backoff_base_seconds = backoff_base_seconds
        self.rate_limiter = rate_limiter
        self._last_request_monotonic = 0.0
        self._access_jwt: Optional[str] = None
        self._refresh_jwt: Optional[str] = None

        self.session = requests.Session()
        self.session.headers.update(REQUEST_HEADERS)

    def clone_for_worker(self) -> "BlueskyAPIClient":
        """Create a per-thread client while sharing the global rate limiter."""
        client = BlueskyAPIClient(
            base_url=self.base_url,
            timeout_seconds=self.timeout_seconds,
            sleep_seconds=self.sleep_seconds,
            max_retries=self.max_retries,
            backoff_base_seconds=self.backoff_base_seconds,
            auth_service=self.auth_service,
            rate_limiter=self.rate_limiter,
        )
        client._access_jwt = self._access_jwt
        client._refresh_jwt = self._refresh_jwt
        if self._access_jwt:
            client.session.headers["Authorization"] = f"Bearer {self._access_jwt}"
        return client

    def authenticate(self, identifier: str, password: str) -> None:
        if not self.auth_service:
            raise BlueskyRequestError("Authenticated mode requires an auth service URL.")

        payload = {"identifier": identifier, "password": password}
        url = f"{self.auth_service}{CREATE_SESSION_PATH}"
        LOGGER.info("Creating authenticated session via official endpoint %s", url)
        data = self._request_json("POST", url, json_body=payload, allow_refresh=False)

        access_jwt = safe_str(data.get("accessJwt"))
        refresh_jwt = safe_str(data.get("refreshJwt"))
        if not access_jwt:
            raise BlueskyRequestError("Authenticated session did not return accessJwt.")

        self._access_jwt = access_jwt
        self._refresh_jwt = refresh_jwt or None
        self.session.headers["Authorization"] = f"Bearer {self._access_jwt}"

        # Authenticated app requests are typically made through the service/PDS host.
        self.base_url = self.auth_service

    def search_posts(self, params: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.base_url}{SEARCH_POSTS_PATH}"
        data = self._request_json("GET", url, params=params)
        if not isinstance(data, dict):
            raise BlueskyRequestError("Unexpected response shape: expected a JSON object.")
        return data

    def _is_known_search_cursor_forbidden(
        self,
        url: str,
        params: Optional[Dict[str, Any]],
        response: requests.Response,
    ) -> bool:
        if response.status_code != 403:
            return False
        if not url.endswith(SEARCH_POSTS_PATH):
            return False
        if not params or "cursor" not in params:
            return False
        if self.session.headers.get("Authorization"):
            return False
        return self.base_url.rstrip("/") in {
            "https://public.api.bsky.app",
            "https://api.bsky.app",
        }

    def _sleep_for_rate_limit(self) -> None:
        if self.rate_limiter:
            self.rate_limiter.wait()
            return

        if self.sleep_seconds <= 0:
            return

        now = time.monotonic()
        elapsed = now - self._last_request_monotonic
        if elapsed < self.sleep_seconds:
            sleep_for = self.sleep_seconds - elapsed
            LOGGER.debug("Sleeping %.2fs before next request", sleep_for)
            time.sleep(sleep_for)

    def _refresh_session(self) -> bool:
        if not self.auth_service or not self._refresh_jwt:
            return False

        headers = {"Authorization": f"Bearer {self._refresh_jwt}"}
        url = f"{self.auth_service}{REFRESH_SESSION_PATH}"
        LOGGER.info("Refreshing authenticated session")

        try:
            data = self._request_json(
                "POST",
                url,
                headers=headers,
                allow_refresh=False,
            )
        except BlueskyRequestError as exc:
            LOGGER.warning("Failed to refresh session: %s", exc)
            return False

        access_jwt = safe_str(data.get("accessJwt"))
        refresh_jwt = safe_str(data.get("refreshJwt"))
        if not access_jwt:
            return False

        self._access_jwt = access_jwt
        self._refresh_jwt = refresh_jwt or self._refresh_jwt
        self.session.headers["Authorization"] = f"Bearer {self._access_jwt}"
        return True

    def _request_json(
        self,
        method: str,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        allow_refresh: bool = True,
    ) -> Any:
        last_error: Optional[Exception] = None
        refreshed_once = False

        for attempt in range(1, self.max_retries + 1):
            self._sleep_for_rate_limit()
            LOGGER.debug("HTTP %s %s params=%s", method, url, params)

            try:
                response = self.session.request(
                    method=method,
                    url=url,
                    params=params,
                    json=json_body,
                    headers=headers,
                    timeout=self.timeout_seconds,
                )
                self._last_request_monotonic = time.monotonic()

                if (
                    response.status_code == 401
                    and allow_refresh
                    and self._refresh_jwt
                    and not refreshed_once
                ):
                    refreshed_once = self._refresh_session()
                    if refreshed_once:
                        LOGGER.info("Retrying request once after session refresh")
                        continue

                if response.status_code in RETRYABLE_STATUS_CODES:
                    raise requests.HTTPError(
                        f"Retryable HTTP {response.status_code}",
                        response=response,
                    )

                if self._is_known_search_cursor_forbidden(url, params, response):
                    preview = response.text[:300].strip().replace("\n", " ")
                    raise BlueskyCursorPaginationForbidden(
                        "Public Bluesky AppView search returned HTTP 403 when using the "
                        "'cursor' parameter. This appears to be a known issue with "
                        "app.bsky.feed.searchPosts pagination on public AppView hosts. "
                        "Try --authenticated, narrow the query/date window so one page is "
                        f"enough, or retry later. Response preview: {preview}"
                    )

                if response.status_code >= 400:
                    preview = response.text[:300].strip().replace("\n", " ")
                    raise BlueskyRequestError(
                        f"HTTP {response.status_code} for {url}. Response preview: {preview}"
                    )

                content_type = response.headers.get("Content-Type", "")
                if "json" not in content_type.lower():
                    preview = response.text[:200].strip().replace("\n", " ")
                    raise BlueskyRequestError(
                        "Expected JSON but got non-JSON content from official endpoint. "
                        f"Content-Type={content_type!r}; preview={preview!r}"
                    )

                return response.json()
            except (requests.Timeout, requests.ConnectionError, requests.HTTPError, ValueError, BlueskyRequestError) as exc:
                last_error = exc

                retryable = True
                retry_after_seconds: Optional[float] = None

                if isinstance(exc, BlueskyCursorPaginationForbidden):
                    retryable = False
                elif isinstance(exc, BlueskyRequestError):
                    retryable = False

                if isinstance(exc, requests.HTTPError):
                    response = exc.response
                    status_code = response.status_code if response is not None else None
                    retryable = status_code in RETRYABLE_STATUS_CODES
                    if response is not None:
                        retry_after_header = response.headers.get("Retry-After")
                        if retry_after_header:
                            try:
                                retry_after_seconds = float(retry_after_header)
                            except ValueError:
                                retry_after_seconds = None

                if attempt >= self.max_retries or not retryable:
                    break

                backoff = retry_after_seconds
                if backoff is None:
                    backoff = self.backoff_base_seconds * (2 ** (attempt - 1))

                LOGGER.warning(
                    "Request attempt %s/%s failed for %s. Backing off %.1fs. Error: %s",
                    attempt,
                    self.max_retries,
                    url,
                    backoff,
                    exc,
                )
                time.sleep(backoff)

        raise BlueskyRequestError(f"Request failed for {url}: {last_error}") from last_error


class BlueskyKeywordCollector:
    def __init__(self, client: BlueskyAPIClient) -> None:
        self.client = client

    def collect(
        self,
        keyword: str,
        start_dt: datetime,
        end_exclusive: datetime,
        page_size: int,
        lang: Optional[str],
        window_days: int,
        max_results: Optional[int],
        parallel_workers: int,
    ) -> List[NormalizedPostRecord]:
        windows = list(iter_windows(start_dt, end_exclusive, window_days))
        deduped: Dict[str, NormalizedPostRecord] = {}
        total_pages = 0
        total_received = 0
        stop_event = threading.Event()

        if parallel_workers <= 1 or len(windows) <= 1:
            for window_start, window_end in windows:
                remaining = None
                if max_results is not None:
                    remaining = max(max_results - len(deduped), 0)
                    if remaining <= 0:
                        break

                result = self._collect_window(
                    client=self.client,
                    keyword=keyword,
                    start_dt=start_dt,
                    end_exclusive=end_exclusive,
                    window_start=window_start,
                    window_end=window_end,
                    page_size=page_size,
                    lang=lang,
                    stop_event=stop_event,
                    local_max_results=remaining,
                )
                total_pages += result.pages
                total_received += result.received
                self._merge_records(deduped, result.records)

                if max_results is not None and len(deduped) >= max_results:
                    LOGGER.info("Reached max-results=%s; stopping collection.", max_results)
                    break
        else:
            worker_count = min(parallel_workers, len(windows))
            LOGGER.info(
                "Collecting %s search windows with %s parallel workers. "
                "Cursor pages inside each window remain sequential.",
                len(windows),
                worker_count,
            )
            executor = futures.ThreadPoolExecutor(max_workers=worker_count)
            future_to_window: Dict[futures.Future[WindowCollectionResult], Tuple[datetime, datetime]] = {}

            try:
                for window_start, window_end in windows:
                    worker_client = self.client.clone_for_worker()
                    future = executor.submit(
                        self._collect_window,
                        worker_client,
                        keyword,
                        start_dt,
                        end_exclusive,
                        window_start,
                        window_end,
                        page_size,
                        lang,
                        stop_event,
                        max_results,
                    )
                    future_to_window[future] = (window_start, window_end)

                for future in futures.as_completed(future_to_window):
                    window_start, window_end = future_to_window[future]
                    try:
                        result = future.result()
                    except Exception:
                        stop_event.set()
                        for pending in future_to_window:
                            pending.cancel()
                        LOGGER.error(
                            "Parallel collection failed for window %s -> %s",
                            to_iso8601_z(window_start),
                            to_iso8601_z(window_end),
                        )
                        raise

                    total_pages += result.pages
                    total_received += result.received
                    self._merge_records(deduped, result.records)

                    if max_results is not None and len(deduped) >= max_results:
                        LOGGER.info(
                            "Reached max-results=%s; asking workers to stop after their current page.",
                            max_results,
                        )
                        stop_event.set()
                        for pending in future_to_window:
                            pending.cancel()
                        break
            finally:
                executor.shutdown(wait=True, cancel_futures=True)

        LOGGER.info(
            "Collection finished. Pages=%s total_received=%s unique_kept=%s",
            total_pages,
            total_received,
            len(deduped),
        )
        return self._finalize_records(deduped, max_results)

    @staticmethod
    def _merge_records(
        target: Dict[str, NormalizedPostRecord],
        incoming: Dict[str, NormalizedPostRecord],
    ) -> None:
        for post_uri, record in incoming.items():
            existing = target.get(post_uri)
            if existing is None:
                target[post_uri] = record
            else:
                target[post_uri] = choose_preferred_record(existing, record)

    @staticmethod
    def _collect_window(
        client: BlueskyAPIClient,
        keyword: str,
        start_dt: datetime,
        end_exclusive: datetime,
        window_start: datetime,
        window_end: datetime,
        page_size: int,
        lang: Optional[str],
        stop_event: threading.Event,
        local_max_results: Optional[int],
    ) -> WindowCollectionResult:
        window_label = (
            f"{to_iso8601_z(window_start)} -> "
            f"{to_iso8601_z(window_end - timedelta(microseconds=1))}"
        )
        cursor: Optional[str] = None
        seen_cursors: set[str] = set()
        page_number = 0
        received = 0
        deduped: Dict[str, NormalizedPostRecord] = {}

        LOGGER.info("Collecting search window %s", window_label)

        while not stop_event.is_set():
            params: Dict[str, Any] = {
                "q": keyword,
                "sort": "latest",
                "since": to_iso8601_z(window_start),
                "until": to_iso8601_z(window_end),
                "limit": page_size,
            }
            if lang:
                params["lang"] = lang
            if cursor:
                params["cursor"] = cursor

            payload = client.search_posts(params)
            posts = payload.get("posts")
            next_cursor = safe_str(payload.get("cursor")) or None
            hits_total = payload.get("hitsTotal")

            if not isinstance(posts, list):
                raise BlueskyRequestError("Unexpected response shape: 'posts' is not a list.")

            page_number += 1
            received += len(posts)
            kept_in_page = 0
            skipped_missing_created = 0

            for item in posts:
                if not isinstance(item, dict):
                    continue

                normalized = normalize_post(keyword=keyword, post_view=item)
                if normalized is None:
                    continue

                created_dt = parse_bsky_datetime(normalized.created_at_raw)
                if created_dt is None:
                    skipped_missing_created += 1
                    LOGGER.warning(
                        "Skipping post without parseable createdAt: %s",
                        normalized.post_uri,
                    )
                    continue

                if not (start_dt <= created_dt < end_exclusive):
                    continue

                existing = deduped.get(normalized.post_uri)
                if existing is None:
                    deduped[normalized.post_uri] = normalized
                else:
                    deduped[normalized.post_uri] = choose_preferred_record(
                        existing=existing,
                        incoming=normalized,
                    )
                kept_in_page += 1

            LOGGER.info(
                "Window %s page %s | received=%s kept_in_range=%s "
                "skipped_missing_created=%s window_unique=%s hits_total=%s",
                window_label,
                page_number,
                len(posts),
                kept_in_page,
                skipped_missing_created,
                len(deduped),
                hits_total,
            )

            if local_max_results is not None and len(deduped) >= local_max_results:
                LOGGER.info(
                    "Window %s reached local max-results=%s.",
                    window_label,
                    local_max_results,
                )
                break

            if not posts or not next_cursor:
                break

            if next_cursor in seen_cursors:
                LOGGER.warning(
                    "Search cursor repeated within window %s; stopping this window to avoid loop.",
                    window_label,
                )
                break

            seen_cursors.add(next_cursor)
            cursor = next_cursor

        return WindowCollectionResult(records=deduped, pages=page_number, received=received)

    @staticmethod
    def _finalize_records(
        deduped: Dict[str, NormalizedPostRecord],
        max_results: Optional[int],
    ) -> List[NormalizedPostRecord]:
        records = sorted(
            deduped.values(),
            key=lambda record: (
                record.created_at_iso or "",
                record.post_uri,
            ),
        )
        if max_results is not None:
            return records[:max_results]
        return records


def ensure_parent_dir(output_path: Path) -> None:
    if output_path.parent and not output_path.parent.exists():
        output_path.parent.mkdir(parents=True, exist_ok=True)


def write_csv(records: Sequence[NormalizedPostRecord], output_path: Path) -> None:
    ensure_parent_dir(output_path)
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=OUTPUT_FIELD_ORDER, extrasaction="ignore")
        writer.writeheader()
        for record in records:
            writer.writerow(record.to_dict())


def write_json(records: Sequence[NormalizedPostRecord], output_path: Path) -> None:
    ensure_parent_dir(output_path)
    payload = [record.to_dict() for record in records]
    with output_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
        handle.write("\n")


def load_auth_credentials(args: argparse.Namespace) -> Tuple[str, str]:
    identifier = args.auth_identifier or os.getenv("BLUESKY_HANDLE") or ""
    password = args.auth_password or os.getenv("BLUESKY_APP_PASSWORD") or ""

    if not identifier:
        raise ValueError(
            "Authenticated mode requires --auth-identifier or BLUESKY_HANDLE."
        )

    if not password:
        password = getpass.getpass("Bluesky app password: ").strip()

    if not password:
        raise ValueError(
            "Authenticated mode requires --auth-password, BLUESKY_APP_PASSWORD, or interactive input."
        )

    return identifier, password


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    setup_logging(args.verbose)

    try:
        validate_args(args)
        start_date = parse_iso_date(args.start_date)
        end_date = parse_iso_date(args.end_date)
    except (ValueError, argparse.ArgumentTypeError) as exc:
        LOGGER.error("%s", exc)
        return 2

    start_dt, end_exclusive = utc_date_bounds(start_date, end_date)

    base_url = PUBLIC_APPVIEW_BASE_URL
    auth_service = args.auth_service if args.authenticated else None
    rate_limiter = (
        PoliteRateLimiter(args.sleep_seconds)
        if args.parallel_workers > 1
        else None
    )

    client = BlueskyAPIClient(
        base_url=base_url,
        timeout_seconds=args.timeout_seconds,
        sleep_seconds=args.sleep_seconds,
        max_retries=args.max_retries,
        backoff_base_seconds=args.backoff_base_seconds,
        auth_service=auth_service,
        rate_limiter=rate_limiter,
    )

    try:
        if args.authenticated:
            identifier, password = load_auth_credentials(args)
            client.authenticate(identifier=identifier, password=password)

        collector = BlueskyKeywordCollector(client)
        records = collector.collect(
            keyword=args.keyword,
            start_dt=start_dt,
            end_exclusive=end_exclusive,
            page_size=args.page_size,
            lang=args.lang,
            window_days=args.window_days,
            max_results=args.max_results,
            parallel_workers=args.parallel_workers,
        )

        output_path = Path(args.output_path)
        if args.output_format == "csv":
            write_csv(records, output_path)
        else:
            write_json(records, output_path)

        LOGGER.info("Wrote %s records to %s", len(records), output_path)
        return 0
    except KeyboardInterrupt:
        LOGGER.error("Interrupted by user.")
        return 130
    except BlueskyRequestError as exc:
        LOGGER.error("Bluesky API request failed: %s", exc)
        return 1
    except requests.RequestException as exc:
        LOGGER.error("HTTP request failed: %s", exc)
        return 1
    except Exception as exc:  # pragma: no cover - final safety net
        LOGGER.exception("Unexpected failure: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
