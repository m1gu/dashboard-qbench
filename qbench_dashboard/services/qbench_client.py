import time
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

import jwt
import requests

from qbench_dashboard.config import QBenchSettings, get_qbench_settings


class QBenchError(RuntimeError):
    pass


class QBenchClient:
    def __init__(self, settings: Optional[QBenchSettings] = None) -> None:
        self.settings = settings or get_qbench_settings()
        self._token_exp = 0.0
        self._token = ""
        self.session = requests.Session()

    def _is_token_expired(self) -> bool:
        return not self._token or time.time() >= self._token_exp

    def _authenticate(self) -> None:
        now = time.time()
        iat = now - self.settings.jwt_leeway
        exp = iat + min(self.settings.jwt_ttl, 3300)
        assertion = jwt.encode(
            {"iat": iat, "exp": exp, "sub": self.settings.client_id},
            self.settings.client_secret,
            algorithm="HS256",
        )
        url = f"{self.settings.base_url}/qbench/oauth2/v1/token"
        data = {
            "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
            "assertion": assertion,
        }
        try:
            response = self.session.post(url, data=data, timeout=30)
            response.raise_for_status()
        except requests.RequestException as exc:
            self._token = ""
            self._token_exp = 0.0
            raise QBenchError(f"Auth request failed: {exc}") from exc

        payload = response.json()
        token = payload.get("access_token")
        if not token:
            self._token = ""
            self._token_exp = 0.0
            message = payload.get("error_description") or payload.get("error") or "unknown auth error"
            raise QBenchError(f"Auth error: {message}")

        self._token = token
        self._token_exp = exp

    def _request(self, method, path: str, *, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = f"{self.settings.base_url}/qbench/api/v1/{path.lstrip('/')}"
        delay = 1.0
        for _ in range(5):
            if self._is_token_expired():
                self._authenticate()
            headers = {"Authorization": f"Bearer {self._token}", "Accept": "application/json"}
            try:
                resp = method(url, params=params, headers=headers, timeout=30)
            except requests.Timeout:
                time.sleep(delay)
                delay = min(delay * 2, 16)
                continue
            except requests.RequestException as exc:
                raise QBenchError(f"Request failed: {exc}") from exc

            if resp.status_code == 401:
                self._token = ""
                self._token_exp = 0.0
                time.sleep(delay)
                delay = min(delay * 2, 16)
                continue
            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After")
                wait_s = float(retry_after) if retry_after else delay
                time.sleep(wait_s)
                delay = min(delay * 2, 16)
                continue

            try:
                resp.raise_for_status()
            except requests.RequestException as exc:
                raise QBenchError(f"HTTP {resp.status_code}: {resp.text}") from exc

            try:
                return resp.json()
            except ValueError as exc:
                raise QBenchError("Response is not JSON") from exc

        raise QBenchError(f"Failed request after retries: {url}")

    def fetch_recent_samples(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        *,
        page_size: int = 50,
        max_days: int = 30,
        default_days: int = 7,
    ) -> List[Dict[str, Any]]:
        """Fetch samples within a given date range (defaults to the last 7 days)."""
        now = datetime.now(timezone.utc)

        def _normalize(value: Optional[Union[datetime, date]], *, pad_end: bool) -> Optional[datetime]:
            if value is None:
                return None
            if isinstance(value, datetime):
                dt = value
            elif isinstance(value, date):
                time_part = datetime.max.time() if pad_end else datetime.min.time()
                dt = datetime.combine(value, time_part)
            else:
                raise TypeError(f"Unsupported date value: {type(value)!r}")
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            else:
                dt = dt.astimezone(timezone.utc)
            return dt

        end_dt = _normalize(end_date, pad_end=True) or now
        lookback_days = max(1, min(default_days, max_days))
        start_dt = _normalize(start_date, pad_end=False) or end_dt - timedelta(days=lookback_days)

        if end_dt < start_dt:
            raise ValueError("Start date must be before or equal to end date.")
        if end_dt - start_dt > timedelta(days=max_days):
            raise ValueError(f"Date range cannot exceed {max_days} days.")

        samples: List[Dict[str, Any]] = []
        page = 1
        while True:
            params = {
                "page_num": page,
                "page_size": page_size,
                "sort_by": "date_created",
                "sort_order": "desc",
            }
            payload = self._request(self.session.get, "sample", params=params)
            page_samples = self._extract_samples(payload)
            if not page_samples:
                break

            filtered: List[Dict[str, Any]] = []
            stop_pagination = False
            for sample in page_samples:
                created = sample.get("date_created")
                if isinstance(created, datetime):
                    if created > end_dt:
                        continue
                    if created < start_dt:
                        stop_pagination = True
                        break
                filtered.append(sample)
            samples.extend(filtered)

            if stop_pagination or len(page_samples) < page_size:
                break
            page += 1
        return samples

    def count_recent_tests(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        *,
        page_size: int = 50,
        max_days: int = 30,
        default_days: int = 7,
        sample_ids: Optional[Sequence[Union[str, int]]] = None,
        chunk_size: int = 40,
    ) -> Tuple[int, List[Tuple[datetime, int]], float, int]:
        """Collect tests created within a date range and return totals and series."""
        now = datetime.now(timezone.utc)

        def _normalize(value: Optional[Union[datetime, date]], *, pad_end: bool) -> Optional[datetime]:
            if value is None:
                return None
            if isinstance(value, datetime):
                dt = value
            elif isinstance(value, date):
                time_part = datetime.max.time() if pad_end else datetime.min.time()
                dt = datetime.combine(value, time_part)
            else:
                raise TypeError(f"Unsupported date value: {type(value)!r}")
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            else:
                dt = dt.astimezone(timezone.utc)
            return dt

        end_dt = _normalize(end_date, pad_end=True) or now
        lookback_days = max(1, min(default_days, max_days))
        start_dt = _normalize(start_date, pad_end=False) or end_dt - timedelta(days=lookback_days)

        if end_dt < start_dt:
            raise ValueError("Start date must be before or equal to end date.")
        if end_dt - start_dt > timedelta(days=max_days):
            raise ValueError(f"Date range cannot exceed {max_days} days.")

        from collections import Counter

        counter: Counter = Counter()
        total = 0
        sum_seconds = 0.0
        duration_count = 0

        def _process_page(items: List[Dict[str, Any]]) -> bool:
            nonlocal total, sum_seconds, duration_count
            stop = False
            for item in items:
                if not isinstance(item, dict):
                    continue
                created = self._parse_date(item.get("date_created"))
                if not isinstance(created, datetime):
                    continue
                if created > end_dt:
                    continue
                if created < start_dt:
                    stop = True
                    break
                total += 1
                counter[created.date()] += 1

                completed = self._parse_date(item.get("report_completed_date"))
                if isinstance(completed, datetime):
                    delta = (completed - created).total_seconds()
                    if delta > 0:
                        sum_seconds += delta
                        duration_count += 1
            return stop

        def _iterate(params: Dict[str, Any]) -> None:
            page = 1
            while True:
                params["page_num"] = page
                payload = self._request(self.session.get, "test", params=params)
                data = payload.get("data")
                if not data:
                    break
                if isinstance(data, dict):
                    page_items = [data]
                else:
                    page_items = list(data)
                if not page_items:
                    break
                stop = _process_page(page_items)
                if stop or len(page_items) < params.get("page_size", page_size):
                    break
                page += 1

        if sample_ids:
            ids = []
            seen = set()
            for sid in sample_ids:
                key = str(sid)
                if key and key not in seen:
                    seen.add(key)
                    ids.append(key)
            if not ids:
                return 0, [], 0.0, 0
            step = max(1, chunk_size)
            for index in range(0, len(ids), step):
                chunk = ids[index : index + step]
                params = {
                    "page_size": page_size,
                    "sort_by": "date_created",
                    "sort_order": "desc",
                    "sample_ids": chunk,
                }
                _iterate(params)
        else:
            params = {
                "page_size": page_size,
                "sort_by": "date_created",
                "sort_order": "desc",
            }
            _iterate(params)

        series = [
            (datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc), count)
            for day, count in sorted(counter.items())
        ]
        return total, series, sum_seconds, duration_count

    def count_recent_customers(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        *,
        page_size: int = 50,
        max_days: int = 30,
        default_days: int = 7,
    ) -> int:
        """Count customers created within the given date range."""
        now = datetime.now(timezone.utc)

        def _normalize(value: Optional[Union[datetime, date]], *, pad_end: bool) -> Optional[datetime]:
            if value is None:
                return None
            if isinstance(value, datetime):
                dt = value
            elif isinstance(value, date):
                time_part = datetime.max.time() if pad_end else datetime.min.time()
                dt = datetime.combine(value, time_part)
            else:
                raise TypeError(f"Unsupported date value: {type(value)!r}")
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            else:
                dt = dt.astimezone(timezone.utc)
            return dt

        end_dt = _normalize(end_date, pad_end=True) or now
        lookback_days = max(1, min(default_days, max_days))
        start_dt = _normalize(start_date, pad_end=False) or end_dt - timedelta(days=lookback_days)

        if end_dt < start_dt:
            raise ValueError("Start date must be before or equal to end date.")
        if end_dt - start_dt > timedelta(days=max_days):
            raise ValueError(f"Date range cannot exceed {max_days} days.")

        total = 0
        page = 1
        while True:
            params = {
                "page_num": page,
                "page_size": page_size,
                "sort_by": "date_created",
                "sort_order": "desc",
            }
            payload = self._request(self.session.get, "customer", params=params)
            data = payload.get("data")
            if not data:
                break
            if isinstance(data, dict):
                items = [data]
            else:
                items = list(data)
            if not items:
                break

            stop = False
            for item in items:
                if not isinstance(item, dict):
                    continue
                created = self._parse_date(item.get("date_created"))
                if not isinstance(created, datetime):
                    continue
                if created > end_dt:
                    continue
                if created < start_dt:
                    stop = True
                    break
                total += 1

            if stop or len(items) < page_size:
                break
            page += 1

        return total

    def _extract_samples(self, payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        data = payload.get("data")
        if not data:
            return []
        if isinstance(data, dict):
            data_iterable = [data]
        else:
            data_iterable = list(data)

        normalized: List[Dict[str, Any]] = []
        for raw in data_iterable:
            sample = self._normalize_sample(raw)
            if sample:
                normalized.append(sample)
        return normalized

    def _normalize_sample(self, raw: Any) -> Optional[Dict[str, Any]]:
        if not isinstance(raw, dict):
            return None
        attributes = raw.get("attributes") if isinstance(raw.get("attributes"), dict) else None
        sample_id = raw.get("sample_id") or raw.get("id")
        status = raw.get("status")
        date_value = raw.get("date_created")

        if attributes:
            sample_id = attributes.get("sample_id") or attributes.get("id") or sample_id
            status = attributes.get("status") or status
            date_value = attributes.get("date_created") or attributes.get("created_at") or date_value

        parsed_date = self._parse_date(date_value)
        has_report = bool(raw.get("has_report"))
        if attributes:
            has_report = bool(attributes.get("has_report", has_report))
        return {
            "id": str(sample_id) if sample_id is not None else "",
            "status": status or "",
            "date_created": parsed_date,
            "has_report": has_report,
        }

    @staticmethod
    def _parse_date(value: Any) -> Optional[datetime]:
        if not value:
            return None
        if isinstance(value, datetime):
            return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        if isinstance(value, (int, float)):
            return datetime.fromtimestamp(float(value), tz=timezone.utc)
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return None
            if text.endswith("Z"):
                text = text[:-1] + "+00:00"
            try:
                parsed = datetime.fromisoformat(text)
            except ValueError:
                parsed = None
            if parsed:
                return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
            numeric = text.replace('.', '', 1).replace('-', '', 1)
            if numeric.isdigit():
                try:
                    return datetime.fromtimestamp(float(text), tz=timezone.utc)
                except ValueError:
                    pass
            if "/" in text:
                for fmt in ("%m/%d/%Y %I:%M %p", "%m/%d/%Y %H:%M", "%m/%d/%Y"):
                    try:
                        parsed = datetime.strptime(text, fmt)
                    except ValueError:
                        continue
                    else:
                        return parsed.replace(tzinfo=timezone.utc)
            return None
        return None
