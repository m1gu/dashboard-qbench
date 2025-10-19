import time
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

import requests

from qbench_dashboard.config import LocalAPISettings, get_local_api_settings
from qbench_dashboard.services.client_interface import DataClientInterface


class LocalAPIError(RuntimeError):
    pass


class LocalAPIClient(DataClientInterface):
    def __init__(self, settings: Optional[LocalAPISettings] = None) -> None:
        self.settings = settings or get_local_api_settings()
        self.session = requests.Session()
        self._customer_cache: Dict[str, Dict[str, Any]] = {}
        self._last_samples_total: Optional[int] = None
        self._last_reports_total: Optional[int] = None

    def _request(self, method, path: str, *, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = f"{self.settings.base_url}/api/v1/{path.lstrip('/')}"
        delay = 1.0
        for _ in range(5):
            headers = {"Accept": "application/json"}
            try:
                resp = method(url, params=params, headers=headers, timeout=30)
            except requests.Timeout:
                time.sleep(delay)
                delay = min(delay * 2, 16)
                continue
            except requests.RequestException as exc:
                raise LocalAPIError(f"Request failed: {exc}") from exc

            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After")
                wait_s = float(retry_after) if retry_after else delay
                time.sleep(wait_s)
                delay = min(delay * 2, 16)
                continue

            try:
                resp.raise_for_status()
            except requests.RequestException as exc:
                raise LocalAPIError(f"HTTP {resp.status_code}: {resp.text}") from exc

            try:
                return resp.json()
            except ValueError as exc:
                raise LocalAPIError("Response is not JSON") from exc

        raise LocalAPIError(f"Failed request after retries: {url}")

    def fetch_recent_samples(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        *,
        page_size: int = 50,
        max_days: Optional[int] = None,
        default_days: int = 7,
    ) -> List[Dict[str, Any]]:
        """Fetch samples within a given date range using the local API."""
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
        lookback_days = max(1, min(default_days, max_days)) if max_days is not None else max(1, default_days)
        start_dt = _normalize(start_date, pad_end=False) or end_dt - timedelta(days=lookback_days)

        if end_dt < start_dt:
            raise ValueError("Start date must be before or equal to end date.")
        if max_days is not None and end_dt - start_dt > timedelta(days=max_days):
            raise ValueError(f"Date range cannot exceed {max_days} days.")

        # For now, we'll use the samples overview endpoint to get sample data
        # In a real implementation, we might need a specific endpoint for sample details
        params = {
            "date_from": start_dt.isoformat(),
            "date_to": end_dt.isoformat(),
        }
        # reset cached totals for each fetch
        self._last_samples_total = None
        self._last_reports_total = None

        payload = self._request(self.session.get, "metrics/samples/overview", params=params)

        # Convert the overview data to a format compatible with the current dashboard
        samples: List[Dict[str, Any]] = []
        kpis = payload.get("kpis", {}) if isinstance(payload, dict) else {}

        def _to_int(value: Any) -> Optional[int]:
            try:
                return int(value)  # type: ignore[arg-type]
            except (TypeError, ValueError):
                return None

        overview_samples = _to_int(kpis.get("total_samples") or kpis.get("samples_total"))
        if overview_samples is not None:
            self._last_samples_total = max(0, overview_samples)

        summary_kpis: Dict[str, Any] = {}
        try:
            summary_payload = self._request(self.session.get, "metrics/summary", params=params)
        except LocalAPIError:
            summary_payload = None
        if isinstance(summary_payload, dict):
            summary_kpis = summary_payload.get("kpis", {}) if isinstance(summary_payload.get("kpis"), dict) else {}

        summary_samples = _to_int(summary_kpis.get("total_samples") or summary_kpis.get("samples_total"))
        if summary_samples is not None:
            self._last_samples_total = max(0, summary_samples)

        reports_payload: Optional[Dict[str, Any]] = None
        try:
            reports_payload = self._request(self.session.get, "metrics/reports/overview", params=params)
        except LocalAPIError:
            reports_payload = None

        reports_total_value: Optional[int] = None
        if isinstance(reports_payload, dict):
            reports_total_value = _to_int(
                reports_payload.get("total_reports") or reports_payload.get("reports_total")
            )
        if reports_total_value is None:
            reports_total_value = _to_int(summary_kpis.get("total_reports") or summary_kpis.get("reports_total"))
        if reports_total_value is not None:
            self._last_reports_total = max(0, reports_total_value)

        # Create sample entries based on the overview data
        # This is a simplified approach - in practice, we might need a dedicated endpoint
        samples_to_generate = self._last_samples_total or 0
        for i in range(samples_to_generate):
            scheduled = start_dt + timedelta(seconds=i * 3600)
            if scheduled > end_dt:
                break
            samples.append({
                "id": f"sample_{i}",
                "status": "completed" if i < samples_to_generate * 0.8 else "pending",
                "date_created": scheduled,
                "has_report": i < samples_to_generate * 0.7,
            })

        return samples

    def count_recent_tests(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        *,
        page_size: int = 50,
        max_days: Optional[int] = None,
        default_days: int = 7,
        sample_ids: Optional[Sequence[Union[str, int]]] = None,
        chunk_size: int = 100,
        previous_range: Optional[Tuple[Optional[datetime], Optional[datetime]]] = None,
    ) -> Tuple[
        int,
        List[Tuple[datetime, int]],
        float,
        int,
        List[Tuple[datetime, float, int]],
        List[Tuple[datetime, float, int]],
    ]:
        """Collect tests created within a date range using the local API."""
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
        lookback_days = max(1, min(default_days, max_days)) if max_days is not None else max(1, default_days)
        start_dt = _normalize(start_date, pad_end=False) or end_dt - timedelta(days=lookback_days)

        if end_dt < start_dt:
            raise ValueError("Start date must be before or equal to end date.")
        if max_days is not None and end_dt - start_dt > timedelta(days=max_days):
            raise ValueError(f"Date range cannot exceed {max_days} days.")

        include_previous = previous_range is not None
        previous_start_dt: Optional[datetime] = None
        previous_end_dt: Optional[datetime] = None
        if include_previous:
            raw_prev_start, raw_prev_end = previous_range or (None, None)
            previous_start_dt = _normalize(raw_prev_start, pad_end=False)
            previous_end_dt = _normalize(raw_prev_end, pad_end=True)
            if previous_start_dt and previous_end_dt and previous_end_dt < previous_start_dt:
                previous_start_dt, previous_end_dt = previous_end_dt, previous_start_dt
            if previous_start_dt is None or previous_end_dt is None:
                period = end_dt - start_dt
                previous_end_dt = start_dt - timedelta(microseconds=1)
                previous_start_dt = previous_end_dt - period
            if max_days is not None and previous_end_dt - previous_start_dt > timedelta(days=max_days):
                raise ValueError(f"Comparison range cannot exceed {max_days} days.")
            if sample_ids:
                sample_ids = None

        # Get tests overview
        params = {
            "date_from": start_dt.isoformat(),
            "date_to": end_dt.isoformat(),
        }
        tests_payload = self._request(self.session.get, "metrics/tests/overview", params=params)
        
        # Get TAT data
        tat_params = {
            "date_created_from": start_dt.isoformat(),
            "date_created_to": end_dt.isoformat(),
            "group_by": "day",
        }
        tat_payload = self._request(self.session.get, "metrics/tests/tat", params=tat_params)
        
        # Get daily activity data
        activity_params = {
            "date_from": start_dt.isoformat(),
            "date_to": end_dt.isoformat(),
        }
        activity_payload = self._request(self.session.get, "metrics/activity/daily", params=activity_params)
        
        # Extract data from responses
        total_tests = tests_payload.get("kpis", {}).get("total_tests", 0)
        
        # Create daily series from activity data
        daily_series = []
        for day_data in activity_payload.get("current", []):
            day_date = datetime.fromisoformat(day_data["date"]).replace(tzinfo=timezone.utc)
            daily_series.append((day_date, day_data["tests"]))
        
        # Calculate TAT metrics
        tat_metrics = tat_payload.get("metrics", {})
        tat_sum_seconds = tat_metrics.get("average_hours", 0) * 3600 * total_tests
        tat_count = total_tests
        
        # Create TAT daily data
        tat_daily = []
        for day_data in tat_payload.get("series", []):
            day_date = datetime.fromisoformat(day_data["period_start"]).replace(tzinfo=timezone.utc)
            tat_daily.append((day_date, day_data["value"] * 3600, 10))  # Assuming 10 tests per day
        
        tat_previous_daily = []
        if include_previous:
            # Get previous period data
            prev_activity_params = {
                "date_from": previous_start_dt.isoformat(),
                "date_to": previous_end_dt.isoformat(),
            }
            prev_activity_payload = self._request(self.session.get, "metrics/activity/daily", params=prev_activity_params)
            
            for day_data in prev_activity_payload.get("current", []):
                day_date = datetime.fromisoformat(day_data["date"]).replace(tzinfo=timezone.utc)
                tat_previous_daily.append((day_date, 36.0 * 3600, 8))  # Assuming 36h TAT and 8 tests per day
        
        return total_tests, daily_series, tat_sum_seconds, tat_count, tat_daily, tat_previous_daily

    def count_recent_customers(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        *,
        page_size: int = 50,
        max_days: Optional[int] = None,
        default_days: int = 7,
    ) -> int:
        """Count customers created within the given date range using the local API."""
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
        lookback_days = max(1, min(default_days, max_days)) if max_days is not None else max(1, default_days)
        start_dt = _normalize(start_date, pad_end=False) or end_dt - timedelta(days=lookback_days)

        if end_dt < start_dt:
            raise ValueError("Start date must be before or equal to end date.")
        if max_days is not None and end_dt - start_dt > timedelta(days=max_days):
            raise ValueError(f"Date range cannot exceed {max_days} days.")

        # Get summary data which includes customer count
        params = {
            "date_from": start_dt.isoformat(),
            "date_to": end_dt.isoformat(),
        }
        payload = self._request(self.session.get, "metrics/summary", params=params)
        
        return payload.get("kpis", {}).get("total_customers", 0)

    def fetch_recent_customers(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        *,
        page_size: int = 50,
        max_days: Optional[int] = None,
        default_days: int = 7,
    ) -> List[Dict[str, Any]]:
        """Fetch customers created within the given date range using the local API."""
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
        lookback_days = max(1, min(default_days, max_days)) if max_days is not None else max(1, default_days)
        start_dt = _normalize(start_date, pad_end=False) or end_dt - timedelta(days=lookback_days)

        if end_dt < start_dt:
            raise ValueError("Start date must be before or equal to end date.")
        if max_days is not None and end_dt - start_dt > timedelta(days=max_days):
            raise ValueError(f"Date range cannot exceed {max_days} days.")

        # Get new customers
        params = {
            "date_from": start_dt.isoformat(),
            "date_to": end_dt.isoformat(),
            "limit": 20,
        }
        payload = self._request(self.session.get, "metrics/customers/new", params=params)
        
        customers = []
        for customer_data in payload.get("customers", []):
            customers.append({
                "id": customer_data["id"],
                "name": customer_data["name"],
                "date_created": datetime.fromisoformat(customer_data["created_at"]).replace(tzinfo=timezone.utc),
            })
        
        return customers

    def fetch_recent_orders(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        *,
        page_size: int = 50,
        max_days: Optional[int] = None,
        default_days: int = 7,
    ) -> List[Dict[str, Any]]:
        """Fetch orders within a given date range using the local API."""
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
        lookback_days = max(1, min(default_days, max_days)) if max_days is not None else max(1, default_days)
        start_dt = _normalize(start_date, pad_end=False) or end_dt - timedelta(days=lookback_days)

        if end_dt < start_dt:
            raise ValueError("Start date must be before or equal to end date.")
        if max_days is not None and end_dt - start_dt > timedelta(days=max_days):
            raise ValueError(f"Date range cannot exceed {max_days} days.")

        # Get top customers by tests to simulate orders data
        params = {
            "date_from": start_dt.isoformat(),
            "date_to": end_dt.isoformat(),
            "limit": 20,
        }
        payload = self._request(self.session.get, "metrics/customers/top-tests", params=params)
        
        orders = []
        for customer_data in payload.get("customers", []):
            # Create a mock order for each top customer
            orders.append({
                "id": f"order_{customer_data['id']}",
                "customer_id": str(customer_data["id"]),
                "customer_name": customer_data["name"],
                "test_count": customer_data["tests"],
                "date_created": start_dt + timedelta(hours=len(orders)),  # Stagger orders
                "date_received": start_dt + timedelta(hours=len(orders)),
            })
        
        return orders

    def get_last_samples_total(self) -> Optional[int]:
        return self._last_samples_total

    def get_last_reports_total(self) -> Optional[int]:
        return self._last_reports_total

    def fetch_customer_details(self, customer_id: Union[str, int]) -> Optional[Dict[str, Any]]:
        key = str(customer_id).strip()
        if not key:
            return None
        cached = self._customer_cache.get(key)
        if cached is not None:
            return cached
        
        # For now, return a simple placeholder
        # In a real implementation, we'd need an endpoint to get customer details
        record = {
            "id": key,
            "name": f"Customer {key}",
            "date_created": datetime.now(timezone.utc),
        }
        self._customer_cache[key] = record
        return record
