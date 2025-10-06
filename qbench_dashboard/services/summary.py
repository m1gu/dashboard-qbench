from datetime import datetime, timezone
from typing import Dict, Optional, Sequence, Tuple

Series = Sequence[Tuple[datetime, int]]


def build_summary(
    *,
    samples_total: int,
    samples_series: Series,
    tests_total: int = 0,
    tests_series: Optional[Series] = None,
    tests_tat_sum: float = 0.0,
    tests_tat_count: int = 0,
    tests_tat_daily: Optional[Sequence[Tuple[datetime, float, int]]] = None,
    tests_tat_daily_previous: Optional[Sequence[Tuple[datetime, float, int]]] = None,
    customers_total: int = 0,
    reports_total: int = 0,
    customers_recent: Optional[Sequence[Dict[str, object]]] = None,
    customer_test_totals: Optional[Sequence[Dict[str, object]]] = None,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
) -> Dict[str, object]:
    def _as_utc(dt: Optional[datetime]) -> Optional[datetime]:
        if dt is None:
            return None
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)

    normalized_samples = [
        (dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc), count)
        for dt, count in samples_series
        if isinstance(dt, datetime)
    ]
    normalized_tests = []
    if tests_series:
        normalized_tests = [
            (dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc), count)
            for dt, count in tests_series
            if isinstance(dt, datetime)
        ]

    tat_average_seconds = 0.0
    if tests_tat_count > 0:
        tat_average_seconds = float(tests_tat_sum) / float(tests_tat_count)

    customers_payload = []
    if customers_recent:
        for item in customers_recent[:20]:
            if not isinstance(item, dict):
                continue
            created = item.get("date_created")
            created_dt = created if isinstance(created, datetime) else None
            customers_payload.append({
                "id": item.get("id"),
                "name": item.get("name"),
                "date_created": _as_utc(created_dt),
            })

    tests_leaderboard = []
    if customer_test_totals:
        for item in customer_test_totals[:10]:
            if not isinstance(item, dict):
                continue
            created = item.get("date_last_order")
            created_dt = created if isinstance(created, datetime) else None
            tests_leaderboard.append({
                "id": item.get("id"),
                "name": item.get("name"),
                "test_count": int(item.get("test_count") or 0),
                "date_last_order": _as_utc(created_dt),
            })

    tat_daily_payload = []
    if tests_tat_daily:
        for dt_value, average_seconds, count_value in tests_tat_daily:
            if not isinstance(dt_value, datetime):
                continue
            normalized_dt = dt_value if dt_value.tzinfo else dt_value.replace(tzinfo=timezone.utc)
            tat_daily_payload.append({
                "date": normalized_dt,
                "average_seconds": float(average_seconds),
                "test_count": int(count_value),
            })

    tat_previous_payload = []
    if tests_tat_daily_previous:
        for dt_value, average_seconds, count_value in tests_tat_daily_previous:
            if not isinstance(dt_value, datetime):
                continue
            normalized_dt = dt_value if dt_value.tzinfo else dt_value.replace(tzinfo=timezone.utc)
            tat_previous_payload.append({
                "date": normalized_dt,
                "average_seconds": float(average_seconds),
                "test_count": int(count_value),
            })

    return {
        "samples_total": samples_total,
        "samples_series": normalized_samples,
        "tests_total": max(0, int(tests_total)),
        "tests_series": normalized_tests,
        "tests_tat_average_seconds": tat_average_seconds,
        "tests_tat_count": max(0, int(tests_tat_count)),
        "tests_tat_daily": tat_daily_payload,
        "tests_tat_daily_previous": tat_previous_payload,
        "customers_total": max(0, int(customers_total)),
        "reports_total": max(0, int(reports_total)),
        "customers_recent": customers_payload,
        "customer_test_totals": tests_leaderboard,
        "start_date": _as_utc(start_date),
        "end_date": _as_utc(end_date),
    }
