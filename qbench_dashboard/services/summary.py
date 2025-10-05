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
    customers_total: int = 0,
    reports_total: int = 0,
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

    return {
        "samples_total": samples_total,
        "samples_series": normalized_samples,
        "tests_total": max(0, int(tests_total)),
        "tests_series": normalized_tests,
        "tests_tat_average_seconds": tat_average_seconds,
        "tests_tat_count": max(0, int(tests_tat_count)),
        "customers_total": max(0, int(customers_total)),
        "reports_total": max(0, int(reports_total)),
        "start_date": _as_utc(start_date),
        "end_date": _as_utc(end_date),
    }
