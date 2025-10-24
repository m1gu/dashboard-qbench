from __future__ import annotations

from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Sequence, Tuple

from PySide6.QtCharts import (
    QAreaSeries,
    QBarCategoryAxis,
    QBarSeries,
    QBarSet,
    QChart,
    QChartView,
    QDateTimeAxis,
    QHorizontalBarSeries,
    QLineSeries,
    QValueAxis,
)
from PySide6.QtCore import QDate, QDateTime, QLocale, QMargins, QTimer, Qt, QThread, QObject, Signal
from PySide6.QtGui import QBrush, QCursor, QColor, QGradient, QLinearGradient, QPalette, QPainter, QPen
from PySide6.QtWidgets import (
    QApplication,
    QAbstractItemView,
    QCalendarWidget,
    QCheckBox,
    QComboBox,
    QDateEdit,
    QFrame,
    QGridLayout,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QScrollArea,
    QTabWidget,
    QSizePolicy,
    QTableWidget,
    QTableWidgetItem,
    QSpinBox,
    QToolTip,
    QVBoxLayout,
    QWidget,
)

from qbench_dashboard.services.client_interface import DataClientInterface
from qbench_dashboard.services.summary import build_summary


class SummaryWorker(QObject):
    finished = Signal(dict)
    error = Signal(str)

    def __init__(
        self,
        client: DataClientInterface,
        *,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        timeframe: str = "daily",
    ) -> None:
        super().__init__()
        self._client = client
        self._start_date = start_date
        self._end_date = end_date
        self._timeframe = timeframe if timeframe in {"daily", "weekly", "monthly"} else "daily"

    def process(self) -> None:
        try:
            samples = self._client.fetch_recent_samples(
                start_date=self._start_date,
                end_date=self._end_date,
            )
            samples_total = len(samples)
            total_getter = getattr(self._client, "get_last_samples_total", None)
            if callable(total_getter):
                try:
                    reported_total = total_getter()
                except Exception:  # pragma: no cover - defensive
                    reported_total = None
                if reported_total is not None:
                    try:
                        samples_total = int(reported_total)
                    except (TypeError, ValueError):
                        pass
            counts = {}
            for sample in samples:
                created = sample.get("date_created")
                if isinstance(created, datetime):
                    key = created.date()
                    counts[key] = counts.get(key, 0) + 1
            samples_series = [
                (datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc), count)
                for day, count in sorted(counts.items())
            ]
            sample_ids: List[str] = []
            seen_ids = set()
            report_seen = set()
            reports_total = 0
            for sample in samples:
                sid = sample.get("id")
                if sid and sid not in seen_ids:
                    seen_ids.add(sid)
                    sample_ids.append(sid)
                has_report = sample.get("has_report") or str(sample.get("status", "")).upper() == "REPORTED"
                if has_report and sid and sid not in report_seen:
                    report_seen.add(sid)
                    reports_total += 1
            reports_getter = getattr(self._client, "get_last_reports_total", None)
            if callable(reports_getter):
                try:
                    reported_reports = reports_getter()
                except Exception:  # pragma: no cover - defensive
                    reported_reports = None
                if reported_reports is not None:
                    try:
                        reports_total = int(reported_reports)
                    except (TypeError, ValueError):
                        pass
            range_start = self._start_date
            range_end = self._end_date
            now_utc = datetime.now(timezone.utc)
            if range_end is None:
                range_end = now_utc
            if range_start is None:
                range_start = range_end - timedelta(days=6)
            if range_end < range_start:
                range_start, range_end = range_end, range_start
            period_delta = range_end - range_start
            previous_range: Optional[Tuple[datetime, datetime]] = None
            if period_delta.total_seconds() > 0:
                previous_end = range_start - timedelta(microseconds=1)
                previous_start = previous_end - period_delta
                previous_range = (previous_start, previous_end)
            tests_sample_ids = sample_ids if previous_range is None else None

            def _load_customers() -> Tuple[List[Dict[str, Any]], int]:
                try:
                    records = self._client.fetch_recent_customers(
                        start_date=self._start_date,
                        end_date=self._end_date,
                    )
                except Exception:
                    count_only = self._client.count_recent_customers(
                        start_date=self._start_date,
                        end_date=self._end_date,
                    )
                    return [], count_only
                return records, len(records)

            def _load_orders() -> List[Dict[str, Any]]:
                try:
                    return self._client.fetch_recent_orders(
                        start_date=self._start_date,
                        end_date=self._end_date,
                    )
                except Exception:
                    return []

            (
                tests_total,
                tests_series,
                tat_sum_seconds,
                tat_count,
                tat_daily,
                tat_previous_daily,
            ) = self._client.count_recent_tests(
                start_date=self._start_date,
                end_date=self._end_date,
                sample_ids=tests_sample_ids,
                previous_range=previous_range,
            )
            customer_records, customers_total = _load_customers()
            customer_orders = _load_orders()
            toppers: List[Dict[str, Any]] = []
            if customer_orders:
                name_map = {
                    str(item.get("id")): (item.get("name") or "")
                    for item in customer_records
                    if isinstance(item, dict) and item.get("id") is not None
                }
                aggregates: Dict[str, Dict[str, Any]] = {}
                fallback_datetime = datetime.min.replace(tzinfo=timezone.utc)
                for order in customer_orders:
                    customer_id = order.get("customer_id")
                    if not customer_id:
                        continue
                    entry = aggregates.get(customer_id)
                    if entry is None:
                        display_name = name_map.get(customer_id, "") or order.get("customer_name") or ""
                        entry = {
                            "id": customer_id,
                            "name": display_name,
                            "test_count": 0,
                            "date_last_order": None,
                        }
                        aggregates[customer_id] = entry
                    elif not entry.get("name"):
                        entry["name"] = name_map.get(customer_id, "") or order.get("customer_name") or customer_id
                    entry["test_count"] += int(order.get("test_count") or 0)
                    created = order.get("date_created")
                    if isinstance(created, datetime):
                        last = entry.get("date_last_order")
                        if last is None or created > last:
                            entry["date_last_order"] = created
                for entry in aggregates.values():
                    if not entry.get("name"):
                        entry["name"] = entry["id"]
                toppers = sorted(
                    aggregates.values(),
                    key=lambda item: (
                        item.get("test_count", 0),
                        item.get("date_last_order") or fallback_datetime,
                    ),
                    reverse=True,
                )
                for entry in toppers[:10]:
                    if not entry.get("name") or entry.get("name") == entry.get("id"):
                        details = self._client.fetch_customer_details(entry["id"])
                        if details and details.get("name"):
                            entry["name"] = details.get("name") or entry["id"]
            try:
                label_distribution = self._client.fetch_test_label_distribution(
                    start_date=self._start_date,
                    end_date=self._end_date,
                )
            except Exception:
                label_distribution = []
            if tests_series is None:
                tests_series = []
            else:
                tests_series = list(tests_series)
            aggregated_samples_series = self._aggregate_time_series(samples_series, self._timeframe)
            aggregated_tests_series = self._aggregate_time_series(tests_series, self._timeframe)

            summary = build_summary(
                samples_total=samples_total,
                samples_series=aggregated_samples_series,
                tests_total=tests_total,
                tests_series=aggregated_tests_series,
                tests_tat_sum=tat_sum_seconds,
                tests_tat_count=tat_count,
                tests_tat_daily=tat_daily,
                tests_tat_daily_previous=tat_previous_daily,
                customers_total=customers_total,
                reports_total=reports_total,
                customers_recent=customer_records,
                customer_test_totals=toppers,
                tests_label_distribution=label_distribution,
                start_date=self._start_date,
                end_date=self._end_date,
            )
            summary["timeframe_mode"] = self._timeframe
        except Exception as exc:  # pylint: disable=broad-except
            self.error.emit(str(exc))
        else:
            self.finished.emit(summary)

    @staticmethod
    def _normalize_datetime(value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    @classmethod
    def _bucket_start(cls, instant: datetime, mode: str) -> datetime:
        normalized = cls._normalize_datetime(instant)
        if mode == "weekly":
            base_date = normalized.date() - timedelta(days=normalized.weekday())
            return datetime.combine(base_date, datetime.min.time(), tzinfo=timezone.utc)
        if mode == "monthly":
            base_date = normalized.date().replace(day=1)
            return datetime.combine(base_date, datetime.min.time(), tzinfo=timezone.utc)
        return datetime.combine(normalized.date(), datetime.min.time(), tzinfo=timezone.utc)

    @classmethod
    def _aggregate_time_series(
        cls,
        series: Sequence[Tuple[datetime, int]],
        mode: str,
    ) -> List[Tuple[datetime, int]]:
        if mode not in {"weekly", "monthly"}:
            normalized_series = [
                (cls._normalize_datetime(dt_value), int(count))
                for dt_value, count in series
                if isinstance(dt_value, datetime)
            ]
            normalized_series.sort(key=lambda entry: entry[0])
            return normalized_series
        aggregates: Dict[datetime, int] = {}
        for dt_value, count in series:
            if not isinstance(dt_value, datetime):
                continue
            bucket = cls._bucket_start(dt_value, mode)
            aggregates[bucket] = aggregates.get(bucket, 0) + int(count or 0)
        return [(key, aggregates[key]) for key in sorted(aggregates.keys())]


class OperationalWorker(QObject):
    finished = Signal(dict)
    error = Signal(str)

    def __init__(
        self,
        client: DataClientInterface,
        *,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        timeframe: str = "week",
        order_limit: int = 15,
    ) -> None:
        super().__init__()
        self._client = client
        self._start_date = start_date
        self._end_date = end_date
        self._timeframe = timeframe if timeframe in {"day", "week", "month"} else "week"
        self._order_limit = max(1, int(order_limit))

    @staticmethod
    def _normalize_datetime(value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    def process(self) -> None:
        try:
            throughput = self._client.fetch_order_throughput(
                start_date=self._start_date,
                end_date=self._end_date,
                interval=self._timeframe,
            )
            cycle_time = self._client.fetch_sample_cycle_time(
                start_date=self._start_date,
                end_date=self._end_date,
                interval="day" if self._timeframe == "day" else self._timeframe,
            )
            funnel = self._client.fetch_order_funnel(
                start_date=self._start_date,
                end_date=self._end_date,
            )
            slow_orders = self._client.fetch_slowest_orders(
                start_date=self._start_date,
                end_date=self._end_date,
                limit=self._order_limit,
            )
        except Exception as exc:  # pylint: disable=broad-except
            self.error.emit(str(exc))
            return

        throughput_points: List[Dict[str, Any]] = []
        for entry in throughput.get("points", []):
            if not isinstance(entry, dict):
                continue
            period = entry.get("period_start")
            if isinstance(period, datetime):
                normalized = self._normalize_datetime(period)
            else:
                normalized = None
            throughput_points.append({
                "period_start": normalized,
                "orders_created": int(entry.get("orders_created") or 0),
                "orders_completed": int(entry.get("orders_completed") or 0),
                "average_completion_hours": float(entry.get("average_completion_hours") or 0.0),
                "median_completion_hours": float(entry.get("median_completion_hours") or 0.0),
            })

        cycle_points: List[Dict[str, Any]] = []
        for entry in cycle_time.get("points", []):
            if not isinstance(entry, dict):
                continue
            period = entry.get("period_start")
            if isinstance(period, datetime):
                normalized = self._normalize_datetime(period)
            else:
                normalized = None
            cycle_points.append({
                "period_start": normalized,
                "completed_samples": int(entry.get("completed_samples") or 0),
                "average_cycle_hours": float(entry.get("average_cycle_hours") or 0.0),
                "median_cycle_hours": float(entry.get("median_cycle_hours") or 0.0),
            })

        matrix_breakdown: List[Dict[str, Any]] = []
        for entry in cycle_time.get("by_matrix_type", []):
            if not isinstance(entry, dict):
                continue
            matrix_breakdown.append({
                "matrix_type": entry.get("matrix_type") or "Unknown",
                "completed_samples": int(entry.get("completed_samples") or 0),
                "average_cycle_hours": float(entry.get("average_cycle_hours") or 0.0),
            })

        funnel_stages: List[Dict[str, Any]] = []
        for entry in funnel.get("stages", []):
            if not isinstance(entry, dict):
                continue
            funnel_stages.append({
                "stage": entry.get("stage") or "unknown",
                "count": int(entry.get("count") or 0),
            })

        slowest_orders: List[Dict[str, Any]] = []
        for entry in slow_orders:
            if not isinstance(entry, dict):
                continue
            slowest_orders.append({
                "order_id": entry.get("order_id") or entry.get("id") or "",
                "customer_name": entry.get("customer_name") or entry.get("customer") or "",
                "status": entry.get("status") or "",
                "completion_hours": float(entry.get("completion_hours") or 0.0),
                "age_hours": float(entry.get("age_hours") or 0.0),
            })

        totals = throughput.get("totals") if isinstance(throughput.get("totals"), dict) else {}
        cycle_totals = cycle_time.get("totals") if isinstance(cycle_time.get("totals"), dict) else {}

        summary = {
            "start_date": self._start_date if isinstance(self._start_date, datetime) else None,
            "end_date": self._end_date if isinstance(self._end_date, datetime) else None,
            "timeframe": self._timeframe,
            "metrics": {
                "orders_completed": int(totals.get("orders_completed") or 0),
                "orders_created": int(totals.get("orders_created") or 0),
                "lead_time_average_hours": float(totals.get("average_completion_hours") or 0.0),
                "lead_time_median_hours": float(totals.get("median_completion_hours") or 0.0),
                "samples_completed": int(cycle_totals.get("completed_samples") or 0),
                "sample_cycle_average_hours": float(cycle_totals.get("average_cycle_hours") or 0.0),
                "sample_cycle_median_hours": float(cycle_totals.get("median_cycle_hours") or 0.0),
            },
            "throughput_points": throughput_points,
            "cycle_points": cycle_points,
            "cycle_by_matrix": matrix_breakdown,
            "funnel_total": int(funnel.get("total_orders") or 0),
            "funnel_stages": funnel_stages,
            "slowest_orders": slowest_orders,
        }
        self.finished.emit(summary)


class PriorityOrdersWorker(QObject):
    finished = Signal(dict)
    error = Signal(str)

    def __init__(
        self,
        client: DataClientInterface,
        *,
        date_from: datetime,
        date_to: datetime,
        min_days_overdue: int,
        sla_hours: int,
        top_limit: int,
    ) -> None:
        super().__init__()
        self._client = client
        self._date_from = date_from
        self._date_to = date_to
        self._min_days_overdue = min_days_overdue
        self._sla_hours = sla_hours
        self._top_limit = top_limit

    def process(self) -> None:
        try:
            payload = self._client.fetch_overdue_orders(
                date_from=self._date_from,
                date_to=self._date_to,
                min_days_overdue=self._min_days_overdue,
                sla_hours=self._sla_hours,
                top_limit=self._top_limit,
            )
        except Exception as exc:  # pylint: disable=broad-except
            self.error.emit(str(exc))
        else:
            self.finished.emit(payload)


class MainWindow(QMainWindow):
    def __init__(self, client: DataClientInterface) -> None:
        super().__init__()
        self._client = client
        self._thread: Optional[QThread] = None
        self._worker: Optional[SummaryWorker] = None
        self._loading = False
        self._operational_thread: Optional[QThread] = None
        self._operational_worker: Optional[OperationalWorker] = None
        self._operational_loading = False
        self._operational_initialized = False
        self._tat_target_seconds = 48 * 3600  # 48-hour SLA target by default
        self._tat_moving_average_window = 7
        self._tat_tooltip_data: Dict[int, Tuple[datetime, float, int]] = {}
        self._test_type_categories: List[str] = []
        self._priority_thread: Optional[QThread] = None
        self._priority_worker: Optional[PriorityOrdersWorker] = None
        self._priority_loading = False
        self._priority_initialized = False
        self._priority_top_limit = 25
        self._priority_min_days_default = 5
        self._priority_sla_hours_default = 240
        self._priority_last_payload: Optional[Dict[str, Any]] = None
        self._priority_heatmap_periods: List[datetime] = []
        self._priority_heatmap_customers: List[str] = []

        self.setWindowTitle("QBench Dashboard")
        self.resize(1280, 720)
        self._apply_dark_palette()

        self.status_label = QLabel("Ready")
        self.status_label.setAlignment(Qt.AlignCenter)
        self.status_label.setStyleSheet("color: #B0BCD5;")

        self.spinner_label = QLabel("")
        self.spinner_label.setAlignment(Qt.AlignCenter)
        self.spinner_label.setStyleSheet("color: #7EE787; font-size: 14px;")
        self.spinner_label.setVisible(False)
        self._spinner_frames = ["|", "/", "-", "\\"]
        self._spinner_index = 0
        self._spinner_timer = QTimer(self)
        self._spinner_timer.setInterval(100)
        self._spinner_timer.timeout.connect(self._advance_spinner)

        self.start_date_edit = self._create_date_edit()
        self.end_date_edit = self._create_date_edit()
        self._initialize_default_range()

        self._timeframe_mode = "daily"
        self._current_timeframe_mode = "daily"
        self._timeframe_manual_override = False
        self._operational_timeframe_mode = "week"
        self._operational_current_timeframe_mode = "week"

        self._timeframe_combo = QComboBox()
        self._timeframe_combo.setStyleSheet(
            "padding: 8px 12px; font-size: 14px; background-color: #1E2A44; color: #E0E8FF; "
            "border: 1px solid #1F3B73; border-radius: 6px;"
        )
        self._timeframe_combo.setMinimumWidth(140)
        self._timeframe_combo.addItem("Daily", "daily")
        self._timeframe_combo.addItem("Weekly", "weekly")
        self._timeframe_combo.addItem("Monthly", "monthly")
        self._timeframe_combo.setCurrentIndex(0)
        self._timeframe_combo.currentIndexChanged.connect(self._on_timeframe_changed)

        self.refresh_button = QPushButton("Refresh")
        self.refresh_button.clicked.connect(self.refresh_data)
        self.refresh_button.setFixedWidth(140)
        self.refresh_button.setStyleSheet(
            "padding: 12px; font-size: 16px; background-color: #1F3B73; color: white; border-radius: 6px;"
        )

        self.chart = QChart()
        self.chart.setBackgroundBrush(Qt.transparent)
        legend = self.chart.legend()
        legend.setVisible(True)
        legend.setLabelBrush(QBrush(Qt.white))
        legend.setBackgroundVisible(False)

        self.samples_set = QBarSet("Samples")
        self.samples_set.setColor(QColor(0x4C, 0x6E, 0xF5))

        self.tests_set = QBarSet("Tests")
        self.tests_set.setColor(QColor(0x7E, 0xE7, 0x87))

        self.bar_series = QBarSeries()
        self.bar_series.append(self.samples_set)
        self.bar_series.append(self.tests_set)
        self.chart.addSeries(self.bar_series)

        self.categories_axis = QBarCategoryAxis()
        self.categories_axis.setLabelsColor(Qt.white)
        self.categories_axis.setTitleText("Fecha")
        self.categories_axis.setTitleBrush(Qt.white)

        self.value_axis = QValueAxis()
        self.value_axis.setLabelFormat("%d")
        self.value_axis.setLabelsColor(Qt.white)
        self.value_axis.setTitleText("Conteo")
        self.value_axis.setTitleBrush(Qt.white)

        self.chart.addAxis(self.categories_axis, Qt.AlignBottom)
        self.chart.addAxis(self.value_axis, Qt.AlignLeft)
        self.bar_series.attachAxis(self.categories_axis)
        self.bar_series.attachAxis(self.value_axis)

        self.chart_view = QChartView(self.chart)
        self.chart_view.setRenderHint(QPainter.Antialiasing, True)
        self.chart_view.setMinimumHeight(480)
        self.chart_view.setStyleSheet("background: rgba(32, 40, 62, 0.6);")

        self.bar_series.hovered.connect(self._on_bar_hover)

        controls_layout = QHBoxLayout()
        controls_layout.setSpacing(12)
        controls_layout.addStretch()
        start_label = QLabel("From")
        start_label.setStyleSheet("color: #B0BCD5; font-size: 14px;")
        controls_layout.addWidget(start_label)
        controls_layout.addWidget(self.start_date_edit)
        end_label = QLabel("To")
        end_label.setStyleSheet("color: #B0BCD5; font-size: 14px;")
        controls_layout.addWidget(end_label)
        controls_layout.addWidget(self.end_date_edit)
        timeframe_label = QLabel("Timeframe")
        timeframe_label.setStyleSheet("color: #B0BCD5; font-size: 14px;")
        controls_layout.addWidget(timeframe_label)
        controls_layout.addWidget(self._timeframe_combo)
        controls_layout.addWidget(self.refresh_button)
        controls_layout.addStretch()

        metrics_layout = QHBoxLayout()
        metrics_layout.setSpacing(16)

        self.samples_card, self.samples_value = self._create_metric_card("Samples", "#E0E8FF")
        self.tests_card, self.tests_value = self._create_metric_card("Tests", "#7EE787")
        self.customers_card, self.customers_value = self._create_metric_card("Customers", "#F4B400")
        self.reports_card, self.reports_value = self._create_metric_card("Reports", "#FF8FAB")
        self.tat_card, self.tat_value = self._create_metric_card("Avg TAT", "#60CDF1")

        for card in (
            self.samples_card,
            self.tests_card,
            self.customers_card,
            self.reports_card,
            self.tat_card,
        ):
            metrics_layout.addWidget(card, 1)

        content_layout = QVBoxLayout()
        content_layout.setSpacing(20)

        header_label = QLabel("MCRLabs Metrics")
        header_label.setAlignment(Qt.AlignLeft | Qt.AlignVCenter)
        header_label.setStyleSheet("color: #E0E8FF; font-size: 26px; font-weight: 700;")
        content_layout.addWidget(header_label)

        content_layout.addLayout(metrics_layout)

        status_layout = QHBoxLayout()
        status_layout.setContentsMargins(0, 0, 0, 0)
        status_layout.setSpacing(6)
        status_layout.setAlignment(Qt.AlignCenter)
        status_layout.addWidget(self.spinner_label)
        status_layout.addWidget(self.status_label)
        content_layout.addLayout(status_layout)

        content_layout.addLayout(controls_layout)
        content_layout.addWidget(self.chart_view)
        self._init_bottom_lists(content_layout)
        self._add_tat_section(content_layout)

        content_layout.addStretch()

        content_widget = QWidget()
        content_widget.setLayout(content_layout)
        content_widget.setStyleSheet("background-color: #0F172A;")

        scroll_area = QScrollArea()
        scroll_area.setWidgetResizable(True)
        scroll_area.setFrameShape(QFrame.NoFrame)
        scroll_area.setStyleSheet("QScrollArea { background-color: #0F172A; }")
        scroll_area.setWidget(content_widget)

        overview_container = QWidget()
        overview_container.setStyleSheet("background-color: #0F172A;")
        overview_layout = QVBoxLayout(overview_container)
        overview_layout.setContentsMargins(0, 0, 0, 0)
        overview_layout.setSpacing(0)
        overview_layout.addWidget(scroll_area)

        self.tabs = QTabWidget()
        self.tabs.setDocumentMode(True)
        self.tabs.setStyleSheet(
            "QTabWidget::pane { border: 0; } "
            "QTabBar::tab { background-color: #111C34; color: #B0BCD5; padding: 10px 18px; border-radius: 8px; } "
            "QTabBar::tab:selected { background-color: #1F3B73; color: white; }"
        )
        self.tabs.addTab(overview_container, "Overview")
        operational_tab = self._build_operational_tab()
        self._operational_tab_index = self.tabs.addTab(operational_tab, "Operational Efficiency")
        priority_tab = self._build_priority_orders_tab()
        self._priority_tab_index = self.tabs.addTab(priority_tab, "Priority Orders")
        self.tabs.currentChanged.connect(self._on_tab_changed)

        self.setCentralWidget(self.tabs)

    def _init_bottom_lists(self, parent_layout: QVBoxLayout) -> None:
        lists_layout = QHBoxLayout()
        lists_layout.setSpacing(16)
        self.new_customers_table = self._create_table_widget(["ID", "Name", "Created"])
        new_customers_panel = self._create_list_panel("New customers", self.new_customers_table)
        lists_layout.addWidget(new_customers_panel, 1)

        self.top_tests_table = self._create_table_widget(["ID", "Name", "Tests"])
        top_tests_panel = self._create_list_panel("Top 10 customers with more tests", self.top_tests_table)
        lists_layout.addWidget(top_tests_panel, 1)

        self.test_types_panel = self._create_test_types_panel()
        lists_layout.addWidget(self.test_types_panel, 1)
        parent_layout.addLayout(lists_layout)

    def _add_tat_section(self, parent_layout: QVBoxLayout) -> None:
        tat_panel = self._create_tat_panel()
        tat_panel.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        parent_layout.addWidget(tat_panel)

    def _create_metric_card(self, title: str, value_color: str) -> Tuple[QFrame, QLabel]:
        frame = QFrame()
        frame.setFrameShape(QFrame.StyledPanel)
        frame.setStyleSheet(
            "QFrame { background-color: #111C34; border: 1px solid #1F3B73; border-radius: 10px; }"
        )
        layout = QVBoxLayout(frame)
        layout.setContentsMargins(16, 16, 16, 16)
        layout.setSpacing(6)

        value_label = QLabel("--")
        value_label.setAlignment(Qt.AlignLeft | Qt.AlignBottom)
        value_label.setStyleSheet(f"font-size: 30px; font-weight: 600; color: {value_color};")

        title_label = QLabel(title)
        title_label.setAlignment(Qt.AlignLeft | Qt.AlignTop)
        title_label.setStyleSheet("color: #B0BCD5; font-size: 13px; font-weight: 500;")

        layout.addWidget(title_label)
        layout.addSpacing(4)
        layout.addWidget(value_label)
        layout.addStretch()

        return frame, value_label

    def _create_table_widget(self, headers: List[str]) -> QTableWidget:
        table = QTableWidget()
        table.setColumnCount(len(headers))
        table.setHorizontalHeaderLabels(headers)
        header = table.horizontalHeader()
        header.setStretchLastSection(False)
        for index in range(table.columnCount()):
            if index == 0:
                mode = QHeaderView.ResizeToContents
            elif index == table.columnCount() - 1:
                mode = QHeaderView.ResizeToContents
            else:
                mode = QHeaderView.Stretch
            header.setSectionResizeMode(index, mode)
        header.setDefaultAlignment(Qt.AlignLeft | Qt.AlignVCenter)
        header.setSectionsClickable(False)
        header.setHighlightSections(False)
        table.verticalHeader().setVisible(False)
        table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        table.setSelectionMode(QAbstractItemView.NoSelection)
        table.setFocusPolicy(Qt.NoFocus)
        table.setAlternatingRowColors(True)
        table.setStyleSheet("QTableWidget { background-color: #0F172A; alternate-background-color: #17233D; color: #E0E8FF; }")
        table.setMinimumHeight(200)
        return table

    def _create_list_panel(self, title: str, content_widget: QWidget) -> QFrame:
        frame = QFrame()
        frame.setFrameShape(QFrame.StyledPanel)
        frame.setStyleSheet("QFrame { background-color: #111C34; border: 1px solid #1F3B73; border-radius: 10px; }")
        layout = QVBoxLayout(frame)
        layout.setContentsMargins(16, 16, 16, 16)
        layout.setSpacing(12)
        title_label = QLabel(title)
        title_label.setStyleSheet("color: #E0E8FF; font-weight: 600; font-size: 14px;")
        layout.addWidget(title_label)
        layout.addWidget(content_widget)
        return frame

    def _create_chart_panel(self, title: str, chart_view: QChartView) -> QFrame:
        frame = QFrame()
        frame.setFrameShape(QFrame.StyledPanel)
        frame.setStyleSheet("QFrame { background-color: #111C34; border: 1px solid #1F3B73; border-radius: 10px; }")
        layout = QVBoxLayout(frame)
        layout.setContentsMargins(16, 16, 16, 16)
        layout.setSpacing(12)

        title_label = QLabel(title)
        title_label.setStyleSheet("color: #E0E8FF; font-weight: 600; font-size: 16px;")
        layout.addWidget(title_label)
        layout.addWidget(chart_view)
        return frame

    def _create_tat_panel(self) -> QFrame:
        frame = QFrame()
        frame.setFrameShape(QFrame.StyledPanel)
        frame.setStyleSheet("QFrame { background-color: #111C34; border: 1px solid #1F3B73; border-radius: 10px; }")

        layout = QVBoxLayout(frame)
        layout.setContentsMargins(16, 16, 16, 16)
        layout.setSpacing(12)

        title_label = QLabel("Daily TAT trend")
        title_label.setStyleSheet("color: #E0E8FF; font-weight: 600; font-size: 14px;")
        layout.addWidget(title_label)

        self.tat_chart = QChart()
        self.tat_chart.setBackgroundBrush(Qt.transparent)
        tat_legend = self.tat_chart.legend()
        tat_legend.setVisible(True)
        tat_legend.setLabelBrush(QBrush(Qt.white))
        tat_legend.setBackgroundVisible(False)

        self.tat_zero_series = QLineSeries()
        self.tat_zero_series.setName("")
        self.tat_zero_series.setVisible(False)

        self.tat_under_series = QLineSeries()
        self.tat_under_series.setName("")
        self.tat_under_series.setVisible(False)

        self.tat_line_series = QLineSeries()
        self.tat_line_series.setName("Daily avg")
        tat_pen = QPen(QColor("#60CDF1"))
        tat_pen.setWidth(2)
        self.tat_line_series.setPen(tat_pen)
        self.tat_line_series.hovered.connect(self._on_tat_point_hover)

        self.tat_threshold_series = QLineSeries()
        self.tat_threshold_series.setName("Target")
        threshold_pen = QPen(QColor("#FFB347"))
        threshold_pen.setWidth(2)
        threshold_pen.setStyle(Qt.DashLine)
        self.tat_threshold_series.setPen(threshold_pen)

        self.tat_over_series = QLineSeries()
        self.tat_over_series.setName("")
        self.tat_over_series.setVisible(False)

        self.tat_moving_avg_series = QLineSeries()
        self.tat_moving_avg_series.setName("7d moving avg")
        moving_pen = QPen(QColor("#9A7FF0"))
        moving_pen.setWidth(2)
        moving_pen.setStyle(Qt.DashDotLine)
        self.tat_moving_avg_series.setPen(moving_pen)

        self.tat_previous_series = QLineSeries()
        self.tat_previous_series.setName("Previous period")
        previous_pen = QPen(QColor("#FF8FAB"))
        previous_pen.setWidth(2)
        previous_pen.setStyle(Qt.DotLine)
        self.tat_previous_series.setPen(previous_pen)
        self.tat_previous_series.setVisible(False)

        self.tat_under_area = QAreaSeries(self.tat_under_series, self.tat_zero_series)
        self.tat_under_area.setName("Within target")
        under_gradient = QLinearGradient(0.0, 0.0, 0.0, 1.0)
        under_gradient.setCoordinateMode(QGradient.ObjectBoundingMode)
        under_gradient.setColorAt(0.0, QColor(0x4C, 0xAF, 0x50, 180))
        under_gradient.setColorAt(1.0, QColor(0x4C, 0xAF, 0x50, 40))
        self.tat_under_area.setBrush(QBrush(under_gradient))
        self.tat_under_area.setPen(QPen(QColor(0x4C, 0xAF, 0x50, 160)))

        self.tat_over_area = QAreaSeries(self.tat_over_series, self.tat_threshold_series)
        self.tat_over_area.setName("Above target")
        over_gradient = QLinearGradient(0.0, 0.0, 0.0, 1.0)
        over_gradient.setCoordinateMode(QGradient.ObjectBoundingMode)
        over_gradient.setColorAt(0.0, QColor(0xE5, 0x73, 0x73, 200))
        over_gradient.setColorAt(1.0, QColor(0xE5, 0x73, 0x73, 60))
        self.tat_over_area.setBrush(QBrush(over_gradient))
        self.tat_over_area.setPen(QPen(QColor(0xE5, 0x73, 0x73, 180)))

        self.tat_axis_x = QDateTimeAxis()
        self.tat_axis_x.setFormat("MMM dd")
        self.tat_axis_x.setLabelsColor(Qt.white)
        self.tat_axis_x.setTitleText("Date")
        self.tat_axis_x.setTitleBrush(Qt.white)

        self.tat_axis_y = QValueAxis()
        self.tat_axis_y.setLabelFormat("%.1f")
        self.tat_axis_y.setLabelsColor(Qt.white)
        self.tat_axis_y.setTitleText("Hours")
        self.tat_axis_y.setTitleBrush(Qt.white)

        self.tat_chart.addSeries(self.tat_under_area)
        self.tat_chart.addSeries(self.tat_over_area)
        self.tat_chart.addSeries(self.tat_line_series)
        self.tat_chart.addSeries(self.tat_moving_avg_series)
        self.tat_chart.addSeries(self.tat_threshold_series)
        self.tat_chart.addSeries(self.tat_previous_series)

        self.tat_chart.addAxis(self.tat_axis_x, Qt.AlignBottom)
        self.tat_chart.addAxis(self.tat_axis_y, Qt.AlignLeft)
        for series in (
            self.tat_under_area,
            self.tat_over_area,
            self.tat_line_series,
            self.tat_moving_avg_series,
            self.tat_threshold_series,
            self.tat_previous_series,
        ):
            series.attachAxis(self.tat_axis_x)
            series.attachAxis(self.tat_axis_y)

        self.tat_chart_view = QChartView(self.tat_chart)
        self.tat_chart_view.setRenderHint(QPainter.Antialiasing, True)
        self.tat_chart_view.setMinimumHeight(450)
        self.tat_chart_view.setStyleSheet("background: rgba(32, 40, 62, 0.6);")
        layout.addWidget(self.tat_chart_view)

        controls_layout = QHBoxLayout()
        controls_layout.setContentsMargins(0, 0, 0, 0)
        controls_layout.addStretch()
        self.tat_compare_checkbox = QCheckBox("Show previous period")
        self.tat_compare_checkbox.setStyleSheet("color: #B0BCD5;")
        self.tat_compare_checkbox.setEnabled(False)
        self.tat_compare_checkbox.toggled.connect(self._on_tat_compare_toggled)
        controls_layout.addWidget(self.tat_compare_checkbox)
        layout.addLayout(controls_layout)

        frame.setMinimumHeight(520)
        return frame

    def _create_test_types_panel(self) -> QFrame:
        frame = QFrame()
        frame.setFrameShape(QFrame.StyledPanel)
        frame.setStyleSheet("QFrame { background-color: #111C34; border: 1px solid #1F3B73; border-radius: 10px; }")

        layout = QVBoxLayout(frame)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(4)

        title_label = QLabel("Types of tests most requested")
        title_label.setStyleSheet("color: #E0E8FF; font-weight: 600; font-size: 14px;")
        layout.addWidget(title_label)

        self.test_types_chart = QChart()
        self.test_types_chart.setBackgroundBrush(Qt.transparent)
        self.test_types_chart.setBackgroundRoundness(0)
        distribution_legend = self.test_types_chart.legend()
        distribution_legend.setVisible(True)
        distribution_legend.setBackgroundVisible(False)
        distribution_legend.setLabelBrush(QBrush(Qt.white))

        self.test_types_series = QHorizontalBarSeries()
        self.test_types_series.setLabelsVisible(False)

        self.test_types_set = QBarSet("Tests")
        self.test_types_set.setColor(QColor(0x7E, 0xE7, 0x87))
        self.test_types_set.hovered.connect(lambda status, index: self._on_test_type_bar_hover(status, index))
        self.test_types_series.append(self.test_types_set)

        self.test_types_chart.addSeries(self.test_types_series)
        self.test_types_chart.legend().setAlignment(Qt.AlignTop)

        self.test_types_axis_values = QValueAxis()
        self.test_types_axis_values.setLabelFormat("%d")
        self.test_types_axis_values.setLabelsColor(Qt.white)
        self.test_types_axis_values.setTitleText("Count")
        self.test_types_axis_values.setTitleBrush(Qt.white)

        self.test_types_axis_categories = QBarCategoryAxis()
        self.test_types_axis_categories.setLabelsColor(Qt.white)

        self.test_types_chart.addAxis(self.test_types_axis_values, Qt.AlignBottom)
        self.test_types_chart.addAxis(self.test_types_axis_categories, Qt.AlignLeft)
        self.test_types_series.attachAxis(self.test_types_axis_values)
        self.test_types_series.attachAxis(self.test_types_axis_categories)


        self.test_types_chart.setMargins(QMargins(20, 10, 20, 10))
        if self.test_types_chart.layout() is not None:
            self.test_types_chart.layout().setContentsMargins(0, 0, 0, 0)

        self.test_types_chart_view = QChartView(self.test_types_chart)
        self.test_types_chart_view.setRenderHint(QPainter.Antialiasing, True)
        self.test_types_chart_view.setMinimumHeight(420)
        self.test_types_chart_view.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self.test_types_chart_view.setStyleSheet("background: rgba(32, 40, 62, 0.6);")

        chart_container = QWidget()
        chart_container_layout = QVBoxLayout(chart_container)
        chart_container_layout.setContentsMargins(0, 0, 0, 0)
        chart_container_layout.setSpacing(0)
        chart_container_layout.addWidget(self.test_types_chart_view)

        self.test_types_scroll = QScrollArea()
        self.test_types_scroll.setWidgetResizable(True)
        self.test_types_scroll.setFrameShape(QFrame.NoFrame)
        self.test_types_scroll.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        self.test_types_scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self.test_types_scroll.setStyleSheet("QScrollArea { background-color: transparent; }")
        self.test_types_scroll.setWidget(chart_container)


        self.test_types_empty_label = QLabel("No test data for the selected range")
        self.test_types_empty_label.setAlignment(Qt.AlignCenter)
        self.test_types_empty_label.setStyleSheet("color: #5F718F; font-size: 13px;")
        self.test_types_empty_label.setVisible(False)

        layout.addWidget(self.test_types_scroll)
        layout.addWidget(self.test_types_empty_label)

        frame.setMinimumHeight(480)
        return frame

    def _on_tat_compare_toggled(self, checked: bool) -> None:
        if not hasattr(self, "tat_previous_series"):
            return
        has_points = self.tat_previous_series and self.tat_previous_series.count() > 0
        self.tat_previous_series.setVisible(bool(checked and has_points))

    def _on_tat_point_hover(self, point, state: bool) -> None:
        if not state:
            QToolTip.hideText()
            return
        if point is None:
            QToolTip.hideText()
            return
        timestamp = int(round(point.x()))
        data = self._tat_tooltip_data.get(timestamp)
        if not data:
            QToolTip.hideText()
            return
        dt_value, avg_seconds, test_count = data
        date_text = dt_value.strftime("%Y-%m-%d")
        hours = int(avg_seconds // 3600)
        minutes = int((avg_seconds % 3600) // 60)
        tooltip = f"{date_text}\nAvg TAT: {hours:02d}h {minutes:02d}m"
        if test_count:
            tooltip += f"\nTests: {test_count}"
        QToolTip.showText(QCursor.pos(), tooltip, self.tat_chart_view)

    def _update_tat_chart(
        self,
        daily_data: Optional[List[Dict[str, Any]]],
        previous_data: Optional[List[Dict[str, Any]]],
    ) -> None:
        points = self._normalize_tat_data(daily_data)
        previous_points = self._normalize_tat_data(previous_data)

        for series in (
            self.tat_line_series,
            self.tat_under_series,
            self.tat_zero_series,
            self.tat_over_series,
            self.tat_threshold_series,
            self.tat_moving_avg_series,
            self.tat_previous_series,
        ):
            series.clear()
        self._tat_tooltip_data.clear()

        target_hours = self._tat_target_seconds / 3600.0
        if not points:
            now = datetime.now(timezone.utc)
            start = now - timedelta(days=6)
            q_start = QDateTime(start)
            q_end = QDateTime(now)
            self.tat_axis_x.setRange(q_start, q_end)
            max_hours = max(target_hours, 1.0)
            self.tat_axis_y.setRange(0.0, max_hours)
            self.tat_compare_checkbox.setEnabled(False)
            self.tat_previous_series.setVisible(False)
            return

        timestamps: List[Tuple[int, float, float, int, datetime]] = []
        for dt_value, avg_seconds, test_count in points:
            qdt = QDateTime(dt_value)
            timestamp = qdt.toMSecsSinceEpoch()
            value_hours = avg_seconds / 3600.0
            self.tat_line_series.append(timestamp, value_hours)
            self.tat_zero_series.append(timestamp, 0.0)
            under_value = min(value_hours, target_hours)
            self.tat_under_series.append(timestamp, under_value)
            self.tat_threshold_series.append(timestamp, target_hours)
            over_value = value_hours if value_hours > target_hours else target_hours
            self.tat_over_series.append(timestamp, over_value)
            self._tat_tooltip_data[int(timestamp)] = (dt_value, avg_seconds, test_count)
            timestamps.append((timestamp, value_hours, avg_seconds, test_count, dt_value))

        moving_window: List[float] = []
        for timestamp, value_hours, avg_seconds, _, _ in timestamps:
            moving_window.append(avg_seconds)
            if len(moving_window) > self._tat_moving_average_window:
                moving_window.pop(0)
            moving_avg = sum(moving_window) / len(moving_window)
            self.tat_moving_avg_series.append(timestamp, moving_avg / 3600.0)

        min_dt = points[0][0]
        max_dt = points[-1][0]
        self.tat_axis_x.setRange(QDateTime(min_dt), QDateTime(max_dt))

        max_hours = max(target_hours, max(value_hours for _, value_hours, _, _, _ in timestamps))
        self.tat_axis_y.setRange(0.0, max(1.0, max_hours * 1.2))

        self.tat_previous_series.clear()
        if previous_points and timestamps:
            prev_values_hours = [avg_seconds / 3600.0 for _, avg_seconds, _ in previous_points]
            for index, (timestamp, _, _, _, _) in enumerate(timestamps):
                if index >= len(prev_values_hours):
                    break
                self.tat_previous_series.append(timestamp, prev_values_hours[index])

        has_previous = self.tat_previous_series.count() > 0
        self.tat_compare_checkbox.setEnabled(has_previous)
        if not has_previous:
            self.tat_compare_checkbox.setChecked(False)
        self.tat_previous_series.setVisible(self.tat_compare_checkbox.isChecked() and has_previous)

    def _update_test_type_chart(self, distribution: Optional[Sequence[Dict[str, Any]]]) -> None:
        if not hasattr(self, "test_types_set"):
            return
        filtered: List[Tuple[str, int]] = []
        if distribution:
            for item in distribution:
                if not isinstance(item, dict):
                    continue
                label = item.get("label") or item.get("label_abbr")
                if not isinstance(label, str):
                    continue
                try:
                    count = int(item.get("count") or 0)
                except (TypeError, ValueError):
                    continue
                if count <= 0:
                    continue
                filtered.append((label, count))

        filtered.sort(key=lambda entry: entry[1], reverse=True)

        chart = getattr(self, "test_types_chart", None)
        series = getattr(self, "test_types_series", None)
        axis_values = getattr(self, "test_types_axis_values", None)
        axis_categories = getattr(self, "test_types_axis_categories", None)

        if chart is None or series is None or axis_values is None or axis_categories is None:
            return

        try:
            series.clear()
        except (RuntimeError, AttributeError):
            series = QHorizontalBarSeries()
            chart.addSeries(series)
            self.test_types_series = series

        self.test_types_set = QBarSet("Tests")
        self.test_types_set.setColor(QColor(0x7E, 0xE7, 0x87))
        self.test_types_set.hovered.connect(lambda status, index: self._on_test_type_bar_hover(status, index))
        self.test_types_series.append(self.test_types_set)

        attached_axes = {axis for axis in self.test_types_series.attachedAxes()}
        if axis_values not in attached_axes:
            if axis_values not in chart.axes():
                chart.addAxis(axis_values, Qt.AlignBottom)
            self.test_types_series.attachAxis(axis_values)
        if axis_categories not in attached_axes:
            if axis_categories not in chart.axes():
                chart.addAxis(axis_categories, Qt.AlignLeft)
            self.test_types_series.attachAxis(axis_categories)

        axis_categories.clear()
        self._test_type_categories = []

        if not filtered:
            if hasattr(self, "test_types_scroll"):
                self.test_types_scroll.setVisible(False)
            self.test_types_empty_label.setVisible(True)
            self.test_types_axis_values.setRange(0, 1)
            return

        if hasattr(self, "test_types_scroll"):
            self.test_types_scroll.setVisible(True)
        self.test_types_empty_label.setVisible(False)

        categories = [label for label, _ in filtered]
        # Reverse so the highest value appears at the top of the horizontal bars
        categories_reversed = list(reversed(categories))
        counts_reversed = list(reversed([count for _, count in filtered]))

        for value in counts_reversed:
            self.test_types_set.append(float(value))

        self.test_types_axis_categories.append(categories_reversed)
        max_value = max(counts_reversed) if counts_reversed else 1
        self.test_types_axis_values.setRange(0, max_value * 1.1 if max_value > 0 else 1)

        self._test_type_categories = categories_reversed

    def _on_test_type_bar_hover(self, status: bool, index: int) -> None:
        if not status or index < 0:
            QToolTip.hideText()
            return
        if index >= len(self._test_type_categories):
            QToolTip.hideText()
            return
        try:
            value = int(round(self.test_types_set.at(index)))
        except (IndexError, RuntimeError):
            QToolTip.hideText()
            return
        label = self._test_type_categories[index]
        QToolTip.showText(QCursor.pos(), f"{label}: {value}", self.test_types_chart_view)

    def _normalize_tat_data(
        self,
        payload: Optional[List[Dict[str, Any]]],
    ) -> List[Tuple[datetime, float, int]]:
        normalized: List[Tuple[datetime, float, int]] = []
        if not payload:
            return normalized
        for item in payload:
            if not isinstance(item, dict):
                continue
            dt_value = self._coerce_datetime(item.get("date"))
            if not dt_value:
                continue
            avg_seconds = float(item.get("average_seconds") or 0.0)
            test_count = int(item.get("test_count") or 0)
            normalized.append((dt_value, avg_seconds, test_count))
        normalized.sort(key=lambda entry: entry[0])
        return normalized

    @staticmethod
    def _coerce_datetime(value: Any) -> Optional[datetime]:
        if isinstance(value, datetime):
            return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return None
            if text.endswith("Z"):
                text = text[:-1] + "+00:00"
            try:
                parsed = datetime.fromisoformat(text)
            except ValueError:
                return None
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
        return None

    def _update_top_tests(self, records: List[Dict[str, Any]]) -> None:
        table = self.top_tests_table
        table.clearContents()
        table.setRowCount(0)
        data = list(records or [])[:10]
        if not data:
            table.setRowCount(1)
            table.setSpan(0, 0, 1, table.columnCount())
            item = QTableWidgetItem("No data")
            item.setTextAlignment(Qt.AlignCenter)
            table.setItem(0, 0, item)
            return
        table.setRowCount(len(data))
        for row, record in enumerate(data):
            identifier = record.get("id")
            name = record.get("name") or ""
            test_count = int(record.get("test_count") or 0)
            id_item = QTableWidgetItem(str(identifier) if identifier is not None else "")
            name_item = QTableWidgetItem(str(name))
            tests_item = QTableWidgetItem(str(test_count))
            id_item.setFlags(id_item.flags() & ~Qt.ItemIsEditable)
            name_item.setFlags(name_item.flags() & ~Qt.ItemIsEditable)
            tests_item.setFlags(tests_item.flags() & ~Qt.ItemIsEditable)
            table.setItem(row, 0, id_item)
            table.setItem(row, 1, name_item)
            table.setItem(row, 2, tests_item)
        table.resizeRowsToContents()

    def _update_new_customers(self, customers: List[Dict[str, Any]]) -> None:
        table = self.new_customers_table
        table.clearContents()
        table.setRowCount(0)
        fallback = datetime.min.replace(tzinfo=timezone.utc)
        records = list(customers or [])

        def sort_key(item: Dict[str, Any]) -> datetime:
            value = item.get("date_created") if isinstance(item, dict) else None
            if isinstance(value, datetime):
                return value
            return fallback

        records.sort(key=sort_key, reverse=True)
        records = records[:10]
        if not records:
            table.setRowCount(1)
            table.setSpan(0, 0, 1, table.columnCount())
            item = QTableWidgetItem("No recent customers")
            item.setTextAlignment(Qt.AlignCenter)
            table.setItem(0, 0, item)
            return
        table.setRowCount(len(records))
        for row, record in enumerate(records):
            identifier = record.get("id")
            name = record.get("name") or ""
            created = self._format_timestamp(record.get("date_created"))
            id_item = QTableWidgetItem(str(identifier) if identifier is not None else "")
            name_item = QTableWidgetItem(str(name))
            created_item = QTableWidgetItem(created)
            id_item.setFlags(id_item.flags() & ~Qt.ItemIsEditable)
            name_item.setFlags(name_item.flags() & ~Qt.ItemIsEditable)
            created_item.setFlags(created_item.flags() & ~Qt.ItemIsEditable)
            table.setItem(row, 0, id_item)
            table.setItem(row, 1, name_item)
            table.setItem(row, 2, created_item)
        table.resizeRowsToContents()

    @staticmethod
    def _format_timestamp(value: Any) -> str:
        if isinstance(value, datetime):
            dt = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
            dt = dt.astimezone(timezone.utc)
            return dt.strftime('%Y-%m-%d')
        if isinstance(value, str):
            return value
        return ''

    def _apply_dark_palette(self) -> None:
        palette = QPalette()
        palette.setColor(QPalette.Window, Qt.black)
        palette.setColor(QPalette.WindowText, Qt.white)
        palette.setColor(QPalette.Base, Qt.black)
        palette.setColor(QPalette.AlternateBase, Qt.black)
        palette.setColor(QPalette.ToolTipBase, Qt.white)
        palette.setColor(QPalette.ToolTipText, Qt.black)
        palette.setColor(QPalette.Text, Qt.white)
        palette.setColor(QPalette.Button, Qt.black)
        palette.setColor(QPalette.ButtonText, Qt.white)
        palette.setColor(QPalette.Highlight, Qt.darkBlue)
        palette.setColor(QPalette.HighlightedText, Qt.white)
        self.setPalette(palette)

    def _create_date_edit(self) -> QDateEdit:
        date_edit = QDateEdit()
        date_edit.setCalendarPopup(True)
        date_edit.setDisplayFormat("yyyy-MM-dd")
        date_edit.setStyleSheet(
            "padding: 10px; font-size: 14px; background-color: #1E2A44; color: #E0E8FF; border: 1px solid #1F3B73; border-radius: 6px;"
        )
        date_edit.setMinimumWidth(130)

        calendar = date_edit.calendarWidget()
        calendar.setLocale(QLocale(QLocale.English, QLocale.UnitedStates))
        calendar.setFirstDayOfWeek(Qt.Sunday)
        calendar.setHorizontalHeaderFormat(QCalendarWidget.SingleLetterDayNames)
        calendar.setMinimumWidth(280)
        calendar.updateGeometry()

        return date_edit

    def _initialize_default_range(self) -> None:
        today = QDate.currentDate()
        self.end_date_edit.setMaximumDate(today)
        self.end_date_edit.setDate(today)
        self.start_date_edit.setMaximumDate(today)
        self.start_date_edit.setDate(today.addDays(-6))

    def _determine_timeframe_mode(self, start_dt: datetime, end_dt: datetime) -> str:
        try:
            start_date = start_dt.date()
            end_date = end_dt.date()
        except AttributeError:
            return "daily"
        days = max(1, (end_date - start_date).days + 1)
        if days <= 14:
            return "daily"
        if days <= 92:
            return "weekly"
        return "monthly"

    def _set_timeframe_selection(self, mode: str, *, programmatic: bool = False) -> None:
        normalized = mode if mode in {"daily", "weekly", "monthly"} else "daily"
        if programmatic:
            self._timeframe_combo.blockSignals(True)
        index = self._timeframe_combo.findData(normalized)
        if index >= 0:
            self._timeframe_combo.setCurrentIndex(index)
        if programmatic:
            self._timeframe_combo.blockSignals(False)
        self._timeframe_mode = normalized

    def _apply_default_timeframe(self, start_dt: datetime, end_dt: datetime) -> None:
        recommended = self._determine_timeframe_mode(start_dt, end_dt)
        self._timeframe_manual_override = False
        self._set_timeframe_selection(recommended, programmatic=True)

    def _get_timeframe_label(self, mode: Optional[str] = None) -> str:
        target = mode or self._timeframe_mode
        index = self._timeframe_combo.findData(target)
        if index >= 0:
            return self._timeframe_combo.itemText(index)
        return (target or "Daily").title()

    def _begin_data_fetch(self, start_dt: datetime, end_dt: datetime) -> None:
        if self._loading:
            return
        self._set_loading(True)
        status_message = "Updating..."
        range_text = self._format_range(start_dt, end_dt)
        if range_text:
            status_message += f" Range: {range_text}"
        timeframe_label = self._get_timeframe_label()
        if timeframe_label:
            status_message += f" | Timeframe: {timeframe_label}"
        self._update_status(status_message)

        self._thread = QThread(self)
        self._worker = SummaryWorker(
            self._client,
            start_date=start_dt,
            end_date=end_dt,
            timeframe=self._timeframe_mode,
        )
        self._worker.moveToThread(self._thread)
        self._thread.started.connect(self._worker.process)
        self._worker.finished.connect(self._thread.quit)
        self._worker.error.connect(self._thread.quit)
        self._thread.finished.connect(self._thread.deleteLater)
        self._worker.finished.connect(self._worker.deleteLater)
        self._worker.error.connect(self._worker.deleteLater)
        self._worker.finished.connect(self._on_worker_finished)
        self._worker.error.connect(self._on_worker_error)
        self._thread.finished.connect(self._on_thread_finished)
        self._thread.start()

    def refresh_data(self) -> None:
        if self._loading:
            return
        try:
            start_dt, end_dt = self._get_selected_range()
        except ValueError as exc:
            self._show_error(str(exc))
            return

        self._apply_default_timeframe(start_dt, end_dt)
        self._begin_data_fetch(start_dt, end_dt)

    def _restart_with_current_range(self) -> None:
        if self._loading:
            return
        try:
            start_dt, end_dt = self._get_selected_range()
        except ValueError as exc:
            self._show_error(str(exc))
            return
        self._begin_data_fetch(start_dt, end_dt)

    def _on_timeframe_changed(self, index: int) -> None:
        mode = self._timeframe_combo.itemData(index)
        if mode not in {"daily", "weekly", "monthly"}:
            return
        previous = self._timeframe_mode
        self._timeframe_mode = mode
        if mode == previous and self._timeframe_manual_override:
            return
        self._timeframe_manual_override = True
        self._restart_with_current_range()

    def _on_worker_finished(self, summary: Dict[str, object]) -> None:
        self._apply_summary(summary)

    def _on_worker_error(self, message: str) -> None:
        self._show_error(message)

    def _on_thread_finished(self) -> None:
        self._set_loading(False)
        self._worker = None
        self._thread = None





    def _apply_summary(self, summary: Dict[str, object]) -> None:
        samples_total = int(summary.get("samples_total", 0) or 0)
        self.samples_value.setText(str(samples_total))

        tests_total = int(summary.get("tests_total", 0) or 0)
        self.tests_value.setText(str(tests_total))

        customers_total = int(summary.get("customers_total", 0) or 0)
        self.customers_value.setText(str(customers_total))

        reports_total = int(summary.get("reports_total", 0) or 0)
        self.reports_value.setText(str(reports_total))

        tat_seconds = float(summary.get("tests_tat_average_seconds", 0.0) or 0.0)
        tat_count = int(summary.get("tests_tat_count", 0) or 0)
        self.tat_value.setText(self._format_tat(tat_seconds, tat_count))

        start_dt = summary.get("start_date")
        end_dt = summary.get("end_date")
        customers_recent = summary.get("customers_recent")
        if isinstance(customers_recent, list):
            self._update_new_customers(customers_recent)
        else:
            self._update_new_customers([])

        tests_leaderboard = summary.get("customer_test_totals")
        if isinstance(tests_leaderboard, list):
            self._update_top_tests(tests_leaderboard)
        else:
            self._update_top_tests([])

        label_distribution = summary.get("tests_label_distribution")
        if isinstance(label_distribution, list):
            self._update_test_type_chart(label_distribution)
        else:
            self._update_test_type_chart([])

        tat_daily = summary.get("tests_tat_daily")
        tat_previous = summary.get("tests_tat_daily_previous")
        daily_list = tat_daily if isinstance(tat_daily, list) else []
        previous_list = tat_previous if isinstance(tat_previous, list) else []
        self._update_tat_chart(daily_list, previous_list)

        timeframe_value = summary.get("timeframe_mode")
        if isinstance(timeframe_value, str):
            self._current_timeframe_mode = timeframe_value
        else:
            self._current_timeframe_mode = self._timeframe_mode

        range_text = self._format_range(start_dt, end_dt)
        now = datetime.now(timezone.utc)
        status_parts = [f"Last update: {now.strftime('%Y-%m-%d %H:%M:%S UTC')}"]
        if range_text:
            status_parts.append(f"Range: {range_text}")
        timeframe_label = self._get_timeframe_label(self._current_timeframe_mode)
        if timeframe_label:
            status_parts.append(f"Timeframe: {timeframe_label}")
        self._update_status(" | ".join(status_parts))

        samples_series = summary.get("samples_series") or []
        tests_series = summary.get("tests_series") or []

        bucket_counts: Dict[datetime, List[int]] = {}
        for dt_value, count in samples_series:
            normalized = self._ensure_utc_datetime(dt_value)
            if not normalized:
                continue
            bucket_counts.setdefault(normalized, [0, 0])[0] = int(count)
        for dt_value, count in tests_series:
            normalized = self._ensure_utc_datetime(dt_value)
            if not normalized:
                continue
            bucket_counts.setdefault(normalized, [0, 0])[1] = int(count)

        self.samples_set.remove(0, self.samples_set.count())
        self.tests_set.remove(0, self.tests_set.count())

        sorted_buckets = sorted(bucket_counts.keys())
        category_labels = []
        max_value = 1
        for bucket in sorted_buckets:
            sample_count, test_count = bucket_counts[bucket]
            category_labels.append(self._format_category_label(bucket, self._current_timeframe_mode))
            self.samples_set.append(float(sample_count))
            self.tests_set.append(float(test_count))
            max_value = max(max_value, sample_count, test_count)

        if not category_labels:
            reference = datetime.now(timezone.utc)
            category_labels = [self._format_category_label(reference, self._current_timeframe_mode)]
            self.samples_set.append(0.0)
            self.tests_set.append(0.0)
            max_value = 1

        self.categories_axis.clear()
        self.categories_axis.append(category_labels)
        axis_title = {
            "daily": "Fecha",
            "weekly": "Semana",
            "monthly": "Mes",
        }.get(self._current_timeframe_mode, "Fecha")
        self.categories_axis.setTitleText(axis_title)
        self.bar_series.setBarWidth(0.4)
        self.value_axis.setRange(0, max_value + 1)

    def _on_bar_hover(self, status: bool, index: int, bar_set: QBarSet) -> None:
        if not status or index < 0:
            QToolTip.hideText()
            return
        categories = self.categories_axis.categories() if hasattr(self.categories_axis, "categories") else []
        if not categories or index >= len(categories):
            QToolTip.hideText()
            return
        value = int(bar_set.at(index))
        label = bar_set.label() or ""
        category = categories[index]
        QToolTip.showText(QCursor.pos(), f"{label}: {value} ({category})", self.chart_view)

    @staticmethod
    def _ensure_utc_datetime(value: Any) -> Optional[datetime]:
        if isinstance(value, datetime):
            return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return None

    def _format_category_label(self, instant: datetime, mode: str) -> str:
        normalized = self._ensure_utc_datetime(instant) or datetime.now(timezone.utc)
        if mode == "monthly":
            return normalized.strftime("%b %Y")
        if mode == "weekly":
            return f"Wk of {normalized.strftime('%b %d')}"
        return normalized.strftime("%b %d")

    def _get_selected_range(self) -> Tuple[datetime, datetime]:
        start_qdate = self.start_date_edit.date()
        end_qdate = self.end_date_edit.date()
        start_dt = datetime(
            start_qdate.year(),
            start_qdate.month(),
            start_qdate.day(),
            tzinfo=timezone.utc,
        )
        end_dt = datetime(
            end_qdate.year(),
            end_qdate.month(),
            end_qdate.day(),
            23,
            59,
            59,
            999999,
            tzinfo=timezone.utc,
        )
        if end_dt < start_dt:
            raise ValueError("Start date cannot be after the end date.")
        return start_dt, end_dt

    def _advance_spinner(self) -> None:
        if not self.spinner_label.isVisible():
            return
        self._spinner_index = (self._spinner_index + 1) % len(self._spinner_frames)
        self.spinner_label.setText(self._spinner_frames[self._spinner_index])

    @staticmethod
    def _format_tat(seconds: float, count: int) -> str:
        if count <= 0 or seconds <= 0:
            return "--"
        total_seconds = int(seconds)
        days, remainder = divmod(total_seconds, 86400)
        hours, _ = divmod(remainder, 3600)
        return f"{days} d {hours} h"

    def _format_range(self, start: Optional[datetime], end: Optional[datetime]) -> str:
        if not isinstance(start, datetime) or not isinstance(end, datetime):
            return ""
        return f"{start.strftime('%Y-%m-%d')} - {end.strftime('%Y-%m-%d')}"

    def _build_operational_tab(self) -> QWidget:
        tab = QWidget()
        tab.setStyleSheet("background-color: #0F172A;")

        content_widget = QWidget()
        content_widget.setStyleSheet("background-color: #0F172A;")
        content_layout = QVBoxLayout(content_widget)
        content_layout.setContentsMargins(24, 24, 24, 24)
        content_layout.setSpacing(20)

        header_label = QLabel("Operational Efficiency")
        header_label.setAlignment(Qt.AlignLeft | Qt.AlignVCenter)
        header_label.setStyleSheet("color: #E0E8FF; font-size: 26px; font-weight: 700;")
        content_layout.addWidget(header_label)

        self.op_spinner_label = QLabel("")
        self.op_spinner_label.setAlignment(Qt.AlignCenter)
        self.op_spinner_label.setStyleSheet("color: #7EE787; font-size: 14px;")
        self.op_spinner_label.setVisible(False)

        self.op_status_label = QLabel("Ready")
        self.op_status_label.setAlignment(Qt.AlignCenter)
        self.op_status_label.setStyleSheet("color: #B0BCD5;")

        self._operational_spinner_frames = ["|", "/", "-", "\\"]
        self._operational_spinner_index = 0
        self._operational_spinner_timer = QTimer(self)
        self._operational_spinner_timer.setInterval(120)
        self._operational_spinner_timer.timeout.connect(self._advance_operational_spinner)

        self.op_start_date_edit = self._create_date_edit()
        self.op_end_date_edit = self._create_date_edit()
        self._initialize_operational_range()

        self._operational_timeframe_combo = QComboBox()
        self._operational_timeframe_combo.setStyleSheet(
            "padding: 8px 12px; font-size: 14px; background-color: #1E2A44; color: #E0E8FF; "
            "border: 1px solid #1F3B73; border-radius: 6px;"
        )
        self._operational_timeframe_combo.setMinimumWidth(140)
        self._operational_timeframe_combo.addItem("Daily", "day")
        self._operational_timeframe_combo.addItem("Weekly", "week")
        self._operational_timeframe_combo.addItem("Monthly", "month")
        self._operational_timeframe_combo.setCurrentIndex(1)
        self._operational_timeframe_combo.currentIndexChanged.connect(self._on_operational_timeframe_changed)
        self._operational_timeframe_mode = self._operational_timeframe_combo.currentData()
        self._operational_current_timeframe_mode = self._operational_timeframe_mode  # type: ignore[assignment]

        self.op_refresh_button = QPushButton("Refresh")
        self.op_refresh_button.clicked.connect(self.refresh_operational_data)
        self.op_refresh_button.setFixedWidth(140)
        self.op_refresh_button.setStyleSheet(
            "padding: 12px; font-size: 16px; background-color: #1F3B73; color: white; border-radius: 6px;"
        )

        metrics_layout = QHBoxLayout()
        metrics_layout.setSpacing(16)
        self.op_lead_avg_card, self.op_lead_avg_value = self._create_metric_card("Avg Lead Time", "#60CDF1")
        self.op_lead_median_card, self.op_lead_median_value = self._create_metric_card("Median Lead Time", "#9A7FF0")
        self.op_orders_completed_card, self.op_orders_completed_value = self._create_metric_card("Orders Completed", "#7EE787")
        self.op_samples_completed_card, self.op_samples_completed_value = self._create_metric_card("Samples Completed", "#F4B400")
        for card in (
            self.op_lead_avg_card,
            self.op_lead_median_card,
            self.op_orders_completed_card,
            self.op_samples_completed_card,
        ):
            metrics_layout.addWidget(card, 1)
        content_layout.addLayout(metrics_layout)

        status_layout = QHBoxLayout()
        status_layout.setContentsMargins(0, 0, 0, 0)
        status_layout.setSpacing(6)
        status_layout.setAlignment(Qt.AlignCenter)
        status_layout.addWidget(self.op_spinner_label)
        status_layout.addWidget(self.op_status_label)
        content_layout.addLayout(status_layout)

        controls_layout = QHBoxLayout()
        controls_layout.setSpacing(12)
        controls_layout.addStretch()
        start_label = QLabel("From")
        start_label.setStyleSheet("color: #B0BCD5; font-size: 14px;")
        controls_layout.addWidget(start_label)
        controls_layout.addWidget(self.op_start_date_edit)
        end_label = QLabel("To")
        end_label.setStyleSheet("color: #B0BCD5; font-size: 14px;")
        controls_layout.addWidget(end_label)
        controls_layout.addWidget(self.op_end_date_edit)
        timeframe_label = QLabel("Interval")
        timeframe_label.setStyleSheet("color: #B0BCD5; font-size: 14px;")
        controls_layout.addWidget(timeframe_label)
        controls_layout.addWidget(self._operational_timeframe_combo)
        controls_layout.addWidget(self.op_refresh_button)
        controls_layout.addStretch()
        content_layout.addLayout(controls_layout)

        self.op_throughput_chart = QChart()
        self.op_throughput_chart.setBackgroundBrush(Qt.transparent)
        throughput_legend = self.op_throughput_chart.legend()
        throughput_legend.setVisible(True)
        throughput_legend.setLabelBrush(QBrush(Qt.white))
        throughput_legend.setBackgroundVisible(False)
        self.op_throughput_created_set = QBarSet("Orders created")
        self.op_throughput_created_set.setColor(QColor(0x4C, 0x6E, 0xF5))
        self.op_throughput_completed_set = QBarSet("Orders completed")
        self.op_throughput_completed_set.setColor(QColor(0x7E, 0xE7, 0x87))
        self.op_throughput_bar_series = QBarSeries()
        self.op_throughput_bar_series.append(self.op_throughput_created_set)
        self.op_throughput_bar_series.append(self.op_throughput_completed_set)
        self.op_throughput_chart.addSeries(self.op_throughput_bar_series)
        self.op_throughput_avg_series = QLineSeries()
        self.op_throughput_avg_series.setName("Avg completion (h)")
        avg_pen = QPen(QColor("#FFB347"))
        avg_pen.setWidth(2)
        self.op_throughput_avg_series.setPen(avg_pen)
        self.op_throughput_avg_series.setPointsVisible(True)
        self.op_throughput_chart.addSeries(self.op_throughput_avg_series)
        self.op_throughput_category_axis = QBarCategoryAxis()
        self.op_throughput_category_axis.setLabelsColor(Qt.white)
        self.op_throughput_chart.addAxis(self.op_throughput_category_axis, Qt.AlignBottom)
        self.op_throughput_count_axis = QValueAxis()
        self.op_throughput_count_axis.setLabelFormat("%d")
        self.op_throughput_count_axis.setLabelsColor(Qt.white)
        self.op_throughput_count_axis.setTitleText("Orders")
        self.op_throughput_count_axis.setTitleBrush(Qt.white)
        self.op_throughput_chart.addAxis(self.op_throughput_count_axis, Qt.AlignLeft)
        self.op_throughput_bar_series.attachAxis(self.op_throughput_category_axis)
        self.op_throughput_bar_series.attachAxis(self.op_throughput_count_axis)
        self.op_throughput_hours_axis = QValueAxis()
        self.op_throughput_hours_axis.setLabelFormat("%.1f")
        self.op_throughput_hours_axis.setLabelsColor(Qt.white)
        self.op_throughput_hours_axis.setTitleText("Hours")
        self.op_throughput_hours_axis.setTitleBrush(Qt.white)
        self.op_throughput_chart.addAxis(self.op_throughput_hours_axis, Qt.AlignRight)
        self.op_throughput_avg_series.attachAxis(self.op_throughput_category_axis)
        self.op_throughput_avg_series.attachAxis(self.op_throughput_hours_axis)
        self.op_throughput_chart_view = QChartView(self.op_throughput_chart)
        self.op_throughput_chart_view.setRenderHint(QPainter.Antialiasing, True)
        self.op_throughput_chart_view.setMinimumHeight(340)
        self.op_throughput_chart_view.setStyleSheet("background: rgba(32, 40, 62, 0.6);")

        self.op_cycle_chart = QChart()
        self.op_cycle_chart.setBackgroundBrush(Qt.transparent)
        cycle_legend = self.op_cycle_chart.legend()
        cycle_legend.setVisible(True)
        cycle_legend.setLabelBrush(QBrush(Qt.white))
        cycle_legend.setBackgroundVisible(False)
        self.op_cycle_bar_set = QBarSet("Samples completed")
        self.op_cycle_bar_set.setColor(QColor(0x3E, 0x9E, 0xBA))
        self.op_cycle_bar_series = QBarSeries()
        self.op_cycle_bar_series.append(self.op_cycle_bar_set)
        self.op_cycle_chart.addSeries(self.op_cycle_bar_series)
        self.op_cycle_avg_series = QLineSeries()
        self.op_cycle_avg_series.setName("Avg cycle (h)")
        cycle_pen = QPen(QColor("#E27D60"))
        cycle_pen.setWidth(2)
        self.op_cycle_avg_series.setPen(cycle_pen)
        self.op_cycle_avg_series.setPointsVisible(True)
        self.op_cycle_chart.addSeries(self.op_cycle_avg_series)
        self.op_cycle_category_axis = QBarCategoryAxis()
        self.op_cycle_category_axis.setLabelsColor(Qt.white)
        self.op_cycle_chart.addAxis(self.op_cycle_category_axis, Qt.AlignBottom)
        self.op_cycle_count_axis = QValueAxis()
        self.op_cycle_count_axis.setLabelFormat("%d")
        self.op_cycle_count_axis.setLabelsColor(Qt.white)
        self.op_cycle_count_axis.setTitleText("Samples")
        self.op_cycle_count_axis.setTitleBrush(Qt.white)
        self.op_cycle_chart.addAxis(self.op_cycle_count_axis, Qt.AlignLeft)
        self.op_cycle_bar_series.attachAxis(self.op_cycle_category_axis)
        self.op_cycle_bar_series.attachAxis(self.op_cycle_count_axis)
        self.op_cycle_hours_axis = QValueAxis()
        self.op_cycle_hours_axis.setLabelFormat("%.1f")
        self.op_cycle_hours_axis.setLabelsColor(Qt.white)
        self.op_cycle_hours_axis.setTitleText("Hours")
        self.op_cycle_hours_axis.setTitleBrush(Qt.white)
        self.op_cycle_chart.addAxis(self.op_cycle_hours_axis, Qt.AlignRight)
        self.op_cycle_avg_series.attachAxis(self.op_cycle_category_axis)
        self.op_cycle_avg_series.attachAxis(self.op_cycle_hours_axis)
        self.op_cycle_chart_view = QChartView(self.op_cycle_chart)
        self.op_cycle_chart_view.setRenderHint(QPainter.Antialiasing, True)
        self.op_cycle_chart_view.setMinimumHeight(340)
        self.op_cycle_chart_view.setStyleSheet("background: rgba(32, 40, 62, 0.6);")

        charts_row = QHBoxLayout()
        charts_row.setSpacing(16)
        throughput_panel = self._create_chart_panel("Order throughput & completion", self.op_throughput_chart_view)
        cycle_panel = self._create_chart_panel("Sample cycle time", self.op_cycle_chart_view)
        charts_row.addWidget(throughput_panel, 1)
        charts_row.addWidget(cycle_panel, 1)
        content_layout.addLayout(charts_row)

        self.op_funnel_chart = QChart()
        self.op_funnel_chart.setBackgroundBrush(Qt.transparent)
        self.op_funnel_chart.legend().setVisible(False)
        self.op_funnel_series = QHorizontalBarSeries()
        self.op_funnel_set = QBarSet("Orders")
        self.op_funnel_series.append(self.op_funnel_set)
        self.op_funnel_chart.addSeries(self.op_funnel_series)
        self.op_funnel_value_axis = QValueAxis()
        self.op_funnel_value_axis.setLabelFormat("%d")
        self.op_funnel_value_axis.setLabelsColor(Qt.white)
        self.op_funnel_value_axis.setTitleText("Orders")
        self.op_funnel_value_axis.setTitleBrush(Qt.white)
        self.op_funnel_chart.addAxis(self.op_funnel_value_axis, Qt.AlignBottom)
        self.op_funnel_series.attachAxis(self.op_funnel_value_axis)
        self.op_funnel_categories_axis = QBarCategoryAxis()
        self.op_funnel_categories_axis.setLabelsColor(Qt.white)
        self.op_funnel_chart.addAxis(self.op_funnel_categories_axis, Qt.AlignLeft)
        self.op_funnel_series.attachAxis(self.op_funnel_categories_axis)
        self.op_funnel_chart_view = QChartView(self.op_funnel_chart)
        self.op_funnel_chart_view.setRenderHint(QPainter.Antialiasing, True)
        self.op_funnel_chart_view.setMinimumHeight(320)
        self.op_funnel_chart_view.setStyleSheet("background: rgba(32, 40, 62, 0.6);")
        funnel_panel = self._create_chart_panel("Order funnel", self.op_funnel_chart_view)

        self.op_matrix_table = self._create_table_widget(["Matrix", "Samples", "Avg time"])
        self.op_matrix_table.setMinimumHeight(200)
        matrix_panel = self._create_list_panel("Cycle time by matrix", self.op_matrix_table)

        self.op_slowest_orders_table = self._create_table_widget(
            ["Order ID", "Customer", "Completion", "Age", "Status"]
        )
        self.op_slowest_orders_table.setMinimumHeight(240)
        slow_orders_panel = self._create_list_panel("Slowest orders", self.op_slowest_orders_table)

        bottom_layout = QHBoxLayout()
        bottom_layout.setSpacing(16)
        bottom_layout.addWidget(funnel_panel, 1)
        tables_layout = QVBoxLayout()
        tables_layout.setSpacing(16)
        tables_layout.addWidget(matrix_panel)
        tables_layout.addWidget(slow_orders_panel)
        bottom_layout.addLayout(tables_layout, 1)
        content_layout.addLayout(bottom_layout)

        content_layout.addStretch()

        scroll_area = QScrollArea()
        scroll_area.setWidgetResizable(True)
        scroll_area.setFrameShape(QFrame.NoFrame)
        scroll_area.setStyleSheet("QScrollArea { background-color: #0F172A; }")
        scroll_area.setWidget(content_widget)

        tab_layout = QVBoxLayout(tab)
        tab_layout.setContentsMargins(0, 0, 0, 0)
        tab_layout.setSpacing(0)
        tab_layout.addWidget(scroll_area)

        return tab

    def _build_priority_orders_tab(self) -> QWidget:
        tab = QWidget()
        tab.setStyleSheet("background-color: #0F172A;")

        layout = QVBoxLayout(tab)
        layout.setContentsMargins(24, 24, 24, 24)
        layout.setSpacing(18)

        header_label = QLabel("Priority Orders")
        header_label.setAlignment(Qt.AlignLeft | Qt.AlignVCenter)
        header_label.setStyleSheet("color: #E0E8FF; font-size: 24px; font-weight: 700;")
        layout.addWidget(header_label)

        metrics_layout = QHBoxLayout()
        metrics_layout.setSpacing(16)
        self.priority_total_card, self.priority_total_value = self._create_metric_card("Overdue orders", "#FF8FAB")
        self.priority_breach_card, self.priority_breach_value = self._create_metric_card("Beyond SLA", "#F97316")
        metrics_layout.addWidget(self.priority_total_card, 1)
        metrics_layout.addWidget(self.priority_breach_card, 1)
        metrics_layout.addStretch()
        layout.addLayout(metrics_layout)

        controls_layout = QHBoxLayout()
        controls_layout.setSpacing(12)
        controls_layout.addStretch()

        min_days_label = QLabel("Minimum days overdue")
        min_days_label.setStyleSheet("color: #B0BCD5; font-size: 13px;")
        self.priority_min_days_spin = QSpinBox()
        self.priority_min_days_spin.setRange(0, 90)
        self.priority_min_days_spin.setValue(self._priority_min_days_default)
        self.priority_min_days_spin.setSingleStep(1)
        self.priority_min_days_spin.setStyleSheet(
            "QSpinBox { padding: 6px 10px; font-size: 14px; background-color: #1E2A44; color: #E0E8FF; "
            "border: 1px solid #1F3B73; border-radius: 6px; }"
        )

        sla_label = QLabel("SLA (hours)")
        sla_label.setStyleSheet("color: #B0BCD5; font-size: 13px;")
        self.priority_sla_hours_spin = QSpinBox()
        self.priority_sla_hours_spin.setRange(0, 720)
        self.priority_sla_hours_spin.setValue(self._priority_sla_hours_default)
        self.priority_sla_hours_spin.setSingleStep(24)
        self.priority_sla_hours_spin.setStyleSheet(
            "QSpinBox { padding: 6px 10px; font-size: 14px; background-color: #1E2A44; color: #E0E8FF; "
            "border: 1px solid #1F3B73; border-radius: 6px; }"
        )

        self.priority_refresh_button = QPushButton("Refresh")
        self.priority_refresh_button.setFixedWidth(140)
        self.priority_refresh_button.setStyleSheet(
            "padding: 10px; font-size: 15px; background-color: #1F3B73; color: white; border-radius: 6px;"
        )
        self.priority_refresh_button.clicked.connect(self.refresh_priority_orders)

        controls_layout.addWidget(min_days_label)
        controls_layout.addWidget(self.priority_min_days_spin)
        controls_layout.addWidget(sla_label)
        controls_layout.addWidget(self.priority_sla_hours_spin)
        controls_layout.addWidget(self.priority_refresh_button)
        layout.addLayout(controls_layout)

        self.priority_status_label = QLabel("Select Refresh to load overdue orders.")
        self.priority_status_label.setAlignment(Qt.AlignLeft | Qt.AlignVCenter)
        self.priority_status_label.setStyleSheet("color: #B0BCD5; font-size: 13px;")
        layout.addWidget(self.priority_status_label)

        self.priority_orders_table = self._create_table_widget(
            ["Order", "Customer", "State", "Created", "Open time", "SLA breach"]
        )
        self.priority_orders_table.setMinimumHeight(260)
        table_panel = self._create_list_panel("Most overdue orders", self.priority_orders_table)
        table_panel.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)

        chart_panel = QFrame()
        chart_panel.setFrameShape(QFrame.StyledPanel)
        chart_panel.setStyleSheet(
            "QFrame { background-color: #111C34; border: 1px solid #1F3B73; border-radius: 10px; }"
        )
        chart_panel.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        chart_layout = QVBoxLayout(chart_panel)
        chart_layout.setContentsMargins(16, 16, 16, 16)
        chart_layout.setSpacing(12)

        chart_title = QLabel("Overdue orders timeline")
        chart_title.setStyleSheet("color: #E0E8FF; font-weight: 600; font-size: 14px;")
        chart_layout.addWidget(chart_title)

        self.priority_chart = QChart()
        self.priority_chart.setBackgroundBrush(Qt.transparent)
        self.priority_chart.legend().setVisible(False)
        self.priority_timeline_series = QLineSeries()
        self.priority_timeline_series.setColor(QColor(0xF9, 0x73, 0x16))
        pen = QPen(QColor("#F97316"))
        pen.setWidth(2)
        self.priority_timeline_series.setPen(pen)
        self.priority_timeline_series.setPointsVisible(True)
        self.priority_chart.addSeries(self.priority_timeline_series)

        self.priority_datetime_axis = QDateTimeAxis()
        self.priority_datetime_axis.setLabelsColor(Qt.white)
        self.priority_datetime_axis.setFormat("MMM d")
        self.priority_datetime_axis.setTitleText("Period")
        self.priority_datetime_axis.setTitleBrush(Qt.white)

        self.priority_value_axis = QValueAxis()
        self.priority_value_axis.setLabelsColor(Qt.white)
        self.priority_value_axis.setLabelFormat("%d")
        self.priority_value_axis.setTitleText("Overdue orders")
        self.priority_value_axis.setTitleBrush(Qt.white)

        self.priority_chart.addAxis(self.priority_datetime_axis, Qt.AlignBottom)
        self.priority_chart.addAxis(self.priority_value_axis, Qt.AlignLeft)
        self.priority_timeline_series.attachAxis(self.priority_datetime_axis)
        self.priority_timeline_series.attachAxis(self.priority_value_axis)

        self.priority_chart_view = QChartView(self.priority_chart)
        self.priority_chart_view.setRenderHint(QPainter.Antialiasing, True)
        self.priority_chart_view.setMinimumHeight(280)
        self.priority_chart_view.setStyleSheet("background: rgba(32, 40, 62, 0.6);")
        chart_layout.addWidget(self.priority_chart_view)

        row_layout = QHBoxLayout()
        row_layout.setSpacing(16)
        row_layout.addWidget(table_panel, 1)
        row_layout.addWidget(chart_panel, 1)
        layout.addLayout(row_layout, 1)

        heatmap_panel = QFrame()
        heatmap_panel.setFrameShape(QFrame.StyledPanel)
        heatmap_panel.setStyleSheet(
            "QFrame { background-color: #111C34; border: 1px solid #1F3B73; border-radius: 10px; }"
        )
        heatmap_layout = QVBoxLayout(heatmap_panel)
        heatmap_layout.setContentsMargins(16, 16, 16, 16)
        heatmap_layout.setSpacing(12)

        heatmap_title = QLabel("Overdue heatmap (customers × period)")
        heatmap_title.setStyleSheet("color: #E0E8FF; font-weight: 600; font-size: 14px;")
        heatmap_layout.addWidget(heatmap_title)

        self.priority_heatmap_table = QTableWidget()
        self.priority_heatmap_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.priority_heatmap_table.setSelectionMode(QAbstractItemView.NoSelection)
        self.priority_heatmap_table.setFocusPolicy(Qt.NoFocus)
        self.priority_heatmap_table.setAlternatingRowColors(False)
        self.priority_heatmap_table.setColumnCount(0)
        self.priority_heatmap_table.setRowCount(0)
        self.priority_heatmap_table.horizontalHeader().setStretchLastSection(False)
        self.priority_heatmap_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.priority_heatmap_table.horizontalHeader().setDefaultAlignment(Qt.AlignCenter)
        vertical_header = self.priority_heatmap_table.verticalHeader()
        vertical_header.setVisible(True)
        vertical_header.setDefaultAlignment(Qt.AlignLeft | Qt.AlignVCenter)
        vertical_header.setSectionResizeMode(QHeaderView.ResizeToContents)
        self.priority_heatmap_table.setStyleSheet(
            "QTableWidget { background-color: #0F172A; color: #E0E8FF; gridline-color: #1F3B73; }"
        )
        self.priority_heatmap_table.setMinimumHeight(220)
        heatmap_layout.addWidget(self.priority_heatmap_table)

        layout.addWidget(heatmap_panel, 1)

        layout.addStretch()
        return tab
    def _initialize_operational_range(self) -> None:
        today = QDate.currentDate()
        self.op_end_date_edit.setDate(today)
        self.op_start_date_edit.setDate(today.addDays(-27))

    def _get_operational_range(self) -> Tuple[datetime, datetime]:
        start_qdate = self.op_start_date_edit.date()
        end_qdate = self.op_end_date_edit.date()
        start_dt = datetime(
            start_qdate.year(),
            start_qdate.month(),
            start_qdate.day(),
            tzinfo=timezone.utc,
        )
        end_dt = datetime(
            end_qdate.year(),
            end_qdate.month(),
            end_qdate.day(),
            23,
            59,
            59,
            999999,
            tzinfo=timezone.utc,
        )
        if end_dt < start_dt:
            raise ValueError("Start date cannot be after the end date.")
        return start_dt, end_dt

    def _advance_operational_spinner(self) -> None:
        if not self.op_spinner_label.isVisible():
            return
        self._operational_spinner_index = (self._operational_spinner_index + 1) % len(self._operational_spinner_frames)
        self.op_spinner_label.setText(self._operational_spinner_frames[self._operational_spinner_index])

    def _set_operational_loading(self, loading: bool) -> None:
        self._operational_loading = loading
        self.op_refresh_button.setEnabled(not loading)
        self.op_start_date_edit.setEnabled(not loading)
        self.op_end_date_edit.setEnabled(not loading)
        self._operational_timeframe_combo.setEnabled(not loading)
        if loading:
            self._operational_spinner_index = 0
            self.op_spinner_label.setText(self._operational_spinner_frames[self._operational_spinner_index])
            self.op_spinner_label.setVisible(True)
            if not self._operational_spinner_timer.isActive():
                self._operational_spinner_timer.start()
        else:
            if self._operational_spinner_timer.isActive():
                self._operational_spinner_timer.stop()
            self.op_spinner_label.setVisible(False)
            self.op_spinner_label.setText("")

    def _update_operational_status(self, message: str) -> None:
        self.op_status_label.setText(message)

    def _show_operational_error(self, message: str) -> None:
        self._update_operational_status("Update failed")
        box = QMessageBox(self)
        box.setIcon(QMessageBox.Critical)
        box.setWindowTitle("Operational efficiency")
        box.setText(message)
        box.exec()

    def _begin_operational_fetch(self, start_dt: datetime, end_dt: datetime) -> None:
        if self._operational_loading:
            return
        self._set_operational_loading(True)
        status_parts = ["Updating..."]
        range_text = self._format_range(start_dt, end_dt)
        if range_text:
            status_parts.append(f"Range: {range_text}")
        timeframe_label = self._get_operational_timeframe_label()
        if timeframe_label:
            status_parts.append(f"Interval: {timeframe_label}")
        self._update_operational_status(" | ".join(status_parts))

        self._operational_thread = QThread(self)
        self._operational_worker = OperationalWorker(
            self._client,
            start_date=start_dt,
            end_date=end_dt,
            timeframe=self._operational_timeframe_mode,
        )
        self._operational_worker.moveToThread(self._operational_thread)
        self._operational_thread.started.connect(self._operational_worker.process)
        self._operational_worker.finished.connect(self._operational_thread.quit)
        self._operational_worker.error.connect(self._operational_thread.quit)
        self._operational_thread.finished.connect(self._operational_thread.deleteLater)
        self._operational_worker.finished.connect(self._operational_worker.deleteLater)
        self._operational_worker.error.connect(self._operational_worker.deleteLater)
        self._operational_worker.finished.connect(self._on_operational_finished)
        self._operational_worker.error.connect(self._on_operational_error)
        self._operational_thread.finished.connect(self._on_operational_thread_finished)
        self._operational_thread.start()

    def refresh_operational_data(self) -> None:
        if self._operational_loading:
            return
        try:
            start_dt, end_dt = self._get_operational_range()
        except ValueError as exc:
            self._show_operational_error(str(exc))
            return
        self._begin_operational_fetch(start_dt, end_dt)

    def refresh_priority_orders(self) -> None:
        if self._priority_loading:
            return
        now = datetime.now(timezone.utc)
        start_dt = now - timedelta(days=30)
        min_days = int(self.priority_min_days_spin.value())
        sla_hours = int(self.priority_sla_hours_spin.value())
        self._begin_priority_fetch(start_dt, now, min_days, sla_hours)

    def _begin_priority_fetch(
        self,
        start_dt: datetime,
        end_dt: datetime,
        min_days_overdue: int,
        sla_hours: int,
    ) -> None:
        if self._priority_loading:
            return
        self._priority_last_payload = None
        self._set_priority_loading(True)
        range_text = f"{start_dt.strftime('%Y-%m-%d')} - {end_dt.strftime('%Y-%m-%d')}"
        self._update_priority_status(f"Updating... | Range: {range_text}")

        self._priority_thread = QThread(self)
        self._priority_worker = PriorityOrdersWorker(
            self._client,
            date_from=start_dt,
            date_to=end_dt,
            min_days_overdue=min_days_overdue,
            sla_hours=sla_hours,
            top_limit=self._priority_top_limit,
        )
        self._priority_worker.moveToThread(self._priority_thread)
        self._priority_thread.started.connect(self._priority_worker.process)
        self._priority_worker.finished.connect(self._priority_thread.quit)
        self._priority_worker.error.connect(self._priority_thread.quit)
        self._priority_thread.finished.connect(self._priority_thread.deleteLater)
        self._priority_worker.finished.connect(self._priority_worker.deleteLater)
        self._priority_worker.error.connect(self._priority_worker.deleteLater)
        self._priority_worker.finished.connect(self._on_priority_finished)
        self._priority_worker.error.connect(self._on_priority_error)
        self._priority_thread.finished.connect(self._on_priority_thread_finished)
        self._priority_thread.start()

    def _set_priority_loading(self, loading: bool) -> None:
        self._priority_loading = loading
        self.priority_refresh_button.setEnabled(not loading)
        self.priority_min_days_spin.setEnabled(not loading)
        self.priority_sla_hours_spin.setEnabled(not loading)
        if loading:
            self._update_priority_status("Loading overdue orders…")

    def _update_priority_status(self, message: str) -> None:
        self.priority_status_label.setText(message)

    def _on_priority_finished(self, payload: Dict[str, Any]) -> None:
        self._priority_last_payload = payload
        self._apply_priority_payload(payload)

    def _on_priority_error(self, message: str) -> None:
        self._update_priority_status("Update failed")
        box = QMessageBox(self)
        box.setIcon(QMessageBox.Critical)
        box.setWindowTitle("Priority orders")
        box.setText(message)
        box.exec()

    def _on_priority_thread_finished(self) -> None:
        self._set_priority_loading(False)
        self._priority_worker = None
        self._priority_thread = None

    def _apply_priority_payload(self, payload: Dict[str, Any]) -> None:
        self._update_priority_kpis(payload.get("kpis", {}))
        self._update_priority_table(payload.get("top_orders", []))
        self._update_priority_chart(payload.get("timeline", []))
        self._update_priority_heatmap(payload.get("heatmap", []))
        params = payload.get("params", {})
        start_text = params.get("date_from")
        end_text = params.get("date_to")
        try:
            if isinstance(start_text, str) and isinstance(end_text, str):
                start_dt = datetime.fromisoformat(start_text)
                end_dt = datetime.fromisoformat(end_text)
                if start_dt.tzinfo is None:
                    start_dt = start_dt.replace(tzinfo=timezone.utc)
                if end_dt.tzinfo is None:
                    end_dt = end_dt.replace(tzinfo=timezone.utc)
                timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
                range_text = f"{start_dt.strftime('%Y-%m-%d')} - {end_dt.strftime('%Y-%m-%d')}"
                self._update_priority_status(f"Last update: {timestamp} | Range: {range_text}")
                return
            raise ValueError
        except Exception:
            self._update_priority_status("Priority orders refreshed.")

    def _update_priority_kpis(self, kpis: Dict[str, Any]) -> None:
        self.priority_total_value.setText(self._format_number(kpis.get("total_overdue")))
        self.priority_breach_value.setText(self._format_number(kpis.get("overdue_beyond_sla")))

    def _update_priority_table(self, records: Sequence[Dict[str, Any]]) -> None:
        table = self.priority_orders_table
        table.setRowCount(0)
        sla_hours = float(self.priority_sla_hours_spin.value())
        if not isinstance(records, Sequence):
            records = []
        for entry in records:
            if not isinstance(entry, dict):
                continue
            order_display = entry.get("custom_formatted_id") or entry.get("order_id") or ""
            customer = entry.get("customer_name") or ""
            state_text = str(entry.get("state") or "").replace("_", " ").title()
            created = entry.get("date_created")
            open_hours = float(entry.get("open_hours") or 0.0)
            breach = open_hours > sla_hours if sla_hours > 0 else open_hours > 0

            row = table.rowCount()
            table.insertRow(row)
            table.setItem(row, 0, QTableWidgetItem(str(order_display)))
            table.setItem(row, 1, QTableWidgetItem(str(customer)))
            table.setItem(row, 2, QTableWidgetItem(state_text))
            table.setItem(row, 3, QTableWidgetItem(self._format_priority_timestamp(created)))
            table.setItem(row, 4, QTableWidgetItem(self._format_duration_hours(open_hours)))
            breach_item = QTableWidgetItem("Yes" if breach else "No")
            if breach:
                breach_item.setForeground(QBrush(QColor("#F97316")))
            table.setItem(row, 5, breach_item)
        table.resizeColumnsToContents()

        if table.rowCount() == 0:
            self._update_priority_status("No overdue orders found in the last 30 days.")

    def _update_priority_chart(self, points: Sequence[Dict[str, Any]]) -> None:
        self.priority_timeline_series.clear()
        if not isinstance(points, Sequence) or not points:
            now = datetime.now(timezone.utc)
            start = now - timedelta(days=30)
            self.priority_datetime_axis.setRange(QDateTime(start), QDateTime(now))
            self.priority_value_axis.setRange(0, 1)
            return

        min_dt: Optional[datetime] = None
        max_dt: Optional[datetime] = None
        max_value = 1
        for entry in points:
            if not isinstance(entry, dict):
                continue
            dt_value = entry.get("period_start")
            if not isinstance(dt_value, datetime):
                continue
            overdue = int(entry.get("overdue_orders") or 0)
            qdt = QDateTime(dt_value)
            self.priority_timeline_series.append(qdt.toMSecsSinceEpoch(), overdue)
            min_dt = dt_value if min_dt is None or dt_value < min_dt else min_dt
            max_dt = dt_value if max_dt is None or dt_value > max_dt else max_dt
            if overdue > max_value:
                max_value = overdue

        if min_dt and max_dt:
            self.priority_datetime_axis.setRange(QDateTime(min_dt), QDateTime(max_dt))
        else:
            now = datetime.now(timezone.utc)
            start = now - timedelta(days=30)
            self.priority_datetime_axis.setRange(QDateTime(start), QDateTime(now))

        self.priority_value_axis.setRange(0, max(1, max_value))

    def _update_priority_heatmap(self, entries: Sequence[Dict[str, Any]]) -> None:
        table = self.priority_heatmap_table
        if table is None:
            return
        table.setUpdatesEnabled(False)
        table.clearContents()
        table.setRowCount(0)
        table.setColumnCount(0)
        self._priority_heatmap_periods = []
        self._priority_heatmap_customers = []

        if not isinstance(entries, Sequence) or not entries:
            table.setUpdatesEnabled(True)
            return

        periods_set: Dict[datetime, None] = {}
        customers_map: Dict[str, str] = {}
        counts: Dict[Tuple[str, datetime], int] = {}
        max_value = 0

        for entry in entries:
            if not isinstance(entry, dict):
                continue
            customer_name = entry.get("customer_name") or ""
            customer_id = entry.get("customer_id")
            if not customer_name and customer_id is not None:
                customer_name = f"Customer {customer_id}"
            dt_value = entry.get("period_start")
            if not isinstance(dt_value, datetime):
                continue
            overdue = int(entry.get("overdue_orders") or 0)
            key = (customer_name or "Unknown", dt_value)
            counts[key] = overdue
            if overdue > max_value:
                max_value = overdue
            periods_set[dt_value] = None
            customers_map[customer_name or "Unknown"] = customer_name or "Unknown"

        periods = sorted(periods_set.keys())
        customers = sorted(customers_map.values())
        self._priority_heatmap_periods = periods
        self._priority_heatmap_customers = customers

        table.setColumnCount(len(periods))
        column_headers = [dt.strftime("%Y-%m-%d") for dt in periods]
        table.setHorizontalHeaderLabels(column_headers)
        table.setRowCount(len(customers))
        for row, customer in enumerate(customers):
            label = customer or "Unknown"
            table.setVerticalHeaderItem(row, QTableWidgetItem(label))

        if max_value <= 0:
            max_value = 1

        for row, customer in enumerate(customers):
            for col, period in enumerate(periods):
                value = counts.get((customer, period), 0)
                if value <= 0:
                    item = QTableWidgetItem("")
                    color = QColor("#182238")
                else:
                    item = QTableWidgetItem(str(value))
                    color = self._priority_heat_color(value / float(max_value))
                item.setTextAlignment(Qt.AlignCenter)
                item.setBackground(QBrush(color))
                if value > 0 and (color.red() + color.green() + color.blue()) / 3 > 192:
                    item.setForeground(QBrush(QColor("#0F172A")))
                else:
                    item.setForeground(QBrush(QColor("#E0E8FF")))
                table.setItem(row, col, item)

        table.resizeColumnsToContents()
        table.setUpdatesEnabled(True)

    @staticmethod
    def _priority_heat_color(ratio: float) -> QColor:
        clamped = max(0.0, min(1.0, ratio))
        start_color = QColor("#1E2A44")
        end_color = QColor("#FF8FAB")
        r = int(start_color.red() + (end_color.red() - start_color.red()) * clamped)
        g = int(start_color.green() + (end_color.green() - start_color.green()) * clamped)
        b = int(start_color.blue() + (end_color.blue() - start_color.blue()) * clamped)
        return QColor(r, g, b)

    @staticmethod
    def _format_priority_timestamp(value: Optional[datetime]) -> str:
        if not isinstance(value, datetime):
            return ""
        normalized = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        normalized = normalized.astimezone(timezone.utc)
        return normalized.strftime("%Y-%m-%d %H:%M")

    @staticmethod
    def _format_duration_hours(value: Any) -> str:
        try:
            hours_total = float(value)
        except (TypeError, ValueError):
            return ""
        if hours_total <= 0:
            return "<1h"
        minutes_total = int(round(hours_total * 60))
        days = minutes_total // (24 * 60)
        hours = (minutes_total % (24 * 60)) // 60
        if days and hours:
            return f"{days}d {hours}h"
        if days:
            return f"{days}d"
        if hours:
            return f"{hours}h"
        minutes = minutes_total % 60
        return f"{minutes}m"

    def _on_operational_timeframe_changed(self, index: int) -> None:
        mode = self._operational_timeframe_combo.itemData(index)
        if mode not in {"day", "week", "month"}:
            return
        if mode == self._operational_timeframe_mode and self._operational_initialized:
            return
        self._operational_timeframe_mode = mode
        if self.tabs.currentIndex() == self._operational_tab_index:
            self.refresh_operational_data()

    def _on_tab_changed(self, index: int) -> None:
        if index == self._operational_tab_index and not self._operational_initialized:
            self._operational_initialized = True
            self.refresh_operational_data()
        elif index == getattr(self, "_priority_tab_index", -1) and not self._priority_initialized:
            self._priority_initialized = True
            self.refresh_priority_orders()

    def _on_operational_finished(self, summary: Dict[str, Any]) -> None:
        self._apply_operational_summary(summary)

    def _on_operational_error(self, message: str) -> None:
        self._show_operational_error(message)

    def _on_operational_thread_finished(self) -> None:
        self._set_operational_loading(False)
        self._operational_worker = None
        self._operational_thread = None

    def _apply_operational_summary(self, summary: Dict[str, Any]) -> None:
        metrics = summary.get("metrics", {})
        self.op_lead_avg_value.setText(self._format_hours(metrics.get("lead_time_average_hours")))
        self.op_lead_median_value.setText(self._format_hours(metrics.get("lead_time_median_hours")))
        self.op_orders_completed_value.setText(self._format_number(metrics.get("orders_completed")))
        samples_completed = metrics.get("samples_completed")
        self.op_samples_completed_value.setText(self._format_number(samples_completed))

        start_dt = summary.get("start_date")
        end_dt = summary.get("end_date")
        if isinstance(start_dt, datetime) and isinstance(end_dt, datetime):
            range_text = self._format_range(start_dt, end_dt)
        else:
            range_text = ""
        self._operational_current_timeframe_mode = summary.get("timeframe", self._operational_timeframe_mode)
        status_parts = [f"Last update: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}"]
        if range_text:
            status_parts.append(f"Range: {range_text}")
        label = self._get_operational_timeframe_label(self._operational_current_timeframe_mode)
        if label:
            status_parts.append(f"Interval: {label}")
        self._update_operational_status(" | ".join(status_parts))

        throughput_points = summary.get("throughput_points", [])
        self._update_throughput_chart(throughput_points)
        cycle_points = summary.get("cycle_points", [])
        self._update_cycle_chart(cycle_points)
        matrix_breakdown = summary.get("cycle_by_matrix", [])
        self._update_matrix_table(matrix_breakdown)
        funnel_stages = summary.get("funnel_stages", [])
        funnel_total = summary.get("funnel_total", 0)
        self._update_funnel_chart(funnel_stages, funnel_total)
        slow_orders = summary.get("slowest_orders", [])
        self._update_slowest_orders_table(slow_orders)

    @staticmethod
    def _format_hours(value: Optional[float]) -> str:
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return "--"
        if numeric <= 0.0:
            return "--"
        minutes_total = int(round(numeric * 60))
        if minutes_total <= 0:
            return "<1h"
        days = minutes_total // (24 * 60)
        hours = (minutes_total % (24 * 60)) // 60
        if days and hours:
            return f"{days}d {hours}h"
        if days:
            return f"{days}d"
        if hours:
            return f"{hours}h"
        minutes = minutes_total % 60
        return f"{minutes}m"

    @staticmethod
    def _format_number(value: Any) -> str:
        try:
            numeric = int(value)
        except (TypeError, ValueError):
            return "--"
        return f"{numeric:,}"

    def _get_operational_timeframe_label(self, mode: Optional[str] = None) -> str:
        selected = mode or self._operational_timeframe_mode
        return {
            "day": "Daily",
            "week": "Weekly",
            "month": "Monthly",
        }.get(selected, "")

    def _format_operational_category(self, dt_value: Optional[datetime]) -> str:
        if not isinstance(dt_value, datetime):
            return ""
        if self._operational_current_timeframe_mode == "month":
            return dt_value.strftime("%b %Y")
        if self._operational_current_timeframe_mode == "week":
            return f"Wk of {dt_value.strftime('%b %d')}"
        return dt_value.strftime("%b %d")

    def _update_throughput_chart(self, points: Sequence[Dict[str, Any]]) -> None:
        self.op_throughput_created_set.remove(0, self.op_throughput_created_set.count())
        self.op_throughput_completed_set.remove(0, self.op_throughput_completed_set.count())
        self.op_throughput_avg_series.clear()

        categories: List[str] = []
        max_orders = 1
        max_hours = 1.0
        for index, entry in enumerate(points):
            period = entry.get("period_start")
            label = self._format_operational_category(period)
            categories.append(label or f"{index + 1}")
            created = int(entry.get("orders_created") or 0)
            completed = int(entry.get("orders_completed") or 0)
            avg_hours = float(entry.get("average_completion_hours") or 0.0)
            self.op_throughput_created_set.append(created)
            self.op_throughput_completed_set.append(completed)
            self.op_throughput_avg_series.append(index + 0.5, avg_hours)
            max_orders = max(max_orders, created, completed)
            max_hours = max(max_hours, avg_hours)

        if not categories:
            categories = ["--"]
            self.op_throughput_created_set.append(0)
            self.op_throughput_completed_set.append(0)
            self.op_throughput_avg_series.append(0.5, 0.0)

        self.op_throughput_category_axis.clear()
        self.op_throughput_category_axis.append(categories)
        self.op_throughput_count_axis.setRange(0, max_orders * 1.2)
        self.op_throughput_hours_axis.setRange(0.0, max_hours * 1.2 if max_hours > 0 else 1.0)

    def _update_cycle_chart(self, points: Sequence[Dict[str, Any]]) -> None:
        self.op_cycle_bar_set.remove(0, self.op_cycle_bar_set.count())
        self.op_cycle_avg_series.clear()

        categories: List[str] = []
        max_samples = 1
        max_hours = 1.0
        for index, entry in enumerate(points):
            period = entry.get("period_start")
            label = self._format_operational_category(period)
            categories.append(label or f"{index + 1}")
            samples_completed = int(entry.get("completed_samples") or 0)
            avg_hours = float(entry.get("average_cycle_hours") or 0.0)
            self.op_cycle_bar_set.append(samples_completed)
            self.op_cycle_avg_series.append(index + 0.5, avg_hours)
            max_samples = max(max_samples, samples_completed)
            max_hours = max(max_hours, avg_hours)

        if not categories:
            categories = ["--"]
            self.op_cycle_bar_set.append(0)
            self.op_cycle_avg_series.append(0.5, 0.0)

        self.op_cycle_category_axis.clear()
        self.op_cycle_category_axis.append(categories)
        self.op_cycle_count_axis.setRange(0, max_samples * 1.2)
        self.op_cycle_hours_axis.setRange(0.0, max_hours * 1.2 if max_hours > 0 else 1.0)

    def _update_matrix_table(self, records: Sequence[Dict[str, Any]]) -> None:
        table = self.op_matrix_table
        table.setRowCount(0)
        for entry in records:
            if not isinstance(entry, dict):
                continue
            row = table.rowCount()
            table.insertRow(row)
            table.setItem(row, 0, QTableWidgetItem(str(entry.get("matrix_type") or "Unknown")))
            table.setItem(row, 1, QTableWidgetItem(self._format_number(entry.get("completed_samples"))))
            table.setItem(row, 2, QTableWidgetItem(self._format_hours(entry.get("average_cycle_hours"))))
        table.resizeColumnsToContents()

    def _update_funnel_chart(self, stages: Sequence[Dict[str, Any]], total_orders: Any) -> None:
        self.op_funnel_set.remove(0, self.op_funnel_set.count())
        categories: List[str] = []
        max_count = max(int(total_orders or 0), 1)
        for entry in stages:
            if not isinstance(entry, dict):
                continue
            stage_name = str(entry.get("stage") or "unknown").replace("_", " ").title()
            count = int(entry.get("count") or 0)
            categories.append(stage_name)
            self.op_funnel_set.append(count)
            max_count = max(max_count, count)
        if not categories:
            categories = ["No data"]
            self.op_funnel_set.append(0)
        self.op_funnel_categories_axis.clear()
        self.op_funnel_categories_axis.append(categories)
        self.op_funnel_value_axis.setRange(0, max_count * 1.1)

    def _update_slowest_orders_table(self, records: Sequence[Dict[str, Any]]) -> None:
        table = self.op_slowest_orders_table
        table.setRowCount(0)
        for entry in records:
            if not isinstance(entry, dict):
                continue
            row = table.rowCount()
            table.insertRow(row)
            order_display = entry.get("order_reference") or entry.get("order_id") or ""
            table.setItem(row, 0, QTableWidgetItem(str(order_display)))
            table.setItem(row, 1, QTableWidgetItem(str(entry.get("customer_name") or "")))
            completion = self._format_hours(entry.get("completion_hours"))
            age = self._format_hours(entry.get("age_hours"))
            table.setItem(row, 2, QTableWidgetItem(completion))
            table.setItem(row, 3, QTableWidgetItem(age))
            status_raw = entry.get("status") or ""
            status_text = str(status_raw).replace("_", " ").title()
            table.setItem(row, 4, QTableWidgetItem(status_text))
        table.resizeColumnsToContents()

    def _set_loading(self, loading: bool) -> None:
        self._loading = loading
        self.refresh_button.setEnabled(not loading)
        self.start_date_edit.setEnabled(not loading)
        self.end_date_edit.setEnabled(not loading)
        self._timeframe_combo.setEnabled(not loading)
        if loading:
            self._spinner_index = 0
            self.spinner_label.setText(self._spinner_frames[self._spinner_index])
            self.spinner_label.setVisible(True)
            if not self._spinner_timer.isActive():
                self._spinner_timer.start()
        else:
            if self._spinner_timer.isActive():
                self._spinner_timer.stop()
            self.spinner_label.setVisible(False)
            self.spinner_label.setText("")

    def _update_status(self, message: str) -> None:
        self.status_label.setText(message)

    def _show_error(self, message: str) -> None:
        self._update_status("Update failed")
        box = QMessageBox(self)
        box.setIcon(QMessageBox.Critical)
        box.setWindowTitle("Error")
        box.setText(message)
        box.exec()


def launch_app(client: DataClientInterface) -> None:
    app = QApplication.instance() or QApplication([])
    window = MainWindow(client)
    window.show()
    window.refresh_data()
    app.exec()
