from __future__ import annotations

from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

from concurrent.futures import ThreadPoolExecutor

from PySide6.QtCharts import (
    QAreaSeries,
    QBarCategoryAxis,
    QBarSeries,
    QBarSet,
    QChart,
    QChartView,
    QDateTimeAxis,
    QLineSeries,
    QValueAxis,
)
from PySide6.QtCore import QDate, QDateTime, QLocale, QTimer, Qt, QThread, QObject, Signal
from PySide6.QtGui import QBrush, QCursor, QColor, QGradient, QLinearGradient, QPalette, QPainter, QPen
from PySide6.QtWidgets import (
    QApplication,
    QAbstractItemView,
    QCalendarWidget,
    QCheckBox,
    QDateEdit,
    QFrame,
    QGridLayout,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
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
    ) -> None:
        super().__init__()
        self._client = client
        self._start_date = start_date
        self._end_date = end_date

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

            with ThreadPoolExecutor(max_workers=3) as executor:
                tests_future = executor.submit(
                    self._client.count_recent_tests,
                    start_date=self._start_date,
                    end_date=self._end_date,
                    sample_ids=tests_sample_ids,
                    previous_range=previous_range,
                )
                customers_future = executor.submit(_load_customers)
                orders_future = executor.submit(_load_orders)

                (
                    tests_total,
                    tests_series,
                    tat_sum_seconds,
                    tat_count,
                    tat_daily,
                    tat_previous_daily,
                ) = tests_future.result()
                customer_records, customers_total = customers_future.result()
                customer_orders = orders_future.result()
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
            summary = build_summary(
                samples_total=samples_total,
                samples_series=samples_series,
                tests_total=tests_total,
                tests_series=tests_series,
                tests_tat_sum=tat_sum_seconds,
                tests_tat_count=tat_count,
                tests_tat_daily=tat_daily,
                tests_tat_daily_previous=tat_previous_daily,
                customers_total=customers_total,
                reports_total=reports_total,
                customers_recent=customer_records,
                customer_test_totals=toppers,
                start_date=self._start_date,
                end_date=self._end_date,
            )
        except Exception as exc:  # pylint: disable=broad-except
            self.error.emit(str(exc))
        else:
            self.finished.emit(summary)


class MainWindow(QMainWindow):
    def __init__(self, client: DataClientInterface) -> None:
        super().__init__()
        self._client = client
        self._thread: Optional[QThread] = None
        self._worker: Optional[SummaryWorker] = None
        self._loading = False
        self._tat_target_seconds = 48 * 3600  # 48-hour SLA target by default
        self._tat_moving_average_window = 7
        self._tat_tooltip_data: Dict[int, Tuple[datetime, float, int]] = {}

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
        self.chart_view.setMinimumHeight(320)
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
        controls_layout.addWidget(self.refresh_button)
        controls_layout.addStretch()

        metrics_layout = QGridLayout()
        metrics_layout.setHorizontalSpacing(24)
        metrics_layout.setVerticalSpacing(6)
        metrics_layout.setColumnStretch(0, 1)
        metrics_layout.setColumnStretch(1, 1)
        metrics_layout.setColumnStretch(2, 1)
        metrics_layout.setColumnStretch(3, 1)
        metrics_layout.setColumnStretch(4, 1)

        metrics_layout.setColumnStretch(0, 1)
        metrics_layout.setColumnStretch(1, 1)
        metrics_layout.setColumnStretch(2, 1)
        metrics_layout.setColumnStretch(3, 1)
        metrics_layout.setColumnStretch(4, 1)

        self.samples_value = QLabel("--")
        self.samples_value.setAlignment(Qt.AlignCenter)
        self.samples_value.setStyleSheet("font-size: 26px; font-weight: 600; color: #E0E8FF;")
        self.samples_label = QLabel("Samples")
        self.samples_label.setAlignment(Qt.AlignCenter)
        self.samples_label.setStyleSheet("color: #B0BCD5;")

        self.tests_value = QLabel("--")
        self.tests_value.setAlignment(Qt.AlignCenter)
        self.tests_value.setStyleSheet("font-size: 26px; font-weight: 600; color: #7EE787;")
        self.tests_label = QLabel("Tests")
        self.tests_label.setAlignment(Qt.AlignCenter)
        self.tests_label.setStyleSheet("color: #B0BCD5;")

        self.customers_value = QLabel("--")
        self.customers_value.setAlignment(Qt.AlignCenter)
        self.customers_value.setStyleSheet("font-size: 26px; font-weight: 600; color: #F4B400;")
        self.customers_label = QLabel("Customers")
        self.customers_label.setAlignment(Qt.AlignCenter)
        self.customers_label.setStyleSheet("color: #B0BCD5;")

        self.reports_value = QLabel("--")
        self.reports_value.setAlignment(Qt.AlignCenter)
        self.reports_value.setStyleSheet("font-size: 26px; font-weight: 600; color: #FF8FAB;")
        self.reports_label = QLabel("Reports")
        self.reports_label.setAlignment(Qt.AlignCenter)
        self.reports_label.setStyleSheet("color: #B0BCD5;")

        self.tat_value = QLabel("--")
        self.tat_value.setAlignment(Qt.AlignCenter)
        self.tat_value.setStyleSheet("font-size: 26px; font-weight: 600; color: #60CDF1;")
        self.tat_label = QLabel("Avg TAT")
        self.tat_label.setAlignment(Qt.AlignCenter)
        self.tat_label.setStyleSheet("color: #B0BCD5;")

        metrics_layout.addWidget(self.samples_value, 0, 0)
        metrics_layout.addWidget(self.samples_label, 1, 0)
        metrics_layout.addWidget(self.tests_value, 0, 1)
        metrics_layout.addWidget(self.tests_label, 1, 1)
        metrics_layout.addWidget(self.customers_value, 0, 2)
        metrics_layout.addWidget(self.customers_label, 1, 2)
        metrics_layout.addWidget(self.reports_value, 0, 3)
        metrics_layout.addWidget(self.reports_label, 1, 3)
        metrics_layout.addWidget(self.tat_value, 0, 4)
        metrics_layout.addWidget(self.tat_label, 1, 4)
        layout = QVBoxLayout()
        layout.setSpacing(20)
        layout.addLayout(metrics_layout)

        status_layout = QHBoxLayout()
        status_layout.setContentsMargins(0, 0, 0, 0)
        status_layout.setSpacing(6)
        status_layout.setAlignment(Qt.AlignCenter)
        status_layout.addWidget(self.spinner_label)
        status_layout.addWidget(self.status_label)
        layout.addLayout(status_layout)

        layout.addLayout(controls_layout)
        layout.addWidget(self.chart_view)
        self._init_bottom_lists(layout)

        container = QWidget()
        container.setLayout(layout)
        container.setStyleSheet("background-color: #0F172A;")

        self.setCentralWidget(container)

    def _init_bottom_lists(self, parent_layout: QVBoxLayout) -> None:
        lists_layout = QHBoxLayout()
        lists_layout.setSpacing(16)
        self.new_customers_table = self._create_table_widget(["ID", "Name", "Created"])
        new_customers_panel = self._create_list_panel("New customers", self.new_customers_table)
        lists_layout.addWidget(new_customers_panel, 1)

        self.top_tests_table = self._create_table_widget(["ID", "Name", "Tests"])
        top_tests_panel = self._create_list_panel("Top 10 customers with more tests", self.top_tests_table)
        lists_layout.addWidget(top_tests_panel, 1)

        tat_panel = self._create_tat_panel()
        lists_layout.addWidget(tat_panel, 1)
        parent_layout.addLayout(lists_layout)

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
        self.tat_chart_view.setMinimumHeight(300)
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

        return frame

    def _create_placeholder_panel(self, title: str) -> QFrame:
        label = QLabel("Coming soon")
        label.setAlignment(Qt.AlignCenter)
        label.setWordWrap(True)
        label.setStyleSheet("color: #5F718F; font-size: 13px;")
        return self._create_list_panel(title, label)

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

    def refresh_data(self) -> None:
        if self._loading:
            return
        try:
            start_dt, end_dt = self._get_selected_range()
        except ValueError as exc:
            self._show_error(str(exc))
            return

        self._set_loading(True)
        status_message = "Updating..."
        range_text = self._format_range(start_dt, end_dt)
        if range_text:
            status_message += f" Range: {range_text}"
        self._update_status(status_message)

        self._thread = QThread(self)
        self._worker = SummaryWorker(
            self._client,
            start_date=start_dt,
            end_date=end_dt,
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

        tat_daily = summary.get("tests_tat_daily")
        tat_previous = summary.get("tests_tat_daily_previous")
        daily_list = tat_daily if isinstance(tat_daily, list) else []
        previous_list = tat_previous if isinstance(tat_previous, list) else []
        self._update_tat_chart(daily_list, previous_list)

        range_text = self._format_range(start_dt, end_dt)
        now = datetime.now(timezone.utc)
        status_parts = [f"Last update: {now.strftime('%Y-%m-%d %H:%M:%S UTC')}"]
        if range_text:
            status_parts.append(f"Range: {range_text}")
        self._update_status(" | ".join(status_parts))

        samples_series = summary.get("samples_series") or []
        tests_series = summary.get("tests_series") or []

        daily_counts = {}
        for dt_value, count in samples_series:
            if isinstance(dt_value, datetime):
                day = dt_value.date()
                daily_counts.setdefault(day, [0, 0])[0] = int(count)
        for dt_value, count in tests_series:
            if isinstance(dt_value, datetime):
                day = dt_value.date()
                daily_counts.setdefault(day, [0, 0])[1] = int(count)

        self.samples_set.remove(0, self.samples_set.count())
        self.tests_set.remove(0, self.tests_set.count())

        sorted_days = sorted(daily_counts.keys())
        category_labels = []
        max_value = 1
        for day in sorted_days:
            sample_count, test_count = daily_counts[day]
            category_labels.append(day.strftime('%b %d'))
            self.samples_set.append(float(sample_count))
            self.tests_set.append(float(test_count))
            max_value = max(max_value, sample_count, test_count)

        if not category_labels:
            reference = datetime.now(timezone.utc)
            category_labels = [reference.strftime('%b %d')]
            self.samples_set.append(0.0)
            self.tests_set.append(0.0)
            max_value = 1

        self.categories_axis.clear()
        self.categories_axis.append(category_labels)
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

    def _set_loading(self, loading: bool) -> None:
        self._loading = loading
        self.refresh_button.setEnabled(not loading)
        self.start_date_edit.setEnabled(not loading)
        self.end_date_edit.setEnabled(not loading)
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
