from __future__ import annotations

from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple

from PySide6.QtCharts import QChart, QChartView, QDateTimeAxis, QLineSeries, QValueAxis
from PySide6.QtCore import QDate, QDateTime, QLocale, Qt, QThread, QObject, Signal
from PySide6.QtGui import QBrush, QCursor, QColor, QPalette, QPainter
from PySide6.QtWidgets import (
    QApplication,
    QCalendarWidget,
    QDateEdit,
    QGridLayout,
    QHBoxLayout,
    QLabel,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QToolTip,
    QVBoxLayout,
    QWidget,
)

from qbench_dashboard.services.qbench_client import QBenchClient
from qbench_dashboard.services.summary import build_summary


class SummaryWorker(QObject):
    finished = Signal(dict)
    error = Signal(str)

    def __init__(
        self,
        client: QBenchClient,
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
            sample_ids = []
            seen_ids = set()
            for sample in samples:
                sid = sample.get("id")
                if sid and sid not in seen_ids:
                    seen_ids.add(sid)
                    sample_ids.append(sid)
            tests_total, tests_series, tat_sum_seconds, tat_count = self._client.count_recent_tests(
                start_date=self._start_date,
                end_date=self._end_date,
                sample_ids=sample_ids,
            )
            customers_total = self._client.count_recent_customers(
                start_date=self._start_date,
                end_date=self._end_date,
            )
            reports_total = sum(
                1
                for sample in samples
                if sample.get("has_report") or str(sample.get("status", "")).upper() == "REPORTED"
            )
            summary = build_summary(
                samples_total=len(samples),
                samples_series=samples_series,
                tests_total=tests_total,
                tests_series=tests_series,
                tests_tat_sum=tat_sum_seconds,
                tests_tat_count=tat_count,
                customers_total=customers_total,
                reports_total=reports_total,
                start_date=self._start_date,
                end_date=self._end_date,
            )
        except Exception as exc:  # pylint: disable=broad-except
            self.error.emit(str(exc))
        else:
            self.finished.emit(summary)


class MainWindow(QMainWindow):
    def __init__(self, client: QBenchClient) -> None:
        super().__init__()
        self._client = client
        self._thread: Optional[QThread] = None
        self._worker: Optional[SummaryWorker] = None
        self._loading = False

        self.setWindowTitle("QBench Dashboard")
        self.resize(1280, 720)
        self._apply_dark_palette()

        self.status_label = QLabel("Listo")
        self.status_label.setAlignment(Qt.AlignCenter)
        self.status_label.setStyleSheet("color: #B0BCD5;")

        self.start_date_edit = self._create_date_edit()
        self.end_date_edit = self._create_date_edit()
        self._initialize_default_range()

        self.refresh_button = QPushButton("Refrescar")
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

        self.samples_series_line = QLineSeries()
        self.samples_series_line.setName("Samples")
        self.samples_series_line.setColor(QColor(0x4C, 0x6E, 0xF5))

        self.tests_series_line = QLineSeries()
        self.tests_series_line.setName("Tests")
        self.tests_series_line.setColor(QColor(0x7E, 0xE7, 0x87))

        self.chart.addSeries(self.samples_series_line)
        self.chart.addSeries(self.tests_series_line)

        self.time_axis = QDateTimeAxis()
        self.time_axis.setFormat("MMM d")
        self.time_axis.setLabelsColor(Qt.white)
        self.time_axis.setTitleText("Fecha")
        self.time_axis.setTitleBrush(Qt.white)

        self.value_axis = QValueAxis()
        self.value_axis.setLabelFormat("%d")
        self.value_axis.setLabelsColor(Qt.white)
        self.value_axis.setTitleText("Conteo")
        self.value_axis.setTitleBrush(Qt.white)

        self.chart.addAxis(self.time_axis, Qt.AlignBottom)
        self.chart.addAxis(self.value_axis, Qt.AlignLeft)
        self.samples_series_line.attachAxis(self.time_axis)
        self.samples_series_line.attachAxis(self.value_axis)
        self.tests_series_line.attachAxis(self.time_axis)
        self.tests_series_line.attachAxis(self.value_axis)

        self.chart_view = QChartView(self.chart)
        self.chart_view.setRenderHint(QPainter.Antialiasing, True)
        self.chart_view.setMinimumHeight(320)
        self.chart_view.setStyleSheet("background: rgba(32, 40, 62, 0.6);")

        self.samples_series_line.hovered.connect(lambda point, state: self._on_series_hover(point, state, "Samples"))
        self.tests_series_line.hovered.connect(lambda point, state: self._on_series_hover(point, state, "Tests"))

        controls_layout = QHBoxLayout()
        controls_layout.setSpacing(12)
        controls_layout.addStretch()
        start_label = QLabel("Desde")
        start_label.setStyleSheet("color: #B0BCD5; font-size: 14px;")
        controls_layout.addWidget(start_label)
        controls_layout.addWidget(self.start_date_edit)
        end_label = QLabel("Hasta")
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
        self.tat_label = QLabel("TAT Promedio")
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
        layout.addWidget(self.status_label)
        layout.addLayout(controls_layout)
        layout.addWidget(self.chart_view)

        container = QWidget()
        container.setLayout(layout)
        container.setStyleSheet("background-color: #0F172A;")

        self.setCentralWidget(container)

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
        status_message = "Actualizando..."
        range_text = self._format_range(start_dt, end_dt)
        if range_text:
            status_message += f" Rango: {range_text}"
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
        range_text = self._format_range(start_dt, end_dt)
        now = datetime.now(timezone.utc)
        status_parts = [f"Ultima actualizacion: {now.strftime('%Y-%m-%d %H:%M:%S UTC')}"]
        if range_text:
            status_parts.append(f"Rango: {range_text}")
        self._update_status(" | ".join(status_parts))

        samples_series = summary.get("samples_series") or []
        tests_series = summary.get("tests_series") or []

        self.samples_series_line.clear()
        self.tests_series_line.clear()

        axis_datetimes = []
        max_value = 1

        def _populate(series, target):
            nonlocal max_value
            for dt_value, count in series:
                if not isinstance(dt_value, datetime):
                    continue
                axis_datetimes.append(dt_value)
                max_value = max(max_value, int(count))
                qdt = QDateTime(dt_value)
                target.append(qdt.toMSecsSinceEpoch(), float(count))

        _populate(samples_series, self.samples_series_line)
        _populate(tests_series, self.tests_series_line)

        axis_start = min(axis_datetimes) if axis_datetimes else None
        axis_end = max(axis_datetimes) if axis_datetimes else None

        if not axis_start and isinstance(start_dt, datetime):
            axis_start = start_dt
        if not axis_end and isinstance(end_dt, datetime):
            axis_end = end_dt

        if axis_start is None or axis_end is None:
            reference = datetime.now(timezone.utc)
            qref_end = QDateTime(reference)
            qref_start = QDateTime(reference).addDays(-1)
            self.time_axis.setRange(qref_start, qref_end)
            axis_end = reference
        else:
            self.time_axis.setRange(QDateTime(axis_start), QDateTime(axis_end))

        if not samples_series and axis_end:
            qdt = QDateTime(axis_end)
            self.samples_series_line.append(qdt.toMSecsSinceEpoch(), 0.0)
        if not tests_series and axis_end:
            qdt = QDateTime(axis_end)
            self.tests_series_line.append(qdt.toMSecsSinceEpoch(), 0.0)

        self.value_axis.setRange(0, max_value + 1)

    def _on_series_hover(self, point, state: bool, label: str) -> None:
        if not state:
            QToolTip.hideText()
            return
        dt = QDateTime.fromMSecsSinceEpoch(int(point.x()))
        tooltip = f"{label}: {int(point.y())} ({dt.toString('yyyy-MM-dd')})"
        QToolTip.showText(QCursor.pos(), tooltip, self.chart_view)

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
            raise ValueError("La fecha inicial no puede ser posterior a la final.")
        if end_dt - start_dt > timedelta(days=30):
            raise ValueError("El rango de fechas no puede superar 30 dias.")
        return start_dt, end_dt

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

    def _update_status(self, message: str) -> None:
        self.status_label.setText(message)

    def _show_error(self, message: str) -> None:
        self._update_status("Error al actualizar")
        box = QMessageBox(self)
        box.setIcon(QMessageBox.Critical)
        box.setWindowTitle("Error")
        box.setText(message)
        box.exec()


def launch_app(client: QBenchClient) -> None:
    app = QApplication.instance() or QApplication([])
    window = MainWindow(client)
    window.show()
    window.refresh_data()
    app.exec()
