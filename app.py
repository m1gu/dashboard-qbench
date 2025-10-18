import sys

from PySide6.QtWidgets import QApplication, QMessageBox

from qbench_dashboard.services.client_factory import create_data_client
from qbench_dashboard.services.qbench_client import QBenchError
from qbench_dashboard.services.local_api_client import LocalAPIError
from qbench_dashboard.ui.main_window import launch_app


def main() -> None:
    try:
        client = create_data_client()
    except (RuntimeError, QBenchError, LocalAPIError, ValueError) as exc:
        app = QApplication.instance() or QApplication([])
        QMessageBox.critical(None, "Configuracion invalida", str(exc))
        sys.exit(1)
    launch_app(client)


if __name__ == "__main__":
    main()
