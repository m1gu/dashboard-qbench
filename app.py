import sys

from PySide6.QtWidgets import QApplication, QMessageBox

from qbench_dashboard.config import get_data_provider, get_local_api_settings, is_frozen_build
from qbench_dashboard.services.client_factory import create_data_client
from qbench_dashboard.services.qbench_client import QBenchError
from qbench_dashboard.services.local_api_client import LocalAPIError
from qbench_dashboard.ui.main_window import launch_app
from qbench_dashboard.services.connectivity import ConnectivityError, ensure_online_connectivity


def main() -> None:
    try:
        provider = get_data_provider()
        if provider == "online":
            base_url = get_local_api_settings().base_url
            timeout = 3.0 if is_frozen_build() else 5.0
            ensure_online_connectivity(base_url, timeout=timeout)
        client = create_data_client()
    except (RuntimeError, QBenchError, LocalAPIError, ValueError, ConnectivityError) as exc:
        app = QApplication.instance() or QApplication([])
        QMessageBox.critical(None, "Configuracion invalida", str(exc))
        sys.exit(1)
    launch_app(client)


if __name__ == "__main__":
    main()
