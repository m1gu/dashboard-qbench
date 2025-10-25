from qbench_dashboard.config import get_data_provider
from qbench_dashboard.services.client_interface import DataClientInterface
from qbench_dashboard.services.qbench_client import QBenchClient, QBenchError
from qbench_dashboard.services.local_api_client import LocalAPIClient, LocalAPIError


def create_data_client() -> DataClientInterface:
    """Factory function to create the appropriate data client based on configuration."""
    provider = get_data_provider()
    
    if provider in {"local", "online"}:
        try:
            return LocalAPIClient()
        except Exception as exc:
            raise LocalAPIError(f"Failed to initialize Local API client: {exc}") from exc
    elif provider == "qbench":
        try:
            return QBenchClient()
        except Exception as exc:
            raise QBenchError(f"Failed to initialize QBench client: {exc}") from exc
    else:
        raise ValueError(
            f"Unknown data provider: {provider}. Valid options are 'qbench', 'local' and 'online'."
        )
