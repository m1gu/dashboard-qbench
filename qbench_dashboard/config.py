import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent
load_dotenv(ROOT.parent / ".env")

DEFAULT_LOCAL_BASE_URLS = {
    "local": "http://localhost:8000",
    "online": "https://6v12xcxn-8000.use.devtunnels.ms",
}


@dataclass
class QBenchSettings:
    base_url: str
    client_id: str
    client_secret: str
    jwt_leeway: int = 5
    jwt_ttl: int = 3300


@dataclass
class LocalAPISettings:
    base_url: str


def get_qbench_settings() -> QBenchSettings:
    base_url = os.getenv("QBENCH_BASE_URL", "").rstrip("/")
    client_id = os.getenv("QBENCH_CLIENT_ID", "")
    client_secret = os.getenv("QBENCH_CLIENT_SECRET", "")
    jwt_leeway = int(os.getenv("QBENCH_JWT_LEEWAY_S", "5"))
    jwt_ttl = int(os.getenv("QBENCH_JWT_TTL_S", "3300"))

    missing = [
        name for name, value in (
            ("QBENCH_BASE_URL", base_url),
            ("QBENCH_CLIENT_ID", client_id),
            ("QBENCH_CLIENT_SECRET", client_secret),
        )
        if not value
    ]
    if missing:
        raise RuntimeError(f"Missing environment variables: {', '.join(missing)}")

    return QBenchSettings(
        base_url=base_url,
        client_id=client_id,
        client_secret=client_secret,
        jwt_leeway=jwt_leeway,
        jwt_ttl=jwt_ttl,
    )


def get_local_api_settings() -> LocalAPISettings:
    provider = get_data_provider()
    default_base = DEFAULT_LOCAL_BASE_URLS.get(provider, DEFAULT_LOCAL_BASE_URLS["local"])
    base_value = os.getenv("LOCAL_API_BASE_URL", default_base)
    base_url = base_value.rstrip("/")
    return LocalAPISettings(base_url=base_url)


def get_data_provider() -> str:
    """Returns the configured data provider: 'qbench', 'local' or 'online'"""
    return os.getenv("DATA_PROVIDER", "qbench").lower()
