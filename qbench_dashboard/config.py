import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent
load_dotenv(ROOT.parent / ".env")


@dataclass
class QBenchSettings:
    base_url: str
    client_id: str
    client_secret: str
    jwt_leeway: int = 5
    jwt_ttl: int = 3300


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
