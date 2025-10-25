from __future__ import annotations

from typing import Optional

import requests


class ConnectivityError(RuntimeError):
    """Raised when the application cannot reach the required online service."""


def ensure_online_connectivity(base_url: str, *, timeout: float = 3.0) -> None:
    """Check that the dashboard can reach the remote API prior to launch."""
    target = base_url.strip() if base_url else ""
    if not target:
        raise ConnectivityError("Endpoint de servicio no configurado.")

    session = requests.Session()
    session.headers.setdefault("User-Agent", "MCRLabsDashboard/1.0")
    response: Optional[requests.Response] = None
    try:
        try:
            response = session.get(target, timeout=timeout, allow_redirects=True, stream=True)
        except requests.RequestException as exc:
            raise ConnectivityError(
                "No se pudo establecer conexión con el servicio en línea. "
                "Verifica tu acceso a internet e inténtalo nuevamente."
            ) from exc
    finally:
        if response is not None:
            response.close()
        session.close()
