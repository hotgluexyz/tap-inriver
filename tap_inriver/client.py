"""HTTP client for inRiver REST API."""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

import backoff
import requests
from hotglue_singer_sdk.exceptions import FatalAPIError, RetriableAPIError


def normalize_base_url(base: str) -> str:
    """Ensure single trailing slash for urljoin."""
    return base.rstrip("/") + "/"


class InRiverClient:
    """Thin wrapper around requests with API key auth and JSON helpers."""

    def __init__(self, base_url: str, api_key: str, timeout: int = 300) -> None:
        self.base_url = normalize_base_url(base_url)
        self.api_key = api_key
        self.timeout = timeout
        self._session = requests.Session()
        self._session.headers.update(
            {
                "X-inRiver-APIKey": api_key,
                "Accept": "application/json",
                "Content-Type": "application/json",
            }
        )

    def _url(self, path: str) -> str:
        path = path if path.startswith("/") else f"/{path}"
        return urljoin(self.base_url, path.lstrip("/"))

    @backoff.on_exception(
        backoff.expo,
        (RetriableAPIError, requests.exceptions.ConnectionError, requests.exceptions.Timeout),
        max_tries=5,
        factor=2,
    )
    def post_json(self, path: str, body: Optional[dict]) -> Any:
        """POST JSON and return parsed JSON."""
        url = self._url(path)
        resp = self._session.post(url, json=body, timeout=self.timeout)
        if resp.status_code in (429,) or 500 <= resp.status_code < 600:
            raise RetriableAPIError(f"{resp.status_code} {resp.reason}: {resp.text[:500]}")
        if 400 <= resp.status_code < 500:
            raise FatalAPIError(f"{resp.status_code} {resp.reason}: {resp.text[:500]}")
        if resp.status_code != 200:
            raise FatalAPIError(f"Unexpected {resp.status_code}: {resp.text[:500]}")
        if not resp.content:
            return None
        return resp.json()

    @backoff.on_exception(
        backoff.expo,
        (RetriableAPIError, requests.exceptions.ConnectionError, requests.exceptions.Timeout),
        max_tries=5,
        factor=2,
    )
    def get_json(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """GET and return parsed JSON. Optional query params for endpoints like /entities/{id}/links."""
        url = self._url(path)
        resp = self._session.get(url, params=params or {}, timeout=self.timeout)
        if resp.status_code in (429,) or 500 <= resp.status_code < 600:
            raise RetriableAPIError(f"{resp.status_code} {resp.reason}: {resp.text[:500]}")
        if 400 <= resp.status_code < 500:
            raise FatalAPIError(f"{resp.status_code} {resp.reason}: {resp.text[:500]}")
        if resp.status_code != 200:
            raise FatalAPIError(f"Unexpected {resp.status_code}: {resp.text[:500]}")
        if not resp.content:
            return None
        return resp.json()

    def get_links(
        self,
        entity_id: int,
        link_type_id: str,
        link_direction: str,
    ) -> List[dict]:
        """GET /api/v1.0.0/entities/{entityId}/links (filter by link type and direction)."""
        path = f"/api/v1.0.0/entities/{entity_id}/links"
        params = {
            "linkTypeId": link_type_id,
            "linkDirection": link_direction,
        }
        data = self.get_json(path, params=params)
        if data is None:
            return []
        if not isinstance(data, list):
            return []
        return data


def field_type_id_to_snake(field_type_id: str) -> str:
    """Map e.g. ItemName -> item_name, SKU -> sku."""
    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", field_type_id)
    s2 = re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1)
    return s2.lower()
