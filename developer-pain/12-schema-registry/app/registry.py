"""A small client for the Apicurio Registry v3 REST API: register schemas, fetch them by id."""
import json
from functools import lru_cache

import httpx

REGISTRY_URL = "http://localhost:8080/apis/registry/v3"
GROUP = "shop"


class CompatibilityError(Exception):
    """The registry refused a new schema version because it breaks the compatibility rule."""


class Registry:
    def __init__(self, base_url: str = REGISTRY_URL, group: str = GROUP):
        self._http = httpx.Client(base_url=base_url, timeout=10)
        self._group = group

    def register(self, artifact_id: str, schema: str) -> int:
        """Register a schema as a new version, or find the version that already has it.

        Returns the version's globalId, the number producers put in the message header.
        """
        r = self._http.post(
            f"/groups/{self._group}/artifacts",
            params={"ifExists": "FIND_OR_CREATE_VERSION"},
            json={
                "artifactId": artifact_id,
                "artifactType": "AVRO",
                "firstVersion": {"content": {"content": schema, "contentType": "application/json"}},
            },
        )
        if r.status_code == 409:
            raise CompatibilityError(r.json().get("detail", r.text))
        r.raise_for_status()
        return r.json()["version"]["globalId"]

    def require_backward_compatibility(self, artifact_id: str) -> None:
        """New versions must be readable by consumers that use the previous version."""
        r = self._http.post(
            f"/groups/{self._group}/artifacts/{artifact_id}/rules",
            json={"ruleType": "COMPATIBILITY", "config": "BACKWARD"},
        )
        if r.status_code not in (204, 409):  # 409: the rule is already set
            r.raise_for_status()

    @lru_cache(maxsize=256)
    def schema_by_id(self, global_id: int) -> dict:
        """Fetch the exact schema a message was written with. Cached: ids never change."""
        r = self._http.get(f"/ids/globalIds/{global_id}")
        r.raise_for_status()
        return json.loads(r.text)
