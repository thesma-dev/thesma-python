"""Compensation resource — executive compensation and board data."""

from __future__ import annotations

from typing import Any

from thesma._generated.models import BoardResponse, CompensationResponse
from thesma._types import DataResponse


class Compensation:
    """Resource for compensation and board endpoints."""

    def __init__(self, client: Any) -> None:
        self._client = client

    def get(
        self,
        cik: str,
        *,
        year: int | None = None,
    ) -> DataResponse[CompensationResponse]:
        """Get executive compensation for a company.

        ``GET /v1/us/sec/companies/{cik}/executive-compensation``
        """
        params: dict[str, Any] = {
            "year": year,
        }
        return self._client.request(  # type: ignore[no-any-return]
            "GET",
            f"/v1/us/sec/companies/{cik}/executive-compensation",
            params=params,
            response_model=DataResponse[CompensationResponse],
        )

    def board(self, cik: str) -> DataResponse[BoardResponse]:
        """Get board of directors for a company.

        ``GET /v1/us/sec/companies/{cik}/board``
        """
        return self._client.request(  # type: ignore[no-any-return]
            "GET",
            f"/v1/us/sec/companies/{cik}/board",
            response_model=DataResponse[BoardResponse],
        )
