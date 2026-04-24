"""Compensation resource — executive compensation and board data."""

from __future__ import annotations

from typing import Any

from thesma._generated.models import BoardResponse, EnrichedCompensationDataResponse
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
        include: str | None = None,
    ) -> EnrichedCompensationDataResponse:
        """Get executive compensation for a company.

        ``GET /v1/us/sec/companies/{cik}/compensation``

        ``include`` accepts ``"labor_context"`` only — compensation does
        NOT carry ``lending_context``. Post-S3 the ``labor_context``
        field is the unified nested ``LaborContext`` with typed
        ``summary`` and ``data_freshness`` sub-objects; see
        ``LaborContext.summary.industry_hiring_trend`` for the derived-
        trend access pattern. If the labour enrichment builder times
        out or errors, the response stays 200 with
        ``result.labor_context`` set to ``None`` and
        ``result.enrichment_warnings`` carrying typed
        ``EnrichmentWarning`` entries (``field``, ``reason``,
        ``message``). A silent ``None`` without a matching warning
        means the builder legitimately had no data.
        """
        params: dict[str, Any] = {
            "year": year,
            "include": include,
        }
        return self._client.request(  # type: ignore[no-any-return]
            "GET",
            f"/v1/us/sec/companies/{cik}/compensation",
            params=params,
            response_model=EnrichedCompensationDataResponse,
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
