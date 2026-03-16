"""Beneficial ownership resource — SEC Schedule 13D/13G filings."""

from __future__ import annotations

from typing import Any

from thesma._generated.models import BeneficialOwnershipItem
from thesma._types import PaginatedResponse


class BeneficialOwnership:
    """Resource for beneficial ownership endpoints."""

    def __init__(self, client: Any) -> None:
        self._client = client

    def list(
        self,
        cik: str,
        *,
        page: int = 1,
        per_page: int = 25,
    ) -> PaginatedResponse[BeneficialOwnershipItem]:
        """List beneficial ownership filings for a company.

        ``GET /v1/us/sec/companies/{cik}/beneficial-ownership``
        """
        params: dict[str, Any] = {
            "page": page,
            "per_page": per_page,
        }
        return self._client.request(  # type: ignore[no-any-return]
            "GET",
            f"/v1/us/sec/companies/{cik}/beneficial-ownership",
            params=params,
            response_model=PaginatedResponse[BeneficialOwnershipItem],
        )

    def list_all(
        self,
        *,
        page: int = 1,
        per_page: int = 25,
    ) -> PaginatedResponse[BeneficialOwnershipItem]:
        """List beneficial ownership filings across all companies.

        ``GET /v1/us/sec/beneficial-ownership``
        """
        params: dict[str, Any] = {
            "page": page,
            "per_page": per_page,
        }
        return self._client.request(  # type: ignore[no-any-return]
            "GET",
            "/v1/us/sec/beneficial-ownership",
            params=params,
            response_model=PaginatedResponse[BeneficialOwnershipItem],
        )
