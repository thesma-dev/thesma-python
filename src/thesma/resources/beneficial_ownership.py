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
        identifier: str,
        *,
        page: int = 1,
        per_page: int = 25,
    ) -> PaginatedResponse[BeneficialOwnershipItem]:
        """List beneficial ownership filings for a company.

        ``GET /v1/us/sec/companies/{identifier}/beneficial-ownership``

        ``identifier`` accepts a CIK or ticker (case-insensitive, with
        ``TickerAlias`` fallback). Unknown identifiers raise
        :class:`thesma.errors.NotFoundError` (api ``0.12.0`` aligned this
        endpoint with the rest of the cluster — pre-``0.12.0`` it returned
        ``200`` with an empty list). Use :meth:`list_all` to query 13D/13G
        filings for subjects outside the tracked universe.
        """
        params: dict[str, Any] = {
            "page": page,
            "per_page": per_page,
        }
        return self._client.request(  # type: ignore[no-any-return]
            "GET",
            f"/v1/us/sec/companies/{identifier}/beneficial-ownership",
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
