"""Sections resource — filing section content, changes, entities, and search."""

from __future__ import annotations

from typing import Any

from thesma._generated.models import (
    EntityResponse,
    SearchPaginatedResponse,
    SectionChangeResponse,
    SectionDetail,
    SectionList,
    SectionSummary,
)
from thesma._types import DataResponse, PaginatedResponse


class Sections:
    """Resource for filing section endpoints."""

    def __init__(self, client: Any) -> None:
        self._client = client

    def list_by_company(
        self,
        identifier: str,
        *,
        page: int = 1,
        per_page: int = 25,
    ) -> PaginatedResponse[SectionSummary]:
        """List sections across a company's filings.

        ``GET /v1/us/sec/companies/{identifier}/sections``

        ``identifier`` accepts a CIK or ticker (case-insensitive, with
        ``TickerAlias`` fallback for stale tickers).
        """
        params: dict[str, Any] = {
            "page": page,
            "per_page": per_page,
        }
        return self._client.request(  # type: ignore[no-any-return]
            "GET",
            f"/v1/us/sec/companies/{identifier}/sections",
            params=params,
            response_model=PaginatedResponse[SectionSummary],
        )

    def list_by_filing(self, accession_number: str) -> DataResponse[SectionList]:
        """List sections for a specific filing.

        ``GET /v1/us/sec/filings/{accession_number}/sections``
        """
        return self._client.request(  # type: ignore[no-any-return]
            "GET",
            f"/v1/us/sec/filings/{accession_number}/sections",
            response_model=DataResponse[SectionList],
        )

    def get(self, accession_number: str, section_type: str) -> DataResponse[SectionDetail]:
        """Get a specific section of a filing.

        ``GET /v1/us/sec/filings/{accession_number}/sections/{section_type}``
        """
        return self._client.request(  # type: ignore[no-any-return]
            "GET",
            f"/v1/us/sec/filings/{accession_number}/sections/{section_type}",
            response_model=DataResponse[SectionDetail],
        )

    def changes(self, accession_number: str, section_type: str) -> DataResponse[SectionChangeResponse]:
        """Get changes between this section and the previous filing's version.

        ``GET /v1/us/sec/filings/{accession_number}/sections/{section_type}/changes``
        """
        return self._client.request(  # type: ignore[no-any-return]
            "GET",
            f"/v1/us/sec/filings/{accession_number}/sections/{section_type}/changes",
            response_model=DataResponse[SectionChangeResponse],
        )

    def entities(
        self,
        identifier: str,
        section_type: str,
        *,
        page: int = 1,
        per_page: int = 50,
    ) -> PaginatedResponse[EntityResponse]:
        """List named entities extracted from a company's sections.

        ``GET /v1/us/sec/companies/{identifier}/sections/{section_type}/entities``

        ``identifier`` accepts a CIK or ticker (see ``list_by_company``).
        """
        params: dict[str, Any] = {
            "page": page,
            "per_page": per_page,
        }
        return self._client.request(  # type: ignore[no-any-return]
            "GET",
            f"/v1/us/sec/companies/{identifier}/sections/{section_type}/entities",
            params=params,
            response_model=PaginatedResponse[EntityResponse],
        )

    def search(
        self,
        *,
        query: str,
        identifier: str | None = None,
        filing_type: str | None = None,
        section_type: str | None = None,
        year: int | None = None,
        min_similarity: float | None = None,
        page: int = 1,
        per_page: int = 20,
    ) -> SearchPaginatedResponse:
        """Search section content using semantic similarity.

        ``GET /v1/us/sec/sections/search``

        :param query: Search text. The api requires at least 3 characters
            after whitespace stripping; shorter queries return ``400`` →
            :class:`thesma.errors.BadRequestError`.
        :param identifier: Optional CIK (zero-padded or stripped 1-10
            digits) or current ticker symbol (case-insensitive) to scope
            the search to one company. Stale tickers fall back to
            ``TickerAlias`` (e.g. ``"FB"`` → META). Unknown identifiers
            return an empty result set, not a 4xx — consistent with every
            other query-filter param on this route.
        :param filing_type: Optional filing-type filter (e.g. ``"10-K"``,
            ``"10-Q"``, ``"20-F"``). Case-sensitive — use the canonical
            form. Pass-through.
        :param section_type: Optional section-type filter (e.g.
            ``"item_1a"``, ``"item_7"``). Case-sensitive. Pass-through.
        :param year: Optional fiscal-year filter. Filters on the
            section's ``fiscal_year`` field, not the calendar year of
            the filing date.
        :param min_similarity: Optional cosine-similarity floor in
            ``[0.0, 1.0]``. Defaults to ``0.3`` server-side when omitted;
            the SDK does not echo that default.
        :param page: 1-indexed page number.
        :param per_page: Page size. Capped at ``50`` server-side.
        """
        params: dict[str, Any] = {
            "q": query,
            "identifier": identifier,
            "filing_type": filing_type,
            "section_type": section_type,
            "year": year,
            "min_similarity": min_similarity,
            "page": page,
            "per_page": per_page,
        }
        return self._client.request(  # type: ignore[no-any-return]
            "GET",
            "/v1/us/sec/sections/search",
            params=params,
            response_model=SearchPaginatedResponse,
        )
