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
        cik: str,
        *,
        page: int = 1,
        per_page: int = 25,
    ) -> PaginatedResponse[SectionSummary]:
        """List sections across a company's filings.

        ``GET /v1/us/sec/companies/{cik}/sections``
        """
        params: dict[str, Any] = {
            "page": page,
            "per_page": per_page,
        }
        return self._client.request(  # type: ignore[no-any-return]
            "GET",
            f"/v1/us/sec/companies/{cik}/sections",
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
        cik: str,
        section_type: str,
        *,
        page: int = 1,
        per_page: int = 50,
    ) -> PaginatedResponse[EntityResponse]:
        """List named entities extracted from a company's sections.

        ``GET /v1/us/sec/companies/{cik}/sections/{section_type}/entities``
        """
        params: dict[str, Any] = {
            "page": page,
            "per_page": per_page,
        }
        return self._client.request(  # type: ignore[no-any-return]
            "GET",
            f"/v1/us/sec/companies/{cik}/sections/{section_type}/entities",
            params=params,
            response_model=PaginatedResponse[EntityResponse],
        )

    def search(
        self,
        *,
        query: str,
        page: int = 1,
        per_page: int = 20,
    ) -> SearchPaginatedResponse:
        """Search section content using semantic similarity.

        ``GET /v1/us/sec/sections/search``
        """
        params: dict[str, Any] = {
            "q": query,
            "page": page,
            "per_page": per_page,
        }
        return self._client.request(  # type: ignore[no-any-return]
            "GET",
            "/v1/us/sec/sections/search",
            params=params,
            response_model=SearchPaginatedResponse,
        )
