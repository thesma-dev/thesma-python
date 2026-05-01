"""Introspection tests pinning the SDK-40 ``cik`` → ``identifier`` rename
plus the SDK-42 follow-on rename of two query-param filters.

Verifies in one shot that:

* The 17 path-param company-resource methods (SDK-40) AND the two query-
  param filters renamed by SDK-42 (``filings.list_all`` and
  ``sections.search``) take ``identifier=`` as a kwarg and no longer
  accept ``cik=``.
* The two fund-CIK path methods on ``holdings`` still take ``cik=``
  (intentionally out of scope for both SDK-40 and SDK-42).
"""

from __future__ import annotations

import inspect

import pytest

from thesma.client import ThesmaClient

# 17 SDK-40 path-param methods + 2 SDK-42 query-param filters.
RENAMED_METHODS = [
    ("companies", "get"),
    ("financials", "get"),
    ("financials", "time_series"),
    ("ratios", "get"),
    ("ratios", "time_series"),
    ("events", "list"),
    ("insider_trades", "list"),
    ("insider_holdings", "list"),
    ("holdings", "holders"),
    ("holdings", "holder_changes"),
    ("compensation", "get"),
    ("compensation", "board"),
    ("proxy_votes", "list"),
    ("beneficial_ownership", "list"),
    ("sections", "list_by_company"),
    ("sections", "entities"),
    ("filings", "list"),
    # SDK-42 (T-230) query-param filter renames:
    ("filings", "list_all"),
    ("sections", "search"),
]

# Fund-CIK path methods that still take ``cik=`` — out of both T-221 and
# T-230 scope; the path segment is literally ``/funds/{cik}``.
NOT_RENAMED_METHODS = [
    ("holdings", "fund_holdings"),
    ("holdings", "fund_changes"),
]


@pytest.mark.parametrize("resource_name,method_name", RENAMED_METHODS)
def test_resource_methods_use_identifier_kwarg(resource_name: str, method_name: str, api_key: str) -> None:
    """Every renamed method takes ``identifier`` as a kwarg, not ``cik``."""
    client = ThesmaClient(api_key=api_key)
    try:
        resource = getattr(client, resource_name)
        method = getattr(resource, method_name)
        sig = inspect.signature(method)
        assert "identifier" in sig.parameters, (
            f"{resource_name}.{method_name} missing 'identifier' kwarg — rename incomplete"
        )
        assert "cik" not in sig.parameters, f"{resource_name}.{method_name} still has 'cik' kwarg — rename incomplete"
    finally:
        client.close()


@pytest.mark.parametrize("resource_name,method_name", NOT_RENAMED_METHODS)
def test_fund_methods_keep_cik_kwarg(resource_name: str, method_name: str, api_key: str) -> None:
    """Fund-CIK path methods still use ``cik`` kwarg (out of scope for the
    SDK-40 + SDK-42 identifier-rename arc)."""
    client = ThesmaClient(api_key=api_key)
    try:
        resource = getattr(client, resource_name)
        method = getattr(resource, method_name)
        sig = inspect.signature(method)
        assert "cik" in sig.parameters, f"{resource_name}.{method_name} should still take 'cik' (out of scope)"
    finally:
        client.close()
