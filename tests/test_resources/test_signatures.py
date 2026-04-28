"""Introspection tests pinning the SDK-40 ``cik`` → ``identifier`` rename.

Verifies in one shot that:

* All 17 path-param company-resource methods take ``identifier=`` as a kwarg
  and no longer accept ``cik=``.
* All 7 OOS methods (fund-CIK methods + ``?cik=`` query-param filters) still
  take ``cik=`` — guards against an over-rename via global find-replace.
"""

from __future__ import annotations

import inspect

import pytest

from thesma.client import ThesmaClient

# AC #10: the 17 renamed methods take ``identifier=`` as a kwarg.
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
]

# Methods that genuinely accept a ``cik`` parameter and are out of T-221 scope:
# the two fund-CIK path methods, plus the two cross-company query-param ``?cik=``
# filters that exist today (``filings.list_all``, ``sections.search``).
#
# The spec's exclusion table also listed ``events.list_all``,
# ``insider_trades.list_all``, and ``beneficial_ownership.list_all`` as
# ``?cik=`` filter methods, but inspection shows those three do not actually
# take a ``cik`` kwarg — there is nothing to assert about non-renaming for
# them, so they are not in this fixture. Their non-renaming is implicitly
# guaranteed by the fact that they have no ``cik`` parameter to rename.
NOT_RENAMED_METHODS = [
    ("holdings", "fund_holdings"),
    ("holdings", "fund_changes"),
    ("filings", "list_all"),
    ("sections", "search"),
]


@pytest.mark.parametrize("resource_name,method_name", RENAMED_METHODS)
def test_company_resource_methods_use_identifier_kwarg(resource_name: str, method_name: str, api_key: str) -> None:
    """Pin AC #10: every renamed method takes ``identifier`` as a kwarg, not ``cik``."""
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
def test_non_company_resource_methods_keep_cik_kwarg(resource_name: str, method_name: str, api_key: str) -> None:
    """Pin AC #8/#9: fund methods and query-param filters still use ``cik`` kwarg."""
    client = ThesmaClient(api_key=api_key)
    try:
        resource = getattr(client, resource_name)
        method = getattr(resource, method_name)
        sig = inspect.signature(method)
        assert "cik" in sig.parameters, f"{resource_name}.{method_name} should still take 'cik' (not in T-221 scope)"
    finally:
        client.close()
