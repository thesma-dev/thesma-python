"""Regenerate Pydantic models from the Thesma API OpenAPI schema.

This script fetches the OpenAPI spec from the Thesma API, generates
Pydantic v2 models using datamodel-code-generator, and writes them
to src/thesma/_generated/models.py.

Implementation in SDK-02.
"""

from __future__ import annotations


def main() -> None:
    """Regenerate models from OpenAPI schema."""
    raise NotImplementedError("Model regeneration not yet implemented — see SDK-02.")


if __name__ == "__main__":
    main()
