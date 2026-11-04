"""
DO NOT DELETE — CodeQL Python language marker

IMPORTANT:
- This is NOT a GitHub Actions workflow file.
- GitHub Actions workflows are YAML files with .yml or .yaml extensions.
- This file is a plain Python module stored under .github/workflows by convention only.

Why this file exists:
- This repository uses CodeQL Advanced Setup (workflow-based scanning).
- Some environment branches may start out minimal.
- If there is no analyzable Python source code in the branch, CodeQL analysis can fail.

Rules:
- Do not import this module.
- Do not add business logic here.
- Keep it tiny and stable.
"""

__all__ = ()

def _codeql_language_marker_noop() -> None:
    return
