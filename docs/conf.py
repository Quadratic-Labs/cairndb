"""Sphinx configuration for the CairnDB documentation.

Build locally with ``make docs`` (one-off) or ``make docs-serve`` (live
reload). Pages are Markdown (MyST); the API reference is generated from
the Google-style docstrings in ``src/cairndb``.
"""

import sys
from importlib.metadata import version as _dist_version
from pathlib import Path

_DOCS = Path(__file__).parent
# Import cairndb from the checkout even without an editable install, and
# make the local extensions in docs/_ext importable.
sys.path.insert(0, str(_DOCS.parent / "src"))
sys.path.insert(0, str(_DOCS / "_ext"))

# -- Project -----------------------------------------------------------------

project = "CairnDB"
author = "CairnDB Contributors"
copyright = "2026, CairnDB Contributors"
release = _dist_version("cairndb")
version = ".".join(release.split(".")[:2])

# -- General -----------------------------------------------------------------

extensions = [
    "myst_parser",
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.intersphinx",
    "sphinx.ext.viewcode",
    "sphinx_copybutton",
    "sphinxcontrib.mermaid",
    "typer_cli",  # docs/_ext/typer_cli.py: the CLI reference
]

source_suffix = {".md": "markdown", ".rst": "restructuredtext"}
root_doc = "index"
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store", "_ext"]

# -- MyST --------------------------------------------------------------------

myst_enable_extensions = [
    "colon_fence",  # ::: admonitions
    "deflist",
    "fieldlist",
    "attrs_inline",
    "tasklist",  # - [ ] checklists
]
myst_heading_anchors = 3
# ```mermaid fences render as diagrams here and on GitHub alike.
myst_fence_as_directive = ["mermaid"]

# -- Autodoc -----------------------------------------------------------------

autodoc_default_options = {
    "members": True,
    "member-order": "bysource",
    "show-inheritance": True,
}
autoclass_content = "both"  # class docstring + __init__ Args
autodoc_typehints = "signature"
autodoc_preserve_defaults = True
# The cloud SDKs are optional extras; mock them so the storage backends'
# reference builds with only the docs extra installed.
autodoc_mock_imports = ["boto3", "botocore", "google", "azure"]

napoleon_google_docstring = True
napoleon_numpy_docstring = False
napoleon_use_rtype = False

intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
}

# -- HTML --------------------------------------------------------------------

html_theme = "furo"
html_baseurl = "https://quadratic-labs.github.io/cairndb/"  # published by .github/workflows/docs.yml
html_title = f"CairnDB {release}"
html_static_path = ["_static"]
html_css_files = ["custom.css"]
html_theme_options = {
    "source_repository": "https://github.com/Quadratic-Labs/cairndb/",
    "source_branch": "main",
    "source_directory": "docs/",
}

copybutton_prompt_text = r"\$ |>>> "
copybutton_prompt_is_regexp = True
