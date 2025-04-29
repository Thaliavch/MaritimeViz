# docs/conf.py
# -*- coding: utf-8 -*-
#
# MaritimeViz documentation build configuration file.

import os
import sys

sys.path.insert(0, os.path.abspath("../src"))
import src.maritimeviz as maritimeviz

# -- Project information -----------------------------------------------------

project = "MaritimeViz"
author = "Thalia Valle, Marcelo Amorin, Enrique Baggio, Paulo Drefhal"
copyright = f"2025, {author}"

# The short X.Y version
version = maritimeviz.__version__
# The full version, including alpha/beta/rc tags
release = maritimeviz.__version__

# -- General configuration ---------------------------------------------------

# Sphinx extensions
extensions = [
    "sphinx.ext.autodoc",  # for inline docstrings
    "sphinx.ext.viewcode",  # add links to the source code
    "sphinx.ext.napoleon",  # for Google/NumPy style docstrings
    "sphinx.ext.autosummary"  # generate stub pages for modules/classes
]

# If you use autosummary, uncomment the next line:
autosummary_generate = True

# Paths that contain templates, relative to this directory.
templates_path = ["_templates"]

# Source file suffix
source_suffix = ".rst"

# The master toctree document
master_doc = "index"

# Patterns to ignore when looking for source files
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

# -- Options for HTML output -------------------------------------------------

# Use the Read-The-Docs theme
html_theme = "sphinx_rtd_theme"

# If you want to tweak theme options, you can add e.g.:
# html_theme_options = {
#     "logo_only": True,
#     "display_version": True,
# }

# Add any paths that contain custom static files (such as style sheets)
html_static_path = ["_static"]

# -- Options for other output formats ----------------------------------------

# HTML help builder base name
htmlhelp_basename = "maritimevizdoc"

# LaTeX output
latex_documents = [
    (master_doc, "maritimeviz.tex", "MaritimeViz Documentation", author,
     "manual"),
]

# Manual page output
man_pages = [
    (master_doc, "maritimeviz", "MaritimeViz Documentation", [author], 1)]

# Texinfo output
texinfo_documents = [
    (master_doc, "maritimeviz", "MaritimeViz Documentation", author,
     "maritimeviz", "One line description of project.", "Miscellaneous"),
]
