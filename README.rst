===========
MaritimeViz
===========


.. image:: https://img.shields.io/pypi/v/maritimeviz.svg
        :target: https://pypi.python.org/pypi/maritimeviz

.. image:: https://img.shields.io/travis/Thaliavch/maritimeviz.svg
        :target: https://travis-ci.com/Thaliavch/maritimeviz

.. image:: https://readthedocs.org/projects/maritimeviz/badge/?version=latest
        :target: https://maritimeviz.readthedocs.io/en/latest/?version=latest
        :alt: Documentation Status


.. image:: https://pyup.io/repos/github/Thaliavch/maritimeviz/shield.svg
     :target: https://pyup.io/repos/github/Thaliavch/maritimeviz/
     :alt: Updates



A Python package designed to analyze and visualize Automatic Identification System (AIS) data, enabling easy exploration of maritime vessel movements through data extraction, cleaning, and analysis. Utilize powerful libraries like NumPy and Pandas for efficient data manipulation, and leverage the interactive mapping capabilities of Leafmap for insightful visualizations of vessel trajectories and other key maritime information.


* Free software: MIT license
* Documentation: https://maritimeviz.readthedocs.io.


Features
--------

* TODO

Credits
-------

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage



Module Overview
===============

``viz.py``
----------

This module offers visualization utilities for maritime data. It includes functions to:

- Generate heatmaps of vessel traffic.
- Plot vessel trajectories over time.
- Create interactive maps highlighting specific maritime events or regions.

``maritimeviz.py``
------------------

Serving as the core of the application, this module orchestrates data processing and visualization. Key functionalities include:

- Integrating data from the AIS database.
- Applying filters based on vessel type, time range, or geographic area.
- Coordinating the generation of visual outputs using the ``viz`` module.

``ais_db.py``
-------------

This module manages interactions with the AIS database. Its responsibilities encompass:

- Establishing and managing database connections.
- Executing queries to retrieve AIS data.
- Preprocessing data for analysis and visualization.
