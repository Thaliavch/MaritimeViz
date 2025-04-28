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
   :alt: Dependency Status

A Python package to ingest, analyze and visualize Automatic Identification System (AIS) data for maritime vessels, with built-in support for global views, rich export formats, interactive Leafmap maps, and integration with the Global Fishing Watch API.

**⚠️ DISCLAIMER:**
This is the **first public release (v0.1.0)** and is still under active development. APIs and behaviors may change in future versions.

Installation
------------

::

    pip install maritimeviz

Quickstart
----------

1. Build or open a DuckDB-backed AIS database and ingest a raw AIS stream:

   .. code-block:: python

       from maritimeviz.ais_db import AISDatabase

       db = AISDatabase("my_data.duckdb")
       db.process("raw_ais_stream.nmea")

2. Query global position reports as GeoJSON:

   .. code-block:: python

       geojson = db.get_geojson(report_type="position")

3. Render on an interactive map:

   .. note::
      The current `Map` class supports method chaining (e.g. ``.map_all(...).ship_routes(...)``), each call
      will add a new layer to the map; however we have to manually add layer control as depicted
      below. Note that this API is still evolving and may be refined in upcoming releases.

   .. code-block:: python

       from maritimeviz.viz import Map as VesselMap

       m1 = VesselMap()
       m1.map_all(geojson, layer_name="Position Reports") \
         .ship_routes(geojson)
       m1.add_layer_control()
       m1.m  # display in Jupyter or Colab

4. Fetch fishing events from Global Fishing Watch:

   .. code-block:: python

       from maritimeviz.gfw_api import GFW_api

       client = GFW_api()
       vessels = client.search_vessel(9111254)
       vessel_id = vessels[1]["combinedSourcesInfo"][0]["vesselId"]
       df_events = client.get_fishing_events(
           vessel_id,
           start_date="2023-01-01",
           end_date="2023-06-30",
           limit=20
       )
       df_events.head()

Core Features
-------------
- **AIS Database Management**

  • Ingest raw AIS streams (NMEA, CSV, etc.) sequentially.

  • Materialize global “position” and “static” views

  • Flexible filtering by MMSI, date range, or spatial polygon, and more.

  • Export to GeoJSON, CSV, Shapefile, KML, Excel, or WKT


- **Interactive Mapping**

  • Leafmap/Folium wrapper for points, routes, heatmaps, and base stations

  • Polygon‐based and MMSI filtering


- **Global Fishing Watch API**

  • Vessel identity lookup (MMSI/IMO → vesselId)

  • Fetch fishing events, effort statistics, vessel insights

  • Built-in mapping helpers


- **Extensible Architecture**

  • Pluggable “message processor” classes for Class A, Class B, long‐range, ASM, etc.

  • Utilities for cache clearing, and data cleaning.

  • Roadmap: buoy/water data ingestion, anomaly detection, ML pipelines


Credits
-------
- **Global Fishing Watch** for vessel & event data (https://globalfishingwatch.org)
- **libais** for AIS message decoding and parsing (https://github.com/schwehr/libais)
- Mentorship and guidance by **Kurt Schewher**
- Built on top of: DuckDB, Pandas, GeoPandas, Leafmap, Folium, Shapely, Cachetools, Requests
- Project template courtesy of audreyr/cookiecutter-pypackage

License
-------
Released under the **MIT License**.

