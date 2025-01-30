"""
Package's Visualization's Module
"""
import json

import leafmap

class Map:
    """
        A simple Map class to visualize AIS data using leafmap.
        """

    def __init__(self, center=[0, 0], zoom=2):
        """
        Create a leafmap Map instance.
        """
        self.m = leafmap.Map(center=center, zoom=zoom)

    def add_route(self, route_geojson, layer_name="Route"):
        """
        Add a vessel route (GeoJSON) to the map.
        """
        if not route_geojson:
            print("Empty or invalid GeoJSON. Nothing to plot.")
            return

        self.m.add_geojson(json.dumps(route_geojson), name=layer_name)
