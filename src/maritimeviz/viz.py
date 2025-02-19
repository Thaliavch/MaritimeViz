"""
Package's Visualization's Module
"""
import json
from sys import prefix

import folium
import leafmap.foliumap as leafmap
import geopandas as gpd

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
        Adds a vessel route to the map using a GeoJSON representation.

        Parameters:
            route_geojson (dict or str): The GeoJSON data representing the vessel route.
            layer_name (str, optional): The name of the layer to be added to the map. Defaults to "Route".

        Behavior:
            - If the provided GeoJSON is empty or invalid, a message is printed and the function returns without modifying the map.
            - If valid GeoJSON is provided, it is added to the map as a GeoJSON layer.

        Example Usage:
            self.add_route(geojson_data, "Vessel Route")
        """

        if not route_geojson:
            print("Empty or invalid GeoJSON. Nothing to plot.")
            return

        self.m.add_geojson(json.dumps(route_geojson), name=layer_name)

    def map_all(self, route_geojson, layer_name="Route"):
        """
            Generates an interactive map displaying vessel locations and routes based on GeoJSON data.

            Parameters:
                route_geojson (str): The file path to a GeoJSON file containing vessel route data.
                layer_name (str, optional): The name of the map layer. Defaults to "Route".

            Behavior:
                - If the GeoJSON file is empty or invalid, a message is printed, and the function returns.
                - Reads the GeoJSON file into a GeoDataFrame.
                - If the data contains valid geometry, extracts longitude and latitude coordinates.
                - Initializes a map centered around the average latitude and longitude of the dataset.
                - Iterates through the data and places markers representing vessels.
                - Each marker includes:
                    - A popup displaying the ship's ID and speed.
                    - A tooltip showing ship details on hover.
                - Returns the generated map object.

            Returns:
                leafmap.Map: An interactive folium-based map with vessel locations.

            Example Usage:
                map_object = self.map_all("vessel_routes.geojson")
                map_object  # Display the map in a Jupyter Notebook or web interface.
            """



        if not route_geojson:
            print("Empty or invalid GeoJSON. Nothing to plot.")
            return
        gdf = gpd.read_file(route_geojson)
        if gdf.empty:
            print("No valid ship route data found.")
            return

        if "latitude" not in gdf.columns or "longitude" not in gdf.columns:
            gdf["longitude"] = gdf["geometry"].apply(lambda geom: geom.x)
            gdf["latitude"] = gdf["geometry"].apply(lambda geom: geom.y)

        self.m = leafmap.Map(location=[gdf.latitude.mean(), gdf.longitude.mean()], zoom_start=4)

        for _, row in gdf.iterrows():
            popup_text = f"Ship ID: {row['mmsi']}<br>Speed: {row['speed']} knots"
            tooltip_text = f"Ship ID: {row['mmsi']} | Speed: {row['speed']} knots"

            folium.Marker(
                icon=folium.Icon(color="blue", icon="ship", prefix='fa'),
                location=[row.geometry.y, row.geometry.x],  # Latitude, Longitude
                popup=folium.Popup(popup_text, max_width=250),  # Click for details
                tooltip=tooltip_text  # Hover to see info
            ).add_to(self.m)


        return self.m

