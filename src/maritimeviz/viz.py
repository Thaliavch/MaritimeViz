"""
Package's Visualization's Module
"""
import json
from sys import prefix

import folium
from folium import Map, Marker, Icon, Popup
from branca.element import Element
import leafmap.foliumap as leafmap
import geopandas as gpd
from shapely.wkt import loads

from src.maritimeviz.utils.viz_utils import plot_with_info, get_info, create_speed_legend

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
        else:
            # Extract latitude and longitude if not already present
            if "latitude" not in gdf.columns or "longitude" not in gdf.columns:
                gdf["longitude"] = gdf["geometry"].apply(lambda geom: geom.x if geom else None)
                gdf["latitude"] = gdf["geometry"].apply(lambda geom: geom.y if geom else None)

            # Ensure there are valid coordinates
            if gdf["latitude"].isnull().all() or gdf["longitude"].isnull().all():
                print("No valid coordinates found in the data.")
            else:
                self.m = leafmap.Map(location=[gdf.latitude.mean(), gdf.longitude.mean()], zoom_start=4)

                for _, row in gdf.iterrows():
                    # Extract all available data dynamically
                    info_text = "<br>".join(
                        [f"{key}: {value}" for key, value in row.items() if value and key != "geometry"])
                        # testing

                    name = row.get("mmsi", row.get("name", row.get("id", "Unknown")))

                    # Ensure latitude and longitude are valid
                    if row.geometry and hasattr(row.geometry, "x") and hasattr(row.geometry, "y"):
                        folium.Marker(
                            icon=folium.Icon(color="blue", icon="ship", prefix="fa"),
                            location=[row.geometry.y, row.geometry.x],  # Latitude, Longitude
                            popup=folium.Popup(info_text, max_width=300),  # Display all available info
                            tooltip='Press for more info'
                        ).add_to(self.m)

        return self.m

    def filter_ships_by_polygon(self, wkt_polygon, gdf):
        """
        Filters ships that fall inside the given WKT polygon.

        Parameters:
        - wkt_polygon (str): WKT string of the polygon area.
        - gdf (GeoDataFrame): GeoDataFrame containing ships with 'latitude' and 'longitude'.

        Returns:
        - Filtered GeoDataFrame with ships inside the polygon.
        """
        polygon = loads(wkt_polygon)  # Convert WKT string to Shapely Polygon
        gdf["geometry"] = gpd.points_from_xy(gdf.longitude, gdf.latitude)  # Convert lat/lon to points

        return gdf[gdf.geometry.within(polygon)]  # Filter points within the polygon

    def ship_map_with_speed(self, wkt_polygon, gdf):
        """
        Creates a folium map displaying ships inside the given WKT polygon, with markers colored based on ship speed.

        Parameters:
        - wkt_polygon (str): WKT string representing the polygon area.
        - gdf (GeoDataFrame): GeoDataFrame containing ship data with 'latitude', 'longitude', and 'speed' attributes.

        Returns:
        - folium.Map: A folium map object centered on the filtered ships, with markers indicating ship positions and speeds.
        """

        # Filter ships inside the polygon
        filtered_gdf = self.filter_ships_by_polygon(wkt_polygon, gdf)

        if filtered_gdf.empty:
            print("No ships found in the selected area.")
            return None

        # Create map centered around filtered ships
        m = folium.Map(
            location=[filtered_gdf.latitude.mean(), filtered_gdf.longitude.mean()],
            zoom_start=6
        )

        for _, row in filtered_gdf.iterrows():
            name, info_text = get_info(row)  # Keep the existing logic

            # Assign color based on speed
            speed = row["speed"]
            if speed <= 2:
                color = "green"
            elif speed <= 10:
                color = "blue"
            elif speed <= 25:
                color = "orange"
            elif speed <= 30:
                color = "red"
            else:
                color = "purple"

            # Add marker
            Marker(
                icon=Icon(color=color, icon="ship", prefix="fa"),
                location=[row.latitude, row.longitude],
                popup=Popup(info_text, max_width=300),
            ).add_to(m)

        # Add legend
        legend_html = create_speed_legend()
        m.get_root().html.add_child(Element(legend_html))

        return m

