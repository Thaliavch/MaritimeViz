"""
Package's Visualization's Module
"""
import json
from sys import prefix

import folium
import leafmap.foliumap
import geopandas as gpd
from folium import Marker, Icon, Popup
from branca.element import Element
from shapely.wkt import loads
from folium.plugins import HeatMap

from src.maritimeviz.utils.viz_utils import *

from MaritimeViz.src.maritimeviz.utils.viz_utils import verify_geojson


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

    def map_all(self, geojson_data, layer_name="Route"):
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

        gdf = verify_geojson(geojson_data)

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

                    icon = check_printable_icon(row) #Getting Icon

                    # Extract all available data dynamically
                    info_text = "<br>".join(
                        [f"{key}: {value}" for key, value in row.items() if value and key != "geometry"])
                        # testing

                    # Ensure latitude and longitude are valid
                    if row.geometry and hasattr(row.geometry, "x") and hasattr(row.geometry, "y"):
                        folium.Marker(
                            icon=folium.Icon(color="blue", icon=icon, prefix="fa"),
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
        try:
            polygon = loads(wkt_polygon)  # Convert WKT string to Shapely Polygon
        except Exception:
            raise ValueError("Invalid WKT polygon format")

        gdf["geometry"] = gpd.points_from_xy(gdf.longitude, gdf.latitude)  # Convert lat/lon to points

        return gdf[gdf.geometry.within(polygon)]  # Filter points within the polygon

    def ship_map_by_polygon(self, wkt_polygon, geojson_data, map_tile='HYBRID'):
        """
        Creates an interactive map displaying ships located **inside the given WKT polygon**,
        with markers colored based on their speed.

        Parameters:
        - wkt_polygon (str): WKT string representing the polygon area of interest.
        - gdf (GeoDataFrame): GeoDataFrame containing ship data, including 'latitude', 'longitude', and 'speed'.
        - map_tile (str, optional): The base map layer to use (e.g., 'HYBRID', 'ROADMAP'). Defaults to 'HYBRID'.

        Returns:
        - folium.Map object displaying:
        - Ships inside the polygon, color-coded by speed.
        - The polygon boundary.
        - A legend explaining speed color codes.
        - Returns None if no ships are found within the polygon.
        """

        # Verify and load GeoJSON data
        gdf = verify_geojson(geojson_data)

        # Filter ships inside the polygon
        filtered_gdf = self.filter_ships_by_polygon(wkt_polygon, gdf)

        if filtered_gdf.empty:
            print("No ships found in the selected area.")
            return None

        # Create map centered around filtered ships
        m = leafmap.foliumap.Map(
            location=[filtered_gdf.latitude.mean(), filtered_gdf.longitude.mean()],
            zoom_start=4
        )

        if map_tile is not None:
            m.add_basemap(map_tile)

        # Highlight the WKT polygon region
        polygon_geom = loads(wkt_polygon)
        polygon_coords = list(polygon_geom.exterior.coords)
        folium.Polygon(
            locations=[(lat, lon) for lon, lat in polygon_coords],
            color='yellow',
            weight=3,
            fill=True,
            fill_opacity=0.2,
            popup="WKT Region"
        ).add_to(m)

        for _, row in filtered_gdf.iterrows():

            info_text = get_info(row)  # Keep the existing logic

            icon = check_printable_icon(row) #Getting Icon

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
                icon=Icon(color=color, icon=icon, prefix="fa"),
                location=[row.latitude, row.longitude],
                popup=Popup(info_text, max_width=300),
            ).add_to(m)

        # Add legend
        legend_html = create_speed_legend()
        m.get_root().html.add_child(Element(legend_html))

        return m

    def ships_route(self, geojson_data, mmsi=None, map_tile='HYBRID'):
        """
        Generates a map showing the routes of ships identified by its MMSI, based on a GeoJSON file.
        If no MMSI is provided, it will display all routes. If a MMSI is provided, only show that specific route.

        Parameters:
        - geojson_data (str): Path to the GeoJSON file containing ship route data.
        - mmsi (int or str, optional): MMSI (Maritime Mobile Service Identity) of the ship to visualize.
        - map_tile (str, optional): Base map layer to use (e.g., 'HYBRID', 'ROADMAP'). Defaults to 'HYBRID'.

        Returns:
        - folium.Map object displaying:
        - The ship's route as a dashed polyline.
        - Markers for the first and last positions.
        - The selected base map.
        - str message if no ship is found with the given MMSI or if there are not enough data points to draw a route.
        """

        # Verify and load GeoJSON data
        gdf = verify_geojson(geojson_data)

        if mmsi is not None:
            if mmsi not in gdf.mmsi.values:
                return 'No ship found with that mmsi'
            gdf = gdf[gdf.mmsi == mmsi]

        if gdf.empty:
            return 'No data available to plot'

        if "timestamp" in gdf.columns:
            gdf = gdf.sort_values(by=["mmsi", "timestamp"])
        else:
            print("*WARNING*: No timestamp found. Sorting by index...")
            gdf = gdf.sort_values(by=["mmsi", gdf.index])

        m = leafmap.foliumap.Map(location=[gdf.latitude.mean(), gdf.longitude.mean()], zoom_start=6)
        if map_tile is not None:
            m.add_basemap(map_tile)

        for ship_id in gdf.mmsi.unique():
            ship = gdf[gdf.mmsi == ship_id]

            if len(ship) < 2:
                print(f"Skipping MMSI {ship_id}: Not enough data points.")
                continue

            first = ship.iloc[0]
            last = ship.iloc[-1]

            folium.Marker(
                location=[first.latitude, first.longitude],
                popup=f"MMSI {ship_id} - First Position",
                icon=folium.Icon(color="green", icon="play", prefix="fa"),
            ).add_to(m)

            folium.Marker(
                location=[last.latitude, last.longitude],
                popup=f"MMSI {ship_id} - Last Position",
                icon=folium.Icon(color="red", icon="stop", prefix="fa"),
            ).add_to(m)

            route_coords = ship[['latitude', 'longitude']].values.tolist()
            folium.PolyLine(
                locations=route_coords,
                color="yellow",
                weight=3,
                opacity=1,
                dash_array='5, 10'
            ).add_to(m)

        return m

    def plot_ship_heatmap(self, geojson_data, map_tile='HYBRID'):
        """
        Generates a heat map showing concentration of ships, based on a GeoJSON file.

        Parameters:
        - geojson_data (str): Path to the GeoJSON file containing ship route data.
        - map_tile (str, optional): Base map layer to use (e.g., 'HYBRID', 'ROADMAP'). Defaults to 'HYBRID'.

        Returns:
        - folium.Map object displaying:
        - Ships concentration by heatmap: heat
        """

        # Verify and load GeoJSON data
        gdf = verify_geojson(geojson_data)

        m = leafmap.foliumap.Map(location=[gdf.latitude.mean(), gdf.longitude.mean()], zoom_start=2)
        if map_tile:
            m.add_basemap(map_tile)

        heat_data = gdf[['latitude', 'longitude']].values.tolist()
        HeatMap(heat_data).add_to(m)
        return m

    # A plot specific for messages from type 4
    def plot_base_stations(self, geojson_data, tagblock_station=None, map_tile="HYBRID"):
        """
        Plots AIS base station messages on a Leafmap map.

        Parameters:
        - geojson_data (str): Path to the GeoJSON file.
        - tagblock_station (str, optional): Station ID to filter by. If None, shows all stations.
        - map_tile (str): Basemap style (e.g., 'ROADMAP', 'HYBRID').

        Returns:
        - leafmap.foliumap.Map: The generated map with base station markers.
        """

        # Verify and load GeoJSON data
        gdf = verify_geojson(geojson_data)


        if "tagblock_station" not in gdf.columns:
            print("No 'tagblock_station' field found in data.")
            return None

        # Filter by station if specified
        if tagblock_station:
            gdf = gdf[gdf.tagblock_station == tagblock_station]
            if gdf.empty:
                print(f"No data found for station: {tagblock_station}")
                return None
            print(f"Displaying only data for station: {tagblock_station}")
        else:
            print("Displaying all stations.")

        # Extract coordinates
        gdf["longitude"] = gdf.geometry.x
        gdf["latitude"] = gdf.geometry.y

        m = leafmap.foliumap.Map(
            location=[gdf.latitude.mean(), gdf.longitude.mean()],
            zoom_start=6
        )

        if map_tile:
            m.add_basemap(map_tile)

        for _, row in gdf.iterrows():
            icon = folium.Icon(color="red", icon=check_printable_icon(row), prefix="fa")

            popup_html = f"""
            <b>Station ID:</b> {row.get('tagblock_station', 'N/A')}<br>
            <b>MMSI:</b> {row.get('mmsi')}<br>
            <b>Message Type (ID):</b> {row.get('id')}<br>
            <b>Date/Time:</b> {row.get('datetime')}<br>
            <b>Received Stations:</b> {row.get('received_stations')}
            """

            folium.Marker(
                location=[row.latitude, row.longitude],
                popup=folium.Popup(popup_html, max_width=300),
                icon=icon,
                tooltip="Base Station"
            ).add_to(m)

        return m


    def ship_by_mmsi(self, geojson_data, mmsi = None, map_tile="HYBRID"):
        if mmsi is None:
            return 'No mmsi provided'

        if geojson_data is None:
            return 'No geojson provided'

        gdf = verify_geojson(geojson_data)

        if mmsi in gdf.mmsi.values:
            ship = gdf[gdf.mmsi == mmsi]

        else:
            return 'No ship found with that mssi'

        m = leafmap.foliumap.Map(location=[ship.latitude.mean(), ship.longitude.mean()], zoom_start=4)
        m.add_title("Map by MMSI", font_size="20px", align="center")
        m.add_basemap(map_tile)

        plot_with_info(ship, m)
