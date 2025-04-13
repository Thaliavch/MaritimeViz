"""
Package's Visualization's Module
"""
import json
from sys import prefix

import folium
import leafmap.foliumap
import geopandas as gpd
import leafmap
from folium import Marker, Icon, Popup
from branca.element import Element
from shapely.wkt import loads
from folium.plugins import HeatMap
from IPython.display import display
from functools import partial
from ipyleaflet import Map, basemaps, basemap_to_tiles




from src.maritimeviz.utils.viz_utils import *



class Map:
    """
    A simple Map class to visualize AIS data using leafmap.
    """

    def __init__(self, center=[0, 0], zoom=2):
        """
        Create a leafmap Map instance.
        """
        self.m = leafmap.foliumap.Map(center=center, zoom=zoom)

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

    def map_all(self, geojson_data, layer_name="Route", map_tile="HYBRID"):
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
                m = leafmap.Map(location=[gdf.latitude.mean(), gdf.longitude.mean()], zoom_start=4)
                m.add_title("All Vessel Routes", font_size="20px", align="center")
                m.add_basemap(map_tile)


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
        m.add_title("Ships inside the polygon", font_size="20px", align="center")
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
        m.add_title("Ship Routes", font_size="20px", align="center")
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
        m.add_title("Ships Concentration", font_size="20px", align="center")
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
        m.add_title("Base Stations", font_size="20px", align="center")
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

    def ship_by_mmsi(self, geojson_data, mmsi=None, map_tile="HYBRID"):
        """
        Generate a map displaying the location and details of a ship identified by its MMSI.

        This function processes a GeoJSON dataset containing ship information, verifies its validity, and extracts
        the specific ship record corresponding to the provided Maritime Mobile Service Identity (MMSI) number.
        It then creates a map centered on the average latitude and longitude of the ship's data points, adds a title,
        applies a specified basemap tile, and plots additional information related to the ship.

        Args:
            geojson_data (dict or str): A valid GeoJSON dataset containing ship tracking information. This data must include
                fields such as "mmsi", "latitude", and "longitude" which are used to filter and position the ship on the map.
            mmsi (int, optional): The Maritime Mobile Service Identity number that uniquely identifies the ship to be plotted.
                If omitted (None), the function returns an error message. Defaults to None.
            map_tile (str, optional): The basemap style to be used for the map. Common options include "HYBRID", "SATELLITE", etc.
                Defaults to "HYBRID".

        Returns:
            folium.Map or str: Returns a folium map object with the ship's data plotted and visualized if the MMSI is found in
            the provided GeoJSON data. If any required input is missing or the ship is not found, one of the following error
            messages is returned:

                - 'No mmsi provided' : When the mmsi argument is None.
                - 'No geojson provided' : When the geojson_data argument is None.
                - 'No ship found with that mssi' : When the MMSI is not present in the GeoJSON dataset.

        Example:
             geojson_data = { ... }  # A valid GeoJSON dict containing ship data
             map_object = instance.ship_by_mmsi(geojson_data, mmsi=123456789, map_tile="SATELLITE")
             # The returned map_object can then be visualized or saved to an HTML file.
        """
        if mmsi is None:
            return 'No mmsi provided'

        if geojson_data is None:
            return 'No geojson provided'

        gdf = verify_geojson(geojson_data)

        if mmsi in gdf.mmsi.values:
            ship = gdf[gdf.mmsi == mmsi]
        else:
            return 'No ship found with that mssi'

        m = leafmap.foliumap.Map(
            location=[ship.latitude.mean(), ship.longitude.mean()],
            zoom_start=4
        )
        m.add_title("Map by MMSI", font_size="20px", align="center")
        m.add_basemap(map_tile)

        n = plot_with_info(ship, m)
        return n

    def ships_by_drawn_shape(self, geojson_data):
        """
        Set up an interactive map with drawing controls to update ship markers based on user-drawn polygons.
        This function is designed for use in environments such as Google Colab where interactive maps
        facilitate dynamic data exploration. When users draw a polygon on the map, a callback function is
        triggered to update the displayed ship markers based on the provided AIS GeoJSON data.

        Parameters:
            geojson_data (str or dict):
                The AIS data in GeoJSON format. This can either be the file path to a GeoJSON file or a
                dictionary containing GeoJSON data. The data should include details necessary for mapping,
                such as coordinates and ship identifiers.

        Returns:
            leafmap.Map:
                An interactive map object that includes drawing controls and a hybrid basemap. The map is
                configured to allow users to draw polygons, which in turn update the ship markers on the map
                based on the provided AIS data.

        Internal Details:
            - A leafmap Map object is instantiated with a default zoom level of 3 and the ipyleaflet interface enabled.
            - A hybrid basemap is added to the map for a comprehensive satellite and road overlay.
            - The function initializes a mutable state dictionary with keys:
                • "features": an empty list that will hold drawn GeoJSON features,
                • "ship_marker_layer": a placeholder for the layer containing ship markers,
                • "ship_polygon_layer": a placeholder for the layer representing drawn polygons.
            - The drawing control's callback is set up using `functools.partial` to bind the state, the map object,
              and the provided geojson_data to a handler function (`handle_draw`). This allows dynamic updates
              of ship markers whenever a new polygon is drawn.

        Example:
            geojson_data = "path/to/ais_data.geojson"  # or geojson_data can be a dict with your AIS data
            interactive_map = instance.ships_by_drawn_shape(geojson_data)
            interactive_map  # Display the map in a compatible environment like Colab
        """
        m = leafmap.Map(zoom=3, ipyleaflet=True)
        m.add_basemap("Hybrid")
        draw_control = m.draw_control

        # Create a mutable state dictionary for drawing features and layers.
        state = {
            "features": [],
            "ship_marker_layer": None,
            "ship_polygon_layer": None
        }

        # Use partial to bind state, map_obj, and geojson_data to the handle_draw callback.
        callback = partial(handle_draw, state, m, geojson_data)
        draw_control.on_draw(callback)

        return m

    def ship_with_speed(self, geojson_data, map_tile="HYBRID"):
        if geojson_data is None:
            return 'No geojson provided'

        gdf = verify_geojson(geojson_data)

        m = leafmap.foliumap.Map(location=[gdf.latitude.mean(), gdf.longitude.mean()], zoom_start=4)
        m.add_title("Map by Speed", font_size="20px", align="center")
        m.add_basemap(map_tile)

        legend_html = create_speed_legend()
        m.get_root().html.add_child(Element(legend_html))

        n = plot_with_info(gdf, m, speed_flag=True)
        return n


    def ship_map_on_click(self, geojson_data, radius_km=300):
        if geojson_data is None:
            return 'No geojson provided'
        gdf = verify_geojson(geojson_data)

        clicked_coords = []

        m = leafmap.Map(center=[0, 0], zoom=3, scroll_wheel_zoom=True)

        satellite = basemap_to_tiles(basemaps.Esri.WorldImagery)

        # Transparent labels (simulate hybrid)
        labels = basemap_to_tiles(basemaps.CartoDB.PositronOnlyLabels)

        m.add_layer(satellite)
        m.add_layer(labels)

        # Optional: customize basemap tiles if desired

        m.on_interaction(create_click_handler(radius_km, m, clicked_coords, gdf))
        display(m)
        return m
