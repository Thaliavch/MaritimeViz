"""
Package's Visualization's Module
"""
import json
from sys import prefix

import folium
import leafmap.foliumap
import geopandas as gpd
import leafmap
from folium import Marker, Icon, Popup, FeatureGroup, LayerControl
from branca.element import Element
from shapely.wkt import loads
from folium.plugins import HeatMap
from IPython.display import display
from functools import partial
from ipyleaflet import Map, basemaps, basemap_to_tiles

from .utils.viz_utils import *

def ships_by_drawn_shape(geojson_data):
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

    m

def ship_map_on_click(geojson_data, radius_km=300):
    """
    Create an interactive map with a clickable interface to display ship data within a specified radius.

    This function sets up an interactive map using the leafmap library. The map is initialized at a global view
    (centered at [0, 0] with a zoom level of 3) and enhanced with two basemap layers: a satellite imagery layer and
    a transparent label layer to simulate a hybrid view. When a user interacts with the map (for example, by clicking
    on it), a custom click handler is triggered. This handler uses the provided GeoJSON ship data to display or update
    ship information within the specified radius (in kilometers) around the clicked location.

    Parameters:
        geojson_data (dict or str):
            A valid GeoJSON dataset or file path containing ship location data. This data is verified using the
            verify_geojson() function and is utilized to filter and display ships based on their geographical positions.
        radius_km (int or float, optional):
            The radius (in kilometers) around a clicked point within which ship data should be filtered and shown.
            The default value is 300 km.

    Returns:
        leafmap.Map or str:
            An interactive map object with the configured layers and click interaction enabled. If the geojson_data
            is None, the function returns the string 'No geojson provided'.

    Internal Workflow:
        - Checks if geojson_data is provided; if not, returns an error message.
        - Verifies the GeoJSON data using verify_geojson().
        - Initializes an empty list to store coordinates from the user's click interactions.
        - Creates a global map object centered at [0, 0] with scroll wheel zoom enabled.
        - Converts basemap tiles for satellite imagery (using Esri WorldImagery) and for transparent labels
            (using CartoDB PositronOnlyLabels) to simulate a hybrid basemap view.
        - Adds both the satellite and label layers to the map.
        - Sets up an interactive event listener using the create_click_handler() callback, binding the radius,
            map object, clicked coordinates list, and the verified GeoJSON data. This handler processes click events
            on the map to update the ship data shown based on the user's input.
        - Displays the map immediately in the current environment.

    Example:
            geojson_data = { ... }  # Provide valid GeoJSON data containing ship location information.
            interactive_map = instance.ship_map_on_click(geojson_data, radius_km=300)
            interactive_map  # This will display the interactive clickable map in a compatible environment.
    """
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

    m


def add_speed_layer(m: leafmap.foliumap.Map, geojson_data) -> leafmap.foliumap.Map:
    """
    Add a ship‐with‐speed layer (and legend) to an existing map.

    Parameters:
    -----------
    m : leafmap.foliumap.Map
        Your map instance (e.g. `m = leafmap.foliumap.Map(...)` or from your Map.m).
    geojson_data : str | dict
        Either a file path or a dict of your AIS GeoJSON.

    Returns:
    --------
    m : leafmap.foliumap.Map
        The same map, now with speed‐colored markers and a legend injected.
    """
    if geojson_data is None:
        print("No geojson provided; skipping speed layer.")
        return m

    gdf = verify_geojson(geojson_data)
    if gdf.empty:
        print("GeoJSON had no valid features; skipping speed layer.")
        return m

    # stick speed‐legend into the map’s HTML
    legend_html = create_speed_legend()
    m.get_root().html.add_child(Element(legend_html))

    # drop in your plotted markers & one LayerControl
    plot_with_info(gdf, m, speed_flag=True)

    return m


class Map:
    """
    A simple Map class to visualize AIS data using leafmap.
    """

    def __init__(self, center=(0, 0), zoom=2):
        # This is a folium-based map
        self.m = leafmap.foliumap.Map(center=center, zoom=zoom)
        self.m.add_basemap(map_tile="HYBRID")
        self._layer_control_added = False

    def _repr_html_(self):
        # Jupyter will call this to get the HTML to render
        return self.m._repr_html_()

    def _maybe_add_layer_control(self):
        if not self._layer_control_added:
            # Add a Folium LayerControl exactly once
            LayerControl().add_to(self.m)
            self._layer_control_added = True
            print("layer control set to ", self._layer_control_added)

    def map_all(self, geojson_data, layer_name="All Vessel Routes"):
        """
        Adds a toggleable layer of all vessel positions/routes to the map.
        """
        gdf = verify_geojson(geojson_data)
        if gdf.empty:
            print("No valid data.")
            return self

        # ensure lat/lon
        if "latitude" not in gdf or "longitude" not in gdf:
            gdf["longitude"] = gdf.geometry.x
            gdf["latitude"] = gdf.geometry.y

        fg = FeatureGroup(name=layer_name, show=True)
        for _, row in gdf.iterrows():
            # skip if no valid geometry
            if row.geometry is None:
                continue
            lon, lat = row.geometry.x, row.geometry.y

            icon_name = check_printable_icon(row)
            info = "<br>".join(
                f"{k}: {v}"
                for k, v in row.items()
                if v is not None and k not in ("geometry","latitude","longitude")
            )

            folium.Marker(
                location=[lat, lon],
                icon=folium.Icon(color="blue", icon=icon_name, prefix="fa"),
                popup=folium.Popup(info, max_width=300),
                tooltip="Press for more info"
            ).add_to(fg)

        fg.add_to(self.m)
        self._maybe_add_control()
        return self
    
    def ship_map_by_polygon(self, wkt_polygon, geojson_data, layer_name="Ships in Polygon"):
        """Adds a layer showing only ships within a WKT polygon, color‐coded by speed."""
        gdf = verify_geojson(geojson_data)
        # spatial filter
        try:
            poly = loads(wkt_polygon)
        except Exception:
            raise ValueError("Invalid WKT polygon format")
        gdf["geometry"] = gpd.points_from_xy(gdf.longitude, gdf.latitude, crs="EPSG:4326")
        filtered = gdf[gdf.geometry.within(poly)]
        if filtered.empty:
            print("No ships found in the selected area.")
            return self

        fg = FeatureGroup(name=layer_name, show=True)
        # draw polygon
        coords = [(lat, lon) for lon, lat in poly.exterior.coords]
        Polygon(locations=coords, color="yellow", weight=3, fill=True, fill_opacity=0.2).add_to(fg)

        for _, row in filtered.iterrows():
            # speed‐based color
            speed = row.get("speed", 0)
            if speed <= 2:
                col = "green"
            elif speed <= 10:
                col = "blue"
            elif speed <= 25:
                col = "orange"
            elif speed <= 30:
                col = "red"
            else:
                col = "purple"

            info = get_info(row)
            icon_name = check_printable_icon(row)
            # skip bad geometry
            if not row.geometry or not hasattr(row.geometry, "x") or not hasattr(row.geometry, "y"):
                continue

            Marker(
                location=[row.geometry.y, row.geometry.x],
                icon=Icon(color=col, icon=icon_name, prefix="fa"),
                popup=Popup(info, max_width=300)
            ).add_to(fg)

        # add speed legend
        fg.add_to(self.m)
        legend = create_speed_legend()
        self.m.get_root().html.add_child(Element(legend))

        self._maybe_add_layer_control()
        return self

    def ships_route(self, geojson_data, mmsi=None, layer_name="Ship Routes"):
        """Adds dashed‐line routes (and start/stop markers) as a separate layer."""
        gdf = verify_geojson(geojson_data)
        if mmsi is not None:
            if mmsi not in gdf.mmsi.values:
                print(f"No ship found with MMSI {mmsi}")
                return self
            gdf = gdf[gdf.mmsi == mmsi]

        if gdf.empty:
            print("No data available to plot.")
            return self

        # sort by timestamp if available
        if "timestamp" in gdf.columns:
            gdf = gdf.sort_values(by=["mmsi", "timestamp"])
        else:
            gdf = gdf.sort_values(by=["mmsi", gdf.index])

        fg = FeatureGroup(name=layer_name, show=True)
        for ship_id in gdf.mmsi.unique():
            ship = gdf[gdf.mmsi == ship_id]
            if len(ship) < 2:
                continue
            first, last = ship.iloc[0], ship.iloc[-1]
            # markers
            Marker(
                location=[first.latitude, first.longitude],
                icon=Icon(color="green", icon="play", prefix="fa"),
                popup=f"MMSI {ship_id} - First"
            ).add_to(fg)
            Marker(
                location=[last.latitude, last.longitude],
                icon=Icon(color="red", icon="stop", prefix="fa"),
                popup=f"MMSI {ship_id} - Last"
            ).add_to(fg)
            # route polyline
            coords = ship[["latitude", "longitude"]].values.tolist()
            folium.PolyLine(locations=coords, color="yellow", weight=3,
                            dash_array="5,10", opacity=1).add_to(fg)

        fg.add_to(self.m)
        self._maybe_add_layer_control()
        return self

    def plot_ship_heatmap(self, geojson_data, layer_name="Heatmap"):
        """Adds a heat‐map layer of ship concentrations."""
        gdf = verify_geojson(geojson_data)
        if gdf.empty:
            print("No data for heatmap.")
            return self

        # ensure lat/lon
        if "latitude" not in gdf.columns or "longitude" not in gdf.columns:
            gdf["longitude"] = gdf.geometry.x
            gdf["latitude"] = gdf.geometry.y

        fg = FeatureGroup(name=layer_name, show=True)
        heat_data = gdf[["latitude", "longitude"]].values.tolist()
        HeatMap(heat_data).add_to(fg)

        fg.add_to(self.m)
        self._maybe_add_layer_control()
        return self

    def plot_base_stations(self, geojson_data, tagblock_station=None, layer_name="Base Stations"):
        """Adds base‐station markers as a toggleable layer."""
        gdf = verify_geojson(geojson_data)
        if "tagblock_station" not in gdf.columns:
            print("No 'tagblock_station' field found.")
            return self

        if tagblock_station:
            gdf = gdf[gdf.tagblock_station == tagblock_station]
            if gdf.empty:
                print(f"No data for station {tagblock_station}.")
                return self

        # ensure lat/lon
        gdf["longitude"] = gdf.geometry.x
        gdf["latitude"] = gdf.geometry.y

        fg = FeatureGroup(name=layer_name, show=True)
        for _, row in gdf.iterrows():
            if not row.geometry or not hasattr(row.geometry, "x"):
                continue
            icon_name = check_printable_icon(row)
            popup = (
                f"<b>Station:</b> {row.tagblock_station}<br>"
                f"<b>MMSI:</b> {row.mmsi}<br>"
                f"<b>Date/Time:</b> {row.datetime}<br>"
                f"<b>Received:</b> {row.received_stations}"
            )
            Marker(
                location=[row.latitude, row.longitude],
                icon=Icon(color="red", icon=icon_name, prefix="fa"),
                popup=Popup(popup, max_width=300)
            ).add_to(fg)

        fg.add_to(self.m)
        self._maybe_add_layer_control()
        return self

    def ship_by_mmsi(self, geojson_data, mmsi, layer_name=None):
        """Adds just one ship’s track as its own layer of points."""
        gdf = verify_geojson(geojson_data)
        if mmsi is None or geojson_data is None:
            print("Must supply both geojson_data and mmsi.")
            return self
        if mmsi not in gdf.mmsi.values:
            print(f"No ship found with MMSI {mmsi}")
            return self

        subset = gdf[gdf.mmsi == mmsi]
        fg = FeatureGroup(name=layer_name or f"Ship {mmsi}", show=True)
        for _, row in subset.iterrows():
            if not row.geometry or not hasattr(row.geometry, "x"):
                continue
            info = get_info(row)
            icon_name = check_printable_icon(row)
            Marker(
                location=[row.geometry.y, row.geometry.x],
                icon=Icon(color="blue", icon=icon_name, prefix="fa"),
                popup=Popup(info, max_width=300)
            ).add_to(fg)

        fg.add_to(self.m)
        self._maybe_add_layer_control()
        return self

    #     if geojson_data is None:
    #         return 'No geojson provided'
    #
    #     gdf = verify_geojson(geojson_data)
    #
    #     legend_html = create_speed_legend()
    #     self.m.get_root().html.add_child(Element(legend_html))
    #
    #     m = plot_with_info(gdf, self.m, speed_flag=True)
    #
    #     display(m)

    # def ship_map_by_polygon(self, wkt_polygon, geojson_data):
    #     """
    #     Create an interactive map to visualize ships located within a user-defined WKT polygon.
    #
    #     This function generates a folium-based map (via the leafmap wrapper) that highlights ships found within a specific
    #     polygonal area. The ships are color-coded by speed and presented with informative markers. The polygon itself is also
    #     drawn on the map to provide spatial context. This visualization is useful for analyzing maritime traffic density, behavior
    #     patterns, or area-specific vessel presence.
    #
    #     Parameters:
    #         wkt_polygon (str):
    #             A Well-Known Text (WKT) string defining the polygonal boundary for spatial filtering.
    #             Only ships located inside this polygon will be visualized.
    #
    #         geojson_data (dict or str):
    #             GeoJSON ship data, either as a Python dictionary or as a file path. It is verified and
    #             converted into a GeoDataFrame using the verify_geojson() utility function.
    #             The data must include at least the following fields: 'latitude', 'longitude', and 'speed'.
    #
    #         map_tile (str, optional):
    #             The base map tile to use for visualization. Defaults to 'HYBRID', but can also accept
    #             other supported tiles like 'ROADMAP', etc.
    #
    #     Returns:
    #         folium.Map or None:
    #             A map object displaying the filtered ships within the WKT polygon area, along with:
    #             - Speed-based color-coded markers
    #             - The polygon boundary as a highlighted region
    #             - A custom legend explaining the speed-color mapping
    #
    #             If no ships are found within the polygon, a message is printed and None is returned.
    #
    #     Internal Workflow:
    #         - The GeoJSON input is verified and converted to a GeoDataFrame.
    #         - Ships are spatially filtered based on their inclusion within the provided WKT polygon.
    #         - If no ships are found, the function exits early.
    #         - A map is initialized and centered around the centroid of the filtered data.
    #         - The polygon boundary is added to the map in yellow with transparency for visual emphasis.
    #         - Each ship is plotted as a marker, color-coded by its speed:
    #             Green (≤2 knots), Blue (≤10), Orange (≤25), Red (≤30), Purple (>30)
    #         - Each marker includes an icon and a popup with detailed ship information.
    #         - A speed legend is added to the map to support interpretation.
    #
    #     Example:
    #         geojson_data = "ships_data.geojson"
    #         wkt_polygon = "POLYGON((-81 25, -81 26, -80 26, -80 25, -81 25))"
    #         m = instance.ship_map_by_polygon(wkt_polygon, geojson_data)
    #         m  # Displays the interactive ship map with the selected polygon filter
    #     """
    #
    #
    #     # Verify and load GeoJSON data
    #     gdf = verify_geojson(geojson_data)
    #
    #     # Filter ships inside the polygon
    #     filtered_gdf = filter_ships_by_polygon(wkt_polygon, gdf)
    #
    #     if filtered_gdf.empty:
    #         print("No ships found in the selected area.")
    #         return None
    #
    #     # Highlight the WKT polygon region
    #     polygon_geom = loads(wkt_polygon)
    #     polygon_coords = list(polygon_geom.exterior.coords)
    #     folium.Polygon(
    #         locations=[(lat, lon) for lon, lat in polygon_coords],
    #         color='yellow',
    #         weight=3,
    #         fill=True,
    #         fill_opacity=0.2,
    #         popup="WKT Region"
    #     ).add_to(self.m)
    #
    #     for _, row in filtered_gdf.iterrows():
    #
    #         info_text = get_info(row)  # Keep the existing logic
    #
    #         icon = check_printable_icon(row) #Getting Icon
    #
    #         # Assign color based on speed
    #         speed = row["speed"]
    #         if speed <= 2:
    #             color = "green"
    #         elif speed <= 10:
    #             color = "blue"
    #         elif speed <= 25:
    #             color = "orange"
    #         elif speed <= 30:
    #             color = "red"
    #         else:
    #             color = "purple"
    #
    #         # Add marker
    #         folium.Marker(
    #             icon=Icon(color=color, icon=icon, prefix="fa"),
    #             location=[row.latitude, row.longitude],
    #             popup=Popup(info_text, max_width=300),
    #         ).add_to(self.m)
    #
    #     # Add legend
    #     legend_html = create_speed_legend()
    #     self.m.get_root().html.add_child(Element(legend_html))
    #
    #     display(self.m)
    #
    # def ships_route(self, geojson_data, mmsi=None):
    #     """
    #     Generate an interactive map to visualize ship routes from GeoJSON data.
    #
    #     This function uses folium (via leafmap) to create a map showing the trajectory of one or more ships
    #     based on their MMSI (Maritime Mobile Service Identity) and position data. If an MMSI is provided,
    #     only that ship’s route is displayed. Otherwise, routes for all ships in the dataset are shown.
    #
    #     The map includes:
    #     - Dashed yellow polylines representing the ship routes.
    #     - A green marker for each ship's starting point.
    #     - A red marker for each ship's final known position.
    #     - A selectable base map tile layer (e.g., 'HYBRID').
    #
    #     Parameters:
    #         geojson_data (str or dict):
    #             Path to or dictionary of a valid GeoJSON file containing ship position data.
    #             The GeoJSON must include 'latitude', 'longitude', and 'mmsi' fields.
    #
    #         mmsi (int or str, optional):
    #             The MMSI of the specific ship to visualize. If omitted, all ships in the data are plotted.
    #
    #         map_tile (str, optional):
    #             The base map style to apply. Default is 'HYBRID'. Can be other valid basemap options supported by leafmap.
    #
    #     Returns:
    #         folium.Map or str:
    #             - A folium.Map object displaying the ship route(s) and key positions.
    #             - A message string if no ship with the specified MMSI is found or if the dataset is empty.
    #
    #     Workflow:
    #         - Load and validate GeoJSON data using `verify_geojson()`.
    #         - Filter by MMSI if provided.
    #         - Sort ship positions by timestamp (if available).
    #         - For each ship with at least 2 points:
    #             - Place start (green) and end (red) markers.
    #             - Draw a dashed polyline showing the path.
    #         - Return the map with all layers and markers.
    #
    #     Example:
    #         m = instance.ships_route("ships.geojson", mmsi=123456789)
    #         m  # Displays an interactive route map in Jupyter or Streamlit
    #     """
    #
    #     # Verify and load GeoJSON data
    #     gdf = verify_geojson(geojson_data)
    #
    #     if mmsi is not None:
    #         if mmsi not in gdf.mmsi.values:
    #             return 'No ship found with that mmsi'
    #         gdf = gdf[gdf.mmsi == mmsi]
    #
    #     if gdf.empty:
    #         return 'No data available to plot'
    #
    #     if "timestamp" in gdf.columns:
    #         gdf = gdf.sort_values(by=["mmsi", "timestamp"])
    #     else:
    #         print("*WARNING*: No timestamp found. Sorting by index...")
    #         gdf = gdf.sort_values(by=["mmsi", gdf.index])
    #
    #     for ship_id in gdf.mmsi.unique():
    #         ship = gdf[gdf.mmsi == ship_id]
    #
    #         if len(ship) < 2:
    #             print(f"Skipping MMSI {ship_id}: Not enough data points.")
    #             continue
    #
    #         first = ship.iloc[0]
    #         last = ship.iloc[-1]
    #
    #         folium.Marker(
    #             location=[first.latitude, first.longitude],
    #             popup=f"MMSI {ship_id} - First Position",
    #             icon=folium.Icon(color="green", icon="play", prefix="fa"),
    #         ).add_to(self.m)
    #
    #         folium.Marker(
    #             location=[last.latitude, last.longitude],
    #             popup=f"MMSI {ship_id} - Last Position",
    #             icon=folium.Icon(color="red", icon="stop", prefix="fa"),
    #         ).add_to(self.m)
    #
    #         route_coords = ship[['latitude', 'longitude']].values.tolist()
    #         folium.PolyLine(
    #             locations=route_coords,
    #             color="yellow",
    #             weight=3,
    #             opacity=1,
    #             dash_array='5, 10'
    #         ).add_to(self.m)
    #
    #     display(self.m)
    #
    # def plot_ship_heatmap(self, geojson_data):
    #     """
    #     Generates a heat map showing concentration of ships, based on a GeoJSON file.
    #
    #     Parameters:
    #     - geojson_data (str): Path to the GeoJSON file containing ship route data.
    #     - map_tile (str, optional): Base map layer to use (e.g., 'HYBRID', 'ROADMAP'). Defaults to 'HYBRID'.
    #
    #     Returns:
    #     - folium.Map object displaying:
    #     - Ships concentration by heatmap: heat
    #     """
    #
    #     # Verify and load GeoJSON data
    #     gdf = verify_geojson(geojson_data)
    #
    #     heat_data = gdf[['latitude', 'longitude']].values.tolist()
    #     HeatMap(heat_data).add_to(self.m)
    #
    #     display(self.m)
    #
    # # A plot specific for messages from type 4
    # def plot_base_stations(self, geojson_data, tagblock_station=None):
    #     """
    #     Plots AIS base station messages on a Leafmap map.
    #
    #     Parameters:
    #     - geojson_data (str): Path to the GeoJSON file.
    #     - tagblock_station (str, optional): Station ID to filter by. If None, shows all stations.
    #     - map_tile (str): Basemap style (e.g., 'ROADMAP', 'HYBRID').
    #
    #     Returns:
    #     - leafmap.foliumap.Map: The generated map with base station markers.
    #     """
    #
    #     # Verify and load GeoJSON data
    #     gdf = verify_geojson(geojson_data)
    #
    #
    #     if "tagblock_station" not in gdf.columns:
    #         print("No 'tagblock_station' field found in data.")
    #         return None
    #
    #     # Filter by station if specified
    #     if tagblock_station:
    #         gdf = gdf[gdf.tagblock_station == tagblock_station]
    #         if gdf.empty:
    #             print(f"No data found for station: {tagblock_station}")
    #             return None
    #         print(f"Displaying only data for station: {tagblock_station}")
    #     else:
    #         print("Displaying all stations.")
    #
    #     # Extract coordinates
    #     gdf["longitude"] = gdf.geometry.x
    #     gdf["latitude"] = gdf.geometry.y
    #
    #     for _, row in gdf.iterrows():
    #         icon = folium.Icon(color="red", icon=check_printable_icon(row), prefix="fa")
    #
    #         popup_html = f"""
    #         <b>Station ID:</b> {row.get('tagblock_station', 'N/A')}<br>
    #         <b>MMSI:</b> {row.get('mmsi')}<br>
    #         <b>Message Type (ID):</b> {row.get('id')}<br>
    #         <b>Date/Time:</b> {row.get('datetime')}<br>
    #         <b>Received Stations:</b> {row.get('received_stations')}
    #         """
    #
    #         folium.Marker(
    #             location=[row.latitude, row.longitude],
    #             popup=folium.Popup(popup_html, max_width=300),
    #             icon=icon,
    #             tooltip="Base Station"
    #         ).add_to(self.m)
    #
    #     display(self.m)
    #
    # def ship_by_mmsi(self, geojson_data, mmsi=None):
    #     """
    #     Generate a map displaying the location and details of a ship identified by its MMSI.
    #
    #     This function processes a GeoJSON dataset containing ship information, verifies its validity, and extracts
    #     the specific ship record corresponding to the provided Maritime Mobile Service Identity (MMSI) number.
    #     It then creates a map centered on the average latitude and longitude of the ship's data points, adds a title,
    #     applies a specified basemap tile, and plots additional information related to the ship.
    #
    #     Args:
    #         geojson_data (dict or str): A valid GeoJSON dataset containing ship tracking information. This data must include
    #             fields such as "mmsi", "latitude", and "longitude" which are used to filter and position the ship on the map.
    #         mmsi (int, optional): The Maritime Mobile Service Identity number that uniquely identifies the ship to be plotted.
    #             If omitted (None), the function returns an error message. Defaults to None.
    #         map_tile (str, optional): The basemap style to be used for the map. Common options include "HYBRID", "SATELLITE", etc.
    #             Defaults to "HYBRID".
    #
    #     Returns:
    #         folium.Map or str: Returns a folium map object with the ship's data plotted and visualized if the MMSI is found in
    #         the provided GeoJSON data. If any required input is missing or the ship is not found, one of the following error
    #         messages is returned:
    #
    #             - 'No mmsi provided' : When the mmsi argument is None.
    #             - 'No geojson provided' : When the geojson_data argument is None.
    #             - 'No ship found with that mssi' : When the MMSI is not present in the GeoJSON dataset.
    #
    #     Example:
    #          geojson_data = { ... }  # A valid GeoJSON dict containing ship data
    #          map_object = instance.ship_by_mmsi(geojson_data, mmsi=123456789, map_tile="SATELLITE")
    #          # The returned map_object can then be visualized or saved to an HTML file.
    #     """
    #     if mmsi is None:
    #         return 'No mmsi provided'
    #
    #     if geojson_data is None:
    #         return 'No geojson provided'
    #
    #     gdf = verify_geojson(geojson_data)
    #
    #     if mmsi in gdf.mmsi.values:
    #         ship = gdf[gdf.mmsi == mmsi]
    #     else:
    #         return 'No ship found with that mssi'
    #
    #     m = plot_with_info(ship, self.m)
    #
    #     display(m)
    #
    #

