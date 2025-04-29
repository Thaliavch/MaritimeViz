"""
Package's Visualization Module
"""

import folium
import geopandas as gpd
import leafmap.foliumap
from branca.element import Element
from folium import Icon, Popup, FeatureGroup, LayerControl
from folium.plugins import HeatMap
from shapely.wkt import loads

from .utils.viz_utils import (
    verify_geojson,
    check_printable_icon,
    get_info,
    create_speed_legend,
)


class Map:
    """
    A class to visualize AIS ship data through interactive maps using Leafmap and Folium.

    Provides multiple visualization options, including ship routes, heatmaps, base stations,
    and ships filtered by drawn polygons.
    """

    def __init__(self, center=(0, 0), zoom=2):
        self.m = leafmap.foliumap.Map(center=center, zoom=zoom)
        self.m.add_basemap(map_tile="HYBRID")
        self._layer_control_added = False

    def _repr_html_(self):
        return self.m._repr_html_()

    def add_layer_control(self):
        if not self._layer_control_added:
            LayerControl().add_to(self.m)
            self._layer_control_added = True

    def map_all(self, geojson_data, layer_name="All Vessel Routes"):
        """
        Add a toggleable marker layer showing all vessel positions on the map.

        Parameters:
            geojson_data (str|dict): GeoJSON vessel data.
            layer_name (str): Name of the layer for display.

        Returns:
            Map: Self, for chaining.
        """
        gdf = verify_geojson(geojson_data)
        if gdf.empty:
            print("No valid data.")
            return self

        if "latitude" not in gdf.columns or "longitude" not in gdf.columns:
            gdf["longitude"] = gdf.geometry.x
            gdf["latitude"] = gdf.geometry.y

        fg = FeatureGroup(name=layer_name, show=True)
        for _, row in gdf.iterrows():
            if row.geometry is None:
                continue
            lon, lat = row.geometry.x, row.geometry.y

            icon_name = check_printable_icon(row)
            info = "<br>".join(
                f"{k}: {v}"
                for k, v in row.items()
                if v is not None and k not in ("geometry", "latitude", "longitude")
            )

            folium.Marker(
                location=[lat, lon],
                icon=Icon(color="blue", icon=icon_name, prefix="fa"),
                popup=Popup(info, max_width=300),
                tooltip="Press for more info",
            ).add_to(fg)

        fg.add_to(self.m)
        return self

    def ship_map_by_polygon(
        self, wkt_polygon, geojson_data, layer_name="Ships in Polygon"
    ):
        """
        Visualize ships located inside a user-defined WKT polygon.

        Ships are filtered spatially and color-coded based on speed.
        If no ships are inside the polygon, a message is printed, and the map is still returned.

        Parameters:
            wkt_polygon (str): WKT format polygon.
            geojson_data (str|dict): Ship data.
            layer_name (str): Display name for the layer.

        Returns:
            Map: Self.
        """
        gdf = verify_geojson(geojson_data)
        try:
            poly = loads(wkt_polygon)
        except Exception:
            raise ValueError("Invalid WKT polygon format")

        gdf["geometry"] = gpd.points_from_xy(
            gdf.longitude, gdf.latitude, crs="EPSG:4326"
        )
        filtered = gdf[gdf.geometry.within(poly)]

        fg = FeatureGroup(name=layer_name, show=True)
        coords = [(lat, lon) for lon, lat in poly.exterior.coords]
        folium.Polygon(
            locations=coords, color="yellow", weight=3, fill=True, fill_opacity=0.2
        ).add_to(fg)

        for _, row in filtered.iterrows():
            if row.geometry is None:
                continue

            speed = row.get("speed", 0)
            color = (
                "green"
                if speed <= 2
                else "blue"
                if speed <= 10
                else "orange"
                if speed <= 25
                else "red"
                if speed <= 30
                else "purple"
            )

            info = get_info(row)
            icon_name = check_printable_icon(row)

            folium.Marker(
                location=[row.geometry.y, row.geometry.x],
                icon=Icon(color=color, icon=icon_name, prefix="fa"),
                popup=Popup(info, max_width=300),
            ).add_to(fg)

        legend = create_speed_legend()
        self.m.get_root().html.add_child(Element(legend))
        fg.add_to(self.m)
        return self

    def ships_route(self, geojson_data, mmsi=None, layer_name="Ship Routes"):
        """
        Visualize ship routes with dashed lines and starting/ending point markers.

        Parameters:
            geojson_data (str|dict): AIS data.
            mmsi (int or str, optional): Specific ship MMSI.
            layer_name (str): Name for the layer.

        Returns:
            Map: Self.
        """
        gdf = verify_geojson(geojson_data)
        if mmsi is not None:
            if mmsi not in gdf.mmsi.values:
                return "No ship found with that mmsi"
            gdf = gdf[gdf.mmsi == mmsi]

        if gdf.empty:
            print("No data available to plot.")
            return self

        if "timestamp" in gdf.columns:
            gdf = gdf.sort_values(by=["mmsi", "timestamp"])

        fg = FeatureGroup(name=layer_name, show=True)
        for ship_id in gdf.mmsi.unique():
            ship = gdf[gdf.mmsi == ship_id]
            if len(ship) < 2:
                continue

            first, last = ship.iloc[0], ship.iloc[-1]
            folium.Marker(
                location=[first.latitude, first.longitude],
                icon=Icon(color="green", icon="play", prefix="fa"),
                popup=Popup(f"MMSI {ship_id} - First"),
            ).add_to(fg)
            folium.Marker(
                location=[last.latitude, last.longitude],
                icon=Icon(color="red", icon="stop", prefix="fa"),
                popup=Popup(f"MMSI {ship_id} - Last"),
            ).add_to(fg)

            coords = ship[["latitude", "longitude"]].values.tolist()
            folium.PolyLine(
                locations=coords, color="yellow", weight=3, dash_array="5,10", opacity=1
            ).add_to(fg)

        fg.add_to(self.m)
        return self

    def plot_ship_heatmap(self, geojson_data, layer_name="Heatmap"):
        """
        Generate a heatmap showing concentration of ships based on GeoJSON data.

        Parameters:
            geojson_data (str|dict): Ship location data.
            layer_name (str): Name for the heatmap layer.

        Returns:
            Map: Self.
        """
        gdf = verify_geojson(geojson_data)
        if gdf.empty:
            print("No data for heatmap.")
            return self

        fg = FeatureGroup(name=layer_name, show=True)
        heat_data = gdf[["latitude", "longitude"]].values.tolist()
        HeatMap(heat_data).add_to(fg)

        fg.add_to(self.m)
        return self

    def plot_base_stations(
        self, geojson_data, tagblock_station=None, layer_name="Base Stations"
    ):
        """
        Plot AIS base station messages on a map, optionally filtering by station ID.

        Parameters:
            geojson_data (str|dict): Base station data.
            tagblock_station (str, optional): Specific station ID to filter.
            layer_name (str): Name for the layer.

        Returns:
            Map: Self.
        """
        gdf = verify_geojson(geojson_data)
        if "tagblock_station" not in gdf.columns:
            print("No 'tagblock_station' field found.")
            return self

        if tagblock_station:
            gdf = gdf[gdf.tagblock_station == tagblock_station]

        fg = FeatureGroup(name=layer_name, show=True)
        for _, row in gdf.iterrows():
            if row.geometry is None:
                continue
            icon_name = check_printable_icon(row)

            lat = row.latitude if "latitude" in row else row.geometry.y
            lon = row.longitude if "longitude" in row else row.geometry.x

            popup = (
                f"<b>Station:</b> {row.get('tagblock_station', 'N/A')}<br>"
                f"<b>MMSI:</b> {row.get('mmsi', 'N/A')}<br>"
                f"<b>Date/Time:</b> {row.get('datetime', 'N/A')}<br>"
                f"<b>Received:</b> {row.get('received_stations', 'N/A')}"
            )
            folium.Marker(
                location=[lat, lon],
                icon=folium.Icon(color="red", icon=icon_name, prefix="fa"),
                popup=folium.Popup(popup, max_width=300),
            ).add_to(fg)

        fg.add_to(self.m)
        return self

    def ship_by_mmsi(self, geojson_data, mmsi, layer_name=None):
        """
        Display the location and information of a ship identified by its MMSI number.

        If the MMSI is not found, a warning is printed and the current map remains unchanged.

        Parameters:
            geojson_data (str|dict): Ship tracking data.
            mmsi (int): Target ship's MMSI.
            layer_name (str, optional): Custom name for the map layer.

        Returns:
            Map: Self.
        """
        gdf = verify_geojson(geojson_data)
        if mmsi not in gdf.mmsi.values:
            print(f"No ship found with MMSI {mmsi}")
            return self

        subset = gdf[gdf.mmsi == mmsi]

        fg = FeatureGroup(name=layer_name or f"Ship {mmsi}", show=True)
        for _, row in subset.iterrows():
            if row.geometry is None:
                continue
            info = get_info(row)
            icon_name = check_printable_icon(row)
            folium.Marker(
                location=[row.geometry.y, row.geometry.x],
                icon=Icon(color="blue", icon=icon_name, prefix="fa"),
                popup=Popup(info, max_width=300),
            ).add_to(fg)

        fg.add_to(self.m)
        return self
