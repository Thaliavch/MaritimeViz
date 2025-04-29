import folium
import geopandas as gpd
from geopandas import GeoDataFrame
from geopy.distance import geodesic
from ipyleaflet import Marker, AwesomeIcon, Polygon, LayerGroup, Circle
from ipywidgets import HTML
from shapely.wkt import loads


def filter_ships_by_polygon(wkt_polygon, gdf):
    """
    Filter ship positions that fall within a specified polygon.

    This function takes a GeoDataFrame of ship positions (with latitude and longitude)
    and returns only those ships located inside the area defined by a WKT (Well-Known Text) polygon.

    Parameters:
        wkt_polygon (str):
            A string in WKT format representing the polygon to filter by.

        gdf (GeoDataFrame):
            A GeoPandas GeoDataFrame containing ship data with at least
            'latitude' and 'longitude' columns.

    Returns:
        GeoDataFrame:
            A filtered GeoDataFrame containing only the ships located inside the polygon.

    Raises:
        ValueError:
            If the provided WKT polygon string is invalid and cannot be parsed.

    Notes:
        - The function creates a new 'geometry' column in the GeoDataFrame using
         latitude and longitude.
        - Ships on the border of the polygon are excluded (strict `within` filter).

    Example:
        polygon = "POLYGON((-81 25, -81 26, -80 26, -80 25, -81 25))"
        filtered_ships = instance.filter_ships_by_polygon(polygon, ships_gdf)
    """

    try:
        polygon = loads(wkt_polygon)  # Convert WKT string to Shapely Polygon
    except Exception:
        raise ValueError("Invalid WKT polygon format")

    gdf["geometry"] = gpd.points_from_xy(
        gdf.longitude, gdf.latitude
    )  # Convert lat/lon to points

    return gdf[gdf.geometry.within(polygon)]  # Filter points within the polygon


def create_speed_legend():
    """
    Generates an HTML string for a fixed-position speed legend to be displayed on a web map.

    The legend shows speed ranges in knots using colored indicators:
        - Green: 0–2 knots
        - Blue: 2–10 knots
        - Orange: 10–25 knots
        - Red: 25–30 knots
        - Purple: 30+ knots

    The legend is styled with a white background, rounded corners, and shadow for better visibility.

    Returns:
        str: A string containing HTML and inline CSS for rendering the speed legend on a map.
    """

    legend_html = """
    <div style="
        position: fixed;
        bottom: 30px; left: 30px;
        width: 150px; height: auto;
        background-color: white;
        z-index:9999;
        font-size:14px;
        border-radius: 10px;
        padding: 10px;
        box-shadow: 2px 2px 5px rgba(0,0,0,0.3);
    ">
        <b>Speed Legend (knots)</b><br>
        <i style="background:green;width:20px;height:10px;display:inline-block;"></i> 0-2 <br>
        <i style="background:blue;width:20px;height:10px;display:inline-block;"></i> 2-10 <br>
        <i style="background:orange;width:20px;height:10px;display:inline-block;"></i> 10-25 <br>
        <i style="background:red;width:20px;height:10px;display:inline-block;"></i> 25-30 <br>
        <i style="background:purple;width:20px;height:10px;display:inline-block;"></i> 30+ <br>
    </div>
    """

    return legend_html


def get_info(row):
    """
    Extracts and formats information from a dictionary representing a data row.

    Parameters:
        row (dict): A dictionary containing key-value pairs. Expected to possibly contain
                    keys such as "mmsi", "name", "id", and "geometry", among others.

    Returns:
        tuple:
            - name (str): The value of the "mmsi" key if present, otherwise "name",
                            then "id", and defaults to "Unknown" if none are found.
            - info_text (str): An HTML-formatted string where each key-value pair
                                (excluding the "geometry" key and any empty values) is
                                presented on a separate line using <br> tags.
    """

    info_text = "<br>".join(
        [f"{key}: {value}" for key, value in row.items() if value and key != "geometry"]
    )
    name = row.get("mmsi", row.get("name", row.get("id", "Unknown")))

    return name, info_text


def plot_with_info(gdf, m, speed_flag=False, color="blue"):
    for _, row in gdf.iterrows():
        name, info_text = get_info(row)
        color = color

        if speed_flag:
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

        if row.geometry and hasattr(row.geometry, "x") and hasattr(row.geometry, "y"):
            folium.Marker(
                icon=folium.Icon(
                    color=color, icon=check_printable_icon(row), prefix="fa"
                ),
                location=[row.geometry.y, row.geometry.x],
                # Latitude, Longitude
                popup=folium.Popup(info_text, max_width=300),
                # Display all available info
                tooltip="Press for more info",  # Use available identifier
            ).add_to(m)

        # display(m)
        m.add_layer_control()
        return m


def check_printable_icon(row):
    """
    Checks for which Icon to use based on message type.

    Parameters:
    - row (tuple[int, str]): row with information from geojson.

    Returns:
    - str: icon name to be used
    """

    try:
        id_ = row["id"]
    except (KeyError, TypeError):
        return "asterisk"

    if id_ in {1, 2, 3, 18, 19, 27}:  # Vessels
        return "ship"
    elif id_ in {4, 11}:  # Land Station
        return "broadcast-tower"
    elif id_ == 9:  # Search and Rescue Aircraft
        return "plane"
    elif id_ == 21:  # Aids to Navigation
        return "plus"
    else:
        return "asterisk"


def verify_geojson(geojson_data):
    """
    Verify if the provided GeoJSON is valid and convert it into a GeoDataFrame.
    If the GeoJSON is not valid, raises a ValueError.

    Parameters:
    - geojson_data (str, dict, or GeoDataFrame): path to GeoJSON file, GeoJSON
    object, or a GeoDataFrame

    Returns:
    - GeoDataFrame: a GeoDataFrame containing the provided GeoJSON data
    """
    try:
        if isinstance(geojson_data, GeoDataFrame):
            return geojson_data

        if isinstance(geojson_data, dict) and "features" in geojson_data:
            return gpd.GeoDataFrame.from_features(geojson_data["features"])

        # Else, assume it's a file path or file-like object
        return gpd.read_file(geojson_data)

    except Exception as e:
        raise ValueError(f"Failed to load GeoJSON: {e}")


# ========================================================================================================================


def handle_draw(state, map_obj, geojson_data, target, action, geo_json):
    """
    Callback function triggered on drawing events.
    'state' is a dictionary passed in (via partial) to hold drawing state.
    """
    geom_type = geo_json.get("geometry", {}).get("type", "").lower()
    if geom_type != "polygon":
        print("Circles are not supported in this version")
        return

    # Append the new polygon feature to the state.
    state["features"].append(geo_json)

    # Update the map with all ship markers based on the current features.
    new_marker_layer, new_polygon_layer = update_map_with_all_ships_for_drawing(
        map_obj,
        geojson_data,
        # Use the geojson file passed into the ships_by_drawn_shape function.
        state["features"],
        state["ship_marker_layer"],
        state["ship_polygon_layer"],
    )

    # Update the state with the new layer groups.
    state["ship_marker_layer"] = new_marker_layer
    state["ship_polygon_layer"] = new_polygon_layer


def update_map_with_all_ships_for_drawing(
    map_obj, geojson_data, features, old_marker_layer, old_polygon_layer
):
    """
    For every drawn polygon stored in 'features', create a polygon overlay and
    ship markers for ships within that polygon. Any previous layers are removed from the map.
    Returns the new marker and polygon layers.
    """
    # Remove old layers if they exist.
    if old_marker_layer is not None and old_marker_layer in map_obj.layers:
        map_obj.remove(old_marker_layer)
    if old_polygon_layer is not None and old_polygon_layer in map_obj.layers:
        map_obj.remove(old_polygon_layer)

    # Create new layer groups for markers and polygon overlays.
    new_marker_layer = LayerGroup()
    new_polygon_layer = LayerGroup()

    # Load AIS ship data once.
    gdf = verify_geojson(geojson_data)

    # Process each drawn polygon feature.
    for feature in features:
        wkt_shape = geojson_to_wkt(feature)

        # Create a polygon overlay.
        poly_geom = loads(wkt_shape)
        coords = list(poly_geom.exterior.coords)
        # ipyleaflet expects (latitude, longitude) pairs.
        locations = [(lat, lon) for lon, lat in coords]
        poly_overlay = Polygon(
            locations=locations,
            color="yellow",
            fill_color="yellow",
            fill_opacity=0.2,
            weight=3,
        )
        new_polygon_layer.add(poly_overlay)

        # Filter AIS ship data to include only ships within this polygon.
        filtered_gdf = filter_ships_by_polygon(wkt_shape, gdf)
        for _, row in filtered_gdf.iterrows():
            # Use the FontAwesome ship icon ("fa-ship") with a constant marker color "blue".
            icon = AwesomeIcon(name="fa-ship", marker_color="blue", icon_color="white")
            marker = folium.Marker(
                location=(row.latitude, row.longitude), draggable=False, icon=icon
            )
            # Attach a popup with ship details (black text).
            _, info_text = get_info(row)
            marker.popup = HTML(value=f"<div style='color:black;'>{info_text}</div>")
            new_marker_layer.add(marker)

    # Add the new layers to the map.
    map_obj.add(new_polygon_layer)
    map_obj.add(new_marker_layer)

    return new_marker_layer, new_polygon_layer


def geojson_to_wkt(geojson_polygon):
    coords = geojson_polygon["geometry"]["coordinates"][0]
    coord_strings = [f"{lon} {lat}" for lon, lat in coords]
    coord_block = ",\n".join(coord_strings)
    wkt = f"POLYGON((\n{coord_block}\n))"
    return wkt


def create_click_handler(radius_km, map_object, clicked_coords, gdf):
    current_ship_group = []
    current_circle = []

    def handle_click(**kwargs):
        if kwargs.get("type") == "click":
            latlng = kwargs.get("coordinates")
            clicked_coords.append(latlng)
            print(f"Clicked at: {latlng}")

            # Clear previous ships and circle
            if current_ship_group:
                try:
                    map_object.remove_layer(current_ship_group[0])
                except Exception as e:
                    print(f"Couldn't remove ship group: {e}")
                current_ship_group.clear()

            if current_circle:
                try:
                    map_object.remove_layer(current_circle[0])
                except Exception as e:
                    print(f"Couldn't remove circle: {e}")
                current_circle.clear()

            # Add new circle
            circle = Circle(
                location=latlng,
                radius=radius_km * 1000,
                color="blue",
                fill_color="blue",
                fill_opacity=0.5,
            )
            map_object.add_layer(circle)
            current_circle.append(circle)

            # Find ships inside the new circle
            ships_in_circle = []
            for _, row in gdf.iterrows():
                coordinates = row.geometry.coords[0]
                ship_location = (coordinates[1], coordinates[0])  # (lat, lon)
                if -90 <= coordinates[1] <= 90 and -180 <= coordinates[0] <= 180:
                    if geodesic(latlng, ship_location).km <= radius_km:
                        ships_in_circle.append(row)

            print(f"Ships found: {len(ships_in_circle)}")

            # Build ship markers and popups
            markers = []
            for ship in ships_in_circle:
                name, info_text = get_info(ship)

                lat, lon = ship.geometry.y, ship.geometry.x
                marker = Marker(location=(lat, lon), draggable=False)

                # Style text to be black
                popup_content = HTML(
                    value=f"<div style='color:black;'>{info_text}</div>"
                )
                marker.popup = popup_content

                markers.append(marker)

            # Add all markers at once using a LayerGroup
            group = LayerGroup(layers=markers)
            map_object.add_layer(group)
            current_ship_group.append(group)

    return handle_click
