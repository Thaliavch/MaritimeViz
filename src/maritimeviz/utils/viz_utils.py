import leafmap
import geopandas as gpd
import folium
from IPython.display import display
from geopandas import GeoDataFrame


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

    legend_html = '''
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
    '''

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

    info_text = "<br>".join([f"{key}: {value}" for key, value in row.items() if value and key != "geometry"])
    name = row.get("mmsi", row.get("name", row.get("id", "Unknown")))

    return name, info_text

def geojson_to_wkt(geojson_polygon):

    coords = geojson_polygon["geometry"]["coordinates"][0]
    coord_strings = [f"{lon} {lat}" for lon, lat in coords]
    coord_block = ",\n".join(coord_strings)
    wkt = f"POLYGON((\n{coord_block}\n))"
    return wkt


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
              icon=folium.Icon(color=color, icon="ship", prefix="fa"),
              location=[row.geometry.y, row.geometry.x],  # Latitude, Longitude
              popup=folium.Popup(info_text, max_width=300),  # Display all available info
              tooltip='Press for more info'  # Use available identifier
          ).add_to(m)

      display(m)
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
    - geojson_data (str, dict, or GeoDataFrame): path to GeoJSON file, GeoJSON object, or a GeoDataFrame

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



