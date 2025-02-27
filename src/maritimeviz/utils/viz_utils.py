import gpd
import leafmap
import geopandas as gpd


def create_speed_legend():
    """Generates an HTML legend for the speed color map."""
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
    info_text = "<br>".join([f"{key}: {value}" for key, value in row.items() if value and key != "geometry"])
    name = row.get("mmsi", row.get("name", row.get("id", "Unknown")))

    return name, info_text



def map(self, route_geojson):
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


    return self.m


