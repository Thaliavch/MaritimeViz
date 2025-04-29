"""Main module."""

import os

import folium
import pandas as pd
import requests
from cachetools import TTLCache
from folium.plugins import HeatMap

from .auth import load_or_get_token, GFW


class GFW_api:
    BASE_URL = "https://gateway.api.globalfishingwatch.org/v3"
    VESSEL_API_ENDPOINT = "vessels/search"
    EVENTS_API_ENDPOINT = "events"
    STATS_API_ENDPOINT = "4wings/stats"
    INSIGHTS_API_ENDPOINT = "insights/vessels"
    GENERATE_PNG_API_ENDPOINT = "4wings/generate-png"

    def __init__(self, token=None):
        """
        Initialize the GFW API client.
        """
        # cache up to 100 results for 300 seconds
        self._cache = TTLCache(maxsize=100, ttl=300)

        if token:
            self._token = token
        else:
            self._token = load_or_get_token(GFW)
        print("Powered by Global Fishing Watch. https://globalfishingwatch.org/")

    @property
    def token(self):
        """Prevent direct access to the token."""
        raise AttributeError(
            "Access to the API token is restricted for security reasons."
        )

    @token.setter
    def token(self, new_token):
        """Allow securely updating the token."""
        if new_token:
            self._token = new_token
            os.environ["GFW_API_TOKEN"] = new_token  # Store in session
        else:
            raise ValueError("Token cannot be empty!")

    # Caching POST requests is not useful
    def _post_request(self, endpoint, payload):
        """
        Private method to send a POST request to the GFW API.
        :param endpoint: API endpoint (excluding the base URL).
        :param payload: Dictionary containing the request body.
        :return: JSON response or None if an error occurs.
        """
        url = f"{self.BASE_URL}/{endpoint}"
        headers = {
            "Authorization": f"Bearer {self._token}",
            "Content-Type": "application/json",
        }

        try:
            if endpoint == "4wings/generate-png":
                response = requests.post(url, params=payload, headers=headers)
            else:
                response = requests.post(url, json=payload, headers=headers)

            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            print(f"Error fetching data: {e}")
            return None

    def _get_request(self, endpoint, params=None):
        """
        Private method to send a GET request to the GFW API.
        :param endpoint: API endpoint (excluding the base URL).
        :param params: Dictionary of query parameters.
        :return: JSON response or None if an error occurs.
        """
        # Create an immutable cache key (tuple: endpoint + sorted params)
        cache_key = (endpoint, frozenset(params.items()) if params else None)

        # Check if the request is already cached
        if cache_key in self._cache:
            print(
                f"\nData fetched from cache. Cache key: {cache_key}\n"
            )  # For debugging purposes
            return self._cache[cache_key]

        url = f"{self.BASE_URL}/{endpoint}"
        headers = {"Authorization": f"Bearer {self._token}"}

        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()  # Raise an error for HTTP issues
            data = response.json()
            self._cache[cache_key] = data  # Store response in cache
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data: {e}")
            return None

    def search_vessel(self, identifier=None):
        """
        Search for a vessel using MMSI or IMO.
        :param identifier: MMSI (9-digit number) or IMO (7-digit number).
        :return: JSON response with vessel details.
        """
        params = {
            "query": identifier,
            "datasets[0]": "public-global-vessel-identity:latest",
        }

        response = self._get_request(self.VESSEL_API_ENDPOINT, params)

        if response and "entries" in response:
            print(response)
            return response["entries"]  # List of vessels matching the query
        return None

    def get_fishing_events(self, vessel_id, start_date, end_date, limit=10, offset=0):
        """
        Get fishing events for a specific vessel and return structured output.
        Includes structured DataFrame, charts, and maps.

        :param vessel_id: Vessel ID (found in the search response).
        :param start_date: Start date (YYYY-MM-DD).
        :param end_date: End date (YYYY-MM-DD).
        :param limit: Number of records to return (default: 10).
        :param offset: Offset for pagination (default: 0).
        :return: Pandas DataFrame with structured event data.
        """
        params = {
            "vessels[0]": vessel_id,
            "datasets[0]": "public-global-fishing-events:latest",
            "start-date": start_date,
            "end-date": end_date,
            "limit": limit,
            "offset": offset,
        }

        data = self._get_request(self.EVENTS_API_ENDPOINT, params)

        if not data or "entries" not in data:
            print("⚠️ No fishing events found for the given vessel.")
            return None

        # Extract relevant fishing event details
        events_data = []
        for event in data["entries"]:
            events_data.append(
                {
                    "Event ID": event.get("id"),
                    "Type": event.get("type"),
                    "Start Time": event.get("start"),
                    "End Time": event.get("end"),
                    "Latitude": event["position"]["lat"]
                    if "position" in event
                    else None,
                    "Longitude": event["position"]["lon"]
                    if "position" in event
                    else None,
                    "EEZ": event["regions"]["eez"] if "regions" in event else [],
                    "RFMO": event["regions"]["rfmo"] if "regions" in event else [],
                    "Total Distance (km)": event["fishing"]["totalDistanceKm"]
                    if "fishing" in event
                    else None,
                    "Avg Speed (knots)": event["fishing"]["averageSpeedKnots"]
                    if "fishing" in event
                    else None,
                }
            )

        df = pd.DataFrame(events_data)

        return df

    @staticmethod
    def fishing_event_map(df):
        """
        Generate an interactive map of fishing events.
        """
        if df.empty or "Latitude" not in df or "Longitude" not in df:
            print("No valid location data available for mapping.")
            return

        # Create Map Centered at the First Event Location
        lat, lon = df["Latitude"].mean(), df["Longitude"].mean()
        fishing_map = folium.Map(location=[lat, lon], zoom_start=4)

        # Add Fishing Event Markers
        for _, row in df.iterrows():
            folium.Marker(
                location=[row["Latitude"], row["Longitude"]],
                popup=f"Event ID: {row['Event ID']}\nType: {row['Type']}",
                icon=folium.Icon(color="blue", icon="info-sign"),
            ).add_to(fishing_map)

        return fishing_map

    @staticmethod
    def fishing_event_heatmap(df):
        """
        Generate a heatmap of fishing event locations.
        """
        if df.empty or "Latitude" not in df or "Longitude" not in df:
            print("No valid location data available for mapping.")
            return

        # Create Map Centered at the Mean Location
        lat, lon = df["Latitude"].mean(), df["Longitude"].mean()
        fishing_map = folium.Map(location=[lat, lon], zoom_start=4)

        # Add Heatmap Layer
        heat_data = [[row["Latitude"], row["Longitude"]] for _, row in df.iterrows()]
        HeatMap(heat_data).add_to(fishing_map)

        return fishing_map

    def get_fishing_stats(self, start_date, end_date, wkt_polygon=None):
        """
        Get fishing effort statistics for a given date range within a WKT
        polygon.

        :param start_date: Start date in YYYY-MM-DD format.
        :param end_date: End date in YYYY-MM-DD format.
        :param wkt_polygon: Polygon in WKT format.
        :return: JSON response with fishing statistics.
        """

        params = {
            "datasets[0]": "public-global-fishing-effort:latest",
            "date-range": f"{start_date},{end_date}",
            "fields": "FLAGS,VESSEL-IDS,ACTIVITY-HOURS",
        }

        if wkt_polygon:
            params["geopolygon"] = wkt_polygon

        # Getting data response
        data = self._get_request(self.STATS_API_ENDPOINT, params)

        if data:
            print(data)
            return data
        else:
            print("No data available for the specified date range.")
            return None

    # GET INSIGHTS FOR A VESSEL RELATED TO FISHING EVENTS
    def get_vessel_insights(self, start_date, end_date, vessels):
        """
        Fetches vessel insights for the given vessels within a specific time range.

        :param start_date: Start date in "YYYY-MM-DD" format.
        :param end_date: End date in "YYYY-MM-DD" format.
        :param vessels: List of dictionaries containing datasetId and vesselId.
        :return: JSON response with vessel insights or an error message.
        """
        payload = {
            "includes": ["FISHING"],
            "startDate": start_date,
            "endDate": end_date,
            "vessels": vessels,
        }

        data = self._post_request(self.INSIGHTS_API_ENDPOINT, payload)

        if data:
            print(data)
            return data
        else:
            print("No data available for the specified date range.")
            return None

    # EXAMPLE 1: AIS APPARENT FISHING EFFORT - GENERATE PNG TILES WITH TEMPORAL FILTER
    def generate_fishing_effort_png_tiles(
        self, interval, dataset, color, start_date, end_date
    ):
        """
        Generates PNG tiles of fishing effort.

        :param interval: Time interval for the visualization (e.g., "DAY").
        :param dataset: Dataset to use (default: "public-global-fishing-effort:latest").
        :param color: Hex color code for visualization (default: "#361c0c").
        :param start_date: Start date in "YYYY-MM-DD" format.
        :param end_date: End date in "YYYY-MM-DD" format.
        """

        params = {
            "interval": interval,
            "datasets[0]": dataset,
            "color": color,
            "date-range": f"{start_date},{end_date}",
        }

        data = self._post_request(self.GENERATE_PNG_API_ENDPOINT, params)

        if data:
            print(data)
            return data
        else:
            print("Error retrieving PNG tiles.")
            return None
