"""Main module."""
import os
import getpass
import requests
from cachetools import TTLCache


class GFW_api:
    BASE_URL = "https://gateway.api.globalfishingwatch.org/v3"
    VESSEL_API_ENDPOINT = "vessels/search"
    EVENTS_API_ENDPOINT = "events"
    STATS_API_ENDPOINT = "4wings/stats"
    INSIGHTS_API_ENDPOINT = "insights/vessels"



    def __init__(self, token=None):
        """
        Initialize the GFW API client.
        """
        #cache up to 100 results for 300 seconds
        self._cache = TTLCache(maxsize=100, ttl=300)

        if token:
            self._token = token
        else:
            self._token = os.environ.get(
                "GFW_API_TOKEN")  # Check environment variable
            if not self._token:
                self._token = getpass.getpass(
                    "Enter your Global Fishing Watch API token: ")
                os.environ[
                    "GFW_API_TOKEN"] = self._token  # Store for session reuse
        print(
            "Powered by Global Fishing Watch. https://globalfishingwatch.org/")

    @property
    def token(self):
        """Prevent direct access to the token."""
        raise AttributeError(
            "Access to the API token is restricted for security reasons.")

    @token.setter
    def token(self, new_token):
        """Allow securely updating the token."""
        if new_token:
            self._token = new_token
            os.environ["GFW_API_TOKEN"] = new_token  # Store in session
        else:
            raise ValueError("Token cannot be empty!")
    
    #Caching POST requests is not useful    
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
            "Content-Type": "application/json"
        }

        try:
            response = requests.post(url, json=payload, headers=headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data: {e}")
            return None
        
    def _make_request(self, endpoint, params=None):
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
            print(f"\nData fetched from cache. Cache key: {cache_key}\n")  # For debugging purposes
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
        params = {"query": identifier,
                  "datasets[0]": "public-global-vessel-identity:latest"}

        response = self._make_request(self.VESSEL_API_ENDPOINT, params)

        if response and "entries" in response:
            print(response)
            return response["entries"]  # List of vessels matching the query
        return None

    def get_fishing_events(self, vessel_id, start_date, end_date, limit=10,
                           offset=0):
        """
        Get fishing events for a specific vessel.
        :param vessel_id: Vessel ID (found in the search response).
        :param start_date: Start date (YYYY-MM-DD).
        :param end_date: End date (YYYY-MM-DD).
        :param limit: Number of records to return (default: 10).
        :param offset: Offset for pagination (default: 0).
        :return: JSON response with fishing events.
        """
        params = {
            "vessels[0]": vessel_id,
            "datasets[0]": "public-global-fishing-events:latest",
            "start-date": start_date,
            "end-date": end_date,
            "limit": limit,
            "offset": offset
        }

        data = self._make_request(self.EVENTS_API_ENDPOINT, params)
        if data and "entries" in data:
            return data["entries"]  # List of fishing events
        return None
    
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
            "fields": "FLAGS,VESSEL-IDS,ACTIVITY-HOURS"  
        }

        if wkt_polygon:
            params["geopolygon"] = wkt_polygon 

        # Gettin data response
        data = self._make_request(self.STATS_API_ENDPOINT, params)
        
        if data:
            print(data)
            return data
        else:
            print("No data available for the specified date range.")
            return None
        
    #GET INSIGHTS FOR A VESSEL RELATED TO FISHING EVENTS
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
            "vessels": vessels
        }

        data = self._post_request(self.INSIGHTS_API_ENDPOINT, payload)
        
        if data:
            print(data)
            return data
        else:
            print("No data available for the specified date range.")
            return None


#token = input('Enter TOKEN: ')
#
#gfw = GFW_api(token)
#
#gfw.search_vessel('7831410')
#
#gfw.get_fishing_stats(start_date='2022-01-01', end_date='2023-01-01')
#
#gfw.get_fishing_stats(start_date='2022-01-01', end_date='2023-01-01')
#
#gfw.get_vessel_insights(start_date='2022-01-01', end_date='2023-01-01', vessels = [
#    {"datasetId": "public-global-vessel-identity:latest", "vesselId": "785101812-2127-e5d2-e8bf-7152c5259f5f"}
#])