"""Main module."""
import os
import getpass
import requests


class GFW_api:
    BASE_URL = "https://gateway.api.globalfishingwatch.org/v3"
    VESSEL_API_ENDPOINT = "vessels/search"
    EVENTS_API_ENDPOINT = "events"



    def __init__(self, token=None):
        """
        Initialize the GFW API client.
        """
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

    def _make_request(self, endpoint, params=None):
        """
        Private method to send a GET request to the GFW API.
        :param endpoint: API endpoint (excluding the base URL).
        :param params: Dictionary of query parameters.
        :return: JSON response or None if an error occurs.
        """
        url = f"{self.BASE_URL}/{endpoint}"
        headers = {"Authorization": f"Bearer {self._token}"}

        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()  # Raise an error for HTTP issues
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
        print(response)

        if response and "entries" in response:
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

'''
#token = input('Enter TOKEN: ')

gfw = GFW_api(token)

gfw.search_vessel('7831410')
'''
