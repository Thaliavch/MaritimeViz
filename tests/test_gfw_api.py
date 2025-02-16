import unittest
import os
from dotenv import load_dotenv
from src.maritimeviz.maritimeviz import GFW_api

# Load environment variables from .env
load_dotenv()


class TestGFWApi(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Initialize API client before running tests."""
        token = os.getenv("GFW_API_TOKEN")  # Load token from .env
        if not token:
            raise ValueError(
                "❌ API token not found! Make sure it's in your .env file.")

        cls.api_client = GFW_api(token)

    def test_search_vessel_works(self):
        """Test searching for a vessel using an IMO number."""
        imo_number = "7831410"  # Example IMO number
        response = self.api_client.search_vessel(imo_number)
        print(response)
        self.assertIsInstance(response, list)  # Should return a list
        self.assertGreater(len(response), 0)  # Ensure at least one result

    def test_get_fishing_events_works(self):
        """Test fetching fishing events for a vessel."""
        vessel_id = "9b3e9019d-d67f-005a-9593-b66b997559e5"  # Example vessel ID
        start_date = "2017-03-01"
        end_date = "2017-03-31"
        response = self.api_client.get_fishing_events(vessel_id, start_date,
                                                      end_date)
        print(response)
        self.assertIsInstance(response, list)  # Should return a list


if __name__ == "__main__":
    unittest.main()
