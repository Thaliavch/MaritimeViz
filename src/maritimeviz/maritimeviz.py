"""Main module."""
import requests


class GFW_api:

    def __init__(self, token=None):
        """
        Initialize the GFW API client.
        """
        self.token = token
        print("Powered by Global Fishing Watch. https://globalfishingwatch.org/"
              
    # Search Function from Global Fishing Watch"
    def search_vessel(self, mmsi=None):

        try:
            headers = {
                'Authorization': f'Bearer {self.token}',
            }

            response = requests.get(
                'https://gateway.api.globalfishingwatch.org/v3/vessels/search?query=368045130&datasets[0]=public-global-vessel-identity:latest&includes[0]=MATCH_CRITERIA&includes[1]=OWNERSHIP&includes[2]=AUTHORIZATIONS',
                headers=headers,
            )

            print(response.status_code)
            print(response.json())

        except Exception as e:
            print(f"Error fetching data: {e}")

'''
#token = input('Enter TOKEN: ')

gfw = GFW_api(token)

gfw.search_vessel('7831410')
'''