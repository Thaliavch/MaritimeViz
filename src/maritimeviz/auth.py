"""
auth.py

Helper functions to load and store API tokens using a .env file.
If a token for a given key isn’t found, the user is prompted to enter it,
and it is saved for use along all the modules.

"""

import os
import getpass
from dotenv import load_dotenv, set_key

ENV_FILE = os.path.join(os.path.dirname(__file__), "../../.env")

# List token lists here
GFW = "GFW_API_TOKEN"


def load_or_get_token(token_key):
    # Loading the .env file from the package directory.
    load_dotenv(ENV_FILE)

    token = os.environ.get(token_key)
    if token:
        return token

    # If not found, prompt the user.
    token = getpass.getpass("Enter your Global Fishing Watch API token {token_key}: ")
    # Save the token to the .env file.
    set_key(ENV_FILE, token_key, token)
    return token
