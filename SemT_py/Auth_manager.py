import requests
import json
import time
import jwt

class AuthManager:
    """
    A class to manage authentication tokens for API access.

    This class handles the retrieval and refreshing of authentication tokens
    required for accessing a secured API. It maintains the token's state and
    automatically refreshes it when it expires.

    Attributes:
    ----------
    api_url : str
        The base URL of the API for authentication.
    username : str
        The username for API authentication.
    password : str
        The password for API authentication.
    token : str
        The current authentication token.
    expiry : float
        The expiration time of the current token in Unix timestamp format.

    Methods:
    -------
    get_token():
        Retrieves the current token, refreshing it if necessary.
    refresh_token():
        Refreshes the authentication token by making a sign-in request.
    get_headers():
        Returns the headers required for API requests, including the authorization token.

    Usage:
    -----
    # Initialize the TokenManager with API credentials
    base_url = "https://api.example.com"
    api_url = f"{base_url}/auth"
    username = "your_username"
    password = "your_password"
    
    token_manager = TokenManager(api_url, username, password)
    
    # Get the token
    token = token_manager.get_token()
    
    # Use the token in API requests
    headers = token_manager.get_headers()
    response = requests.get(f"{base_url}/some_endpoint", headers=headers)
    """
    def __init__(self, api_url, username, password):
        self.api_url = api_url.rstrip('/')
        self.signin_url = f"{self.api_url}/auth/signin"
        self.username = username
        self.password = password
        self.token = None
        self.expiry = 0

    def get_token(self):
        """
        Retrieve the current authentication token.

        If the token is expired or not yet retrieved, this method will refresh
        the token by calling the `refresh_token` method.

        Returns:
        -------
        str
            The current authentication token.
        """
        if self.token is None or time.time() >= self.expiry:
            self.refresh_token()
        return self.token

    def refresh_token(self):
        """
        Refresh the authentication token by making a sign-in request.

        This method sends a POST request to the sign-in endpoint with the
        provided username and password. It updates the token and expiry
        attributes based on the response.

        Raises:
        ------
        requests.RequestException
            If the sign-in request fails.
        """
        signin_data = {"username": self.username, "password": self.password}
        signin_headers = {
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/json;charset=UTF-8",
        }

        try:
            response = requests.post(self.signin_url, headers=signin_headers, data=json.dumps(signin_data))
            response.raise_for_status()
            token_info = response.json()
            self.token = token_info.get("token")
            
            if self.token:
                decoded = jwt.decode(self.token, options={"verify_signature": False})
                self.expiry = decoded.get('exp', time.time() + 3600)
            else:
                self.expiry = time.time() + 3600
                
        except requests.RequestException as e:
            print(f"Sign-in request failed: {e}")
            if hasattr(e, 'response'):
                print(f"Response status code: {e.response.status_code}")
                print(f"Response content: {e.response.text}")
            self.token = None
            self.expiry = 0

    def get_headers(self):
        """
        Return the headers required for API requests, including the authorization token.

        This method constructs the headers needed for making authenticated API
        requests, using the current token.

        Returns:
        -------
        dict
            A dictionary containing the headers for API requests.
        """
        return {
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/json;charset=UTF-8",
            "Authorization": f"Bearer {self.get_token()}"
        }