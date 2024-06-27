import requests
from time import time

class TokenManager:
    def __init__(self, api_url, signin_data, signin_headers):
        self.api_url = api_url
        self.signin_url = f"{api_url}{'auth/signin' if 'localhost' in api_url else 'signin'}"
        self.signin_data = signin_data
        self.signin_headers = signin_headers
        self.token = None
        self.expiry = 0

    def get_token(self):
        if self.token is None or time() >= self.expiry:
            self.refresh_token()
        return self.token

    def refresh_token(self):
        try:
            response = requests.post(self.signin_url, headers=self.signin_headers, json=self.signin_data)
            response.raise_for_status()
            user_info = response.json()
            self.token = user_info.get("token")
            self.expiry = time() + 3600  # Assuming the token expires in 1 hour
        except requests.RequestException as e:
            print(f"Sign-in request failed: {e}")
            self.token = None
            self.expiry = 0