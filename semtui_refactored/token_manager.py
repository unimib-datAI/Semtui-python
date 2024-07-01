import requests
from time import time
from urllib.parse import urljoin

class TokenManager:
    def __init__(self, api_url, username, password):
        self.api_url = api_url.rstrip('/') + '/'  # Ensure the URL ends with a slash
        self.signin_url = urljoin(self.api_url, 'auth/signin')
        self.username = username
        self.password = password
        self.token = None
        self.expiry = 0

    def get_token(self):
        if self.token is None or time() >= self.expiry:
            self.refresh_token()
        return self.token

    def refresh_token(self):
        headers = {
            'Accept': 'application/json, text/plain, */*',
            'Content-Type': 'application/json;charset=UTF-8',
            'Origin': self.api_url.rstrip('/'),
            'Referer': self.api_url
        }
        data = {
            "username": self.username,
            "password": self.password
        }
        
        try:
            response = requests.post(self.signin_url, headers=headers, json=data)
            response.raise_for_status()
            token_info = response.json()
            self.token = token_info.get("token")
            
            # Calculate expiry from JWT if possible, otherwise use a default
            if self.token:
                import jwt
                decoded = jwt.decode(self.token, options={"verify_signature": False})
                self.expiry = decoded.get('exp', time() + 3600)  # Default to 1 hour if 'exp' not found
            else:
                self.expiry = time() + 3600  # Default to 1 hour
                
        except requests.RequestException as e:
            print(f"Sign-in request failed: {e}")
            self.token = None
            self.expiry = 0