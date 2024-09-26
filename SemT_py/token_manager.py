import requests
import json
import time
import jwt

class TokenManager:
    def __init__(self, api_url, username, password):
        self.api_url = api_url.rstrip('/')
        self.signin_url = f"{self.api_url}/auth/signin"
        self.username = username
        self.password = password
        self.token = None
        self.expiry = 0

    def get_token(self):
        if self.token is None or time.time() >= self.expiry:
            self.refresh_token()
        return self.token

    def refresh_token(self):
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
        return {
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/json;charset=UTF-8",
            "Authorization": f"Bearer {self.get_token()}"
        }