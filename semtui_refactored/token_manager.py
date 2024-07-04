import requests
import time
import jwt
from urllib.parse import urljoin, urlparse
from fake_useragent import UserAgent

class TokenManager:
    def __init__(self, api_url, username, password):
        self.api_url = api_url.rstrip('/')
        self.signin_url = urljoin(self.api_url, '/api/auth/signin')
        self.me_url = urljoin(self.api_url, '/api/auth/me')
        self.username = username
        self.password = password
        self.token = None
        self.expiry = 0
        self.session = requests.Session()
        self.ua = UserAgent()
        
        # Extract the origin from the API URL
        parsed_url = urlparse(self.api_url)
        self.origin = f"{parsed_url.scheme}://{parsed_url.netloc}"

    def get_token(self):
        if self.token is None or time.time() >= self.expiry:
            self.refresh_token()
        return self.token

    def refresh_token(self):
        headers = self._get_headers()
        data = {
            "username": self.username,
            "password": self.password
        }
        
        try:
            # Perform OPTIONS request for CORS preflight
            self.session.options(self.signin_url, headers=headers)
            
            # Perform POST request for signin
            response = self.session.post(self.signin_url, headers=headers, json=data)
            response.raise_for_status()
            token_info = response.json()
            self.token = token_info.get("token")
            
            if self.token:
                decoded = jwt.decode(self.token, options={"verify_signature": False})
                self.expiry = decoded.get('exp', time.time() + 3600)
                
                # Perform POST request to /api/auth/me
                me_headers = self._get_headers(with_token=True)
                me_response = self.session.post(self.me_url, headers=me_headers, json={"token": self.token})
                me_response.raise_for_status()
                
            else:
                self.expiry = time.time() + 3600
                
        except requests.RequestException as e:
            print(f"Sign-in request failed: {e}")
            if hasattr(e, 'response'):
                print(f"Response status code: {e.response.status_code}")
                print(f"Response content: {e.response.text}")
            self.token = None
            self.expiry = 0
        except jwt.DecodeError as e:
            print(f"Failed to decode JWT token: {e}")
            self.token = None
            self.expiry = 0

    def _get_headers(self, with_token=False):
        headers = {
            'Accept': 'application/json, text/plain, */*',
            'Content-Type': 'application/json;charset=UTF-8',
            'Origin': self.origin,
            'Referer': f"{self.origin}/",
            'User-Agent': self.ua.random
        }
        if with_token:
            headers['Authorization'] = f'Bearer {self.token}'
        else:
            headers['Authorization'] = 'Bearer null'
        return headers

    def get_headers(self):
        return self._get_headers(with_token=True)