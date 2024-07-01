import requests
import time
import jwt
from urllib.parse import urljoin
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class TokenManager:
    def __init__(self, api_url, username, password):
        self.api_url = api_url.rstrip('/')
        self.signin_url = urljoin(self.api_url, '/api/auth/signin')
        self.username = username
        self.password = password
        self.token = None
        self.expiry = 0
        logger.debug(f"Initialized TokenManager with API URL: {self.api_url}")

    def get_token(self):
        if self.token is None or time.time() >= self.expiry:
            self.refresh_token()
        return self.token

    def refresh_token(self):
        headers = {
            'Accept': 'application/json, text/plain, */*',
            'Content-Type': 'application/json;charset=UTF-8',
            'Origin': self.api_url,
            'Referer': self.api_url + '/'
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
            
            if self.token:
                decoded = jwt.decode(self.token, options={"verify_signature": False})
                self.expiry = decoded.get('exp', time.time() + 3600)
                logger.debug(f"Token refreshed successfully. Expires at: {time.ctime(self.expiry)}")
            else:
                self.expiry = time.time() + 3600
                logger.warning("Token not found in response. Setting default expiry.")
                
        except requests.RequestException as e:
            logger.error(f"Sign-in request failed: {e}")
            if hasattr(e, 'response'):
                logger.error(f"Response status code: {e.response.status_code}")
                logger.error(f"Response content: {e.response.text}")
            self.token = None
            self.expiry = 0
        except jwt.DecodeError as e:
            logger.error(f"Failed to decode JWT token: {e}")
            self.token = None
            self.expiry = 0
