"""Environment-backed application configuration."""
import os
from pathlib import Path
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / '.env')

DB_SERVER = os.getenv('DB_SERVER', 'localhost')
DB_NAME = os.getenv('DB_NAME', 'RealtimeEcommerceDB')
DB_USERNAME = os.getenv('DB_USERNAME', '')
DB_PASSWORD = os.getenv('DB_PASSWORD', '')
DB_DRIVER = os.getenv('DB_DRIVER', '')
DB_TRUSTED_CONNECTION = os.getenv('DB_TRUSTED_CONNECTION', 'yes').lower() in {'1', 'true', 'yes'}
API_HOST = os.getenv('API_HOST', '127.0.0.1')
API_PORT = int(os.getenv('API_PORT', '8000'))
API_KEY = os.getenv('API_KEY', '')
