# config.py
import os
from dotenv import load_dotenv

load_dotenv() # Loads variables from .env file

# --- Daemon Configuration ---
DAEMON_HOST = "0.0.0.0"
DAEMON_PORT = 9999

# --- Database Configuration ---
# The DATABASE_URL is now primarily controlled by the environment variable.
# This provides a default for local development if the env var is not set.
DATABASE_URL = os.environ.get('DATABASE_URL', 'postgresql://aviv:aviv@localhost/aviv')
