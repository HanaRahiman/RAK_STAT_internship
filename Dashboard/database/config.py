import os
from dotenv import load_dotenv
from urllib.parse import quote_plus

# Load environment variables from .env file
load_dotenv()

# Database configuration
DATABASE_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'dashboard_db'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'password')
}

# URL encode the password to handle special characters
encoded_password = quote_plus(DATABASE_CONFIG['password'])

# Database URL for SQLAlchemy with properly encoded password
DATABASE_URL = f"postgresql://{DATABASE_CONFIG['user']}:{encoded_password}@{DATABASE_CONFIG['host']}:{DATABASE_CONFIG['port']}/{DATABASE_CONFIG['database']}"

# Alternative connection parameters for direct psycopg2 usage
DATABASE_PARAMS = {
    'host': DATABASE_CONFIG['host'],
    'port': DATABASE_CONFIG['port'], 
    'database': DATABASE_CONFIG['database'],
    'user': DATABASE_CONFIG['user'],
    'password': DATABASE_CONFIG['password']
}

# Together AI configuration
TOGETHER_AI_CONFIG = {
    'api_key': os.getenv('TOGETHER_API_KEY'),
    'model': 'meta-llama/Llama-3.3-70B-Instruct-Turbo-Free',
    'max_tokens': 1000,
    'temperature': 0.7
}

# Application settings
APP_CONFIG = {
    'debug': os.getenv('DEBUG', 'False').lower() == 'true',
    'page_title': 'Socio-Economic Platform',
    'page_icon': '📊'
} 