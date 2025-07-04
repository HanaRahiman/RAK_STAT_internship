import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import logging
from .config import DATABASE_CONFIG

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_database():
    """Create the database if it doesn't exist"""
    try:
        # Connect to PostgreSQL server (not to specific database)
        conn = psycopg2.connect(
            host=DATABASE_CONFIG['host'],
            port=DATABASE_CONFIG['port'],
            user=DATABASE_CONFIG['user'],
            password=DATABASE_CONFIG['password'],
            database='postgres'  # Connect to default postgres database
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        # Check if database exists
        cursor.execute(f"SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{DATABASE_CONFIG['database']}'")
        exists = cursor.fetchone()
        
        if not exists:
            cursor.execute(f"CREATE DATABASE {DATABASE_CONFIG['database']}")
            logger.info(f"Database '{DATABASE_CONFIG['database']}' created successfully")
        else:
            logger.info(f"Database '{DATABASE_CONFIG['database']}' already exists")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"Error creating database: {e}")
        return False

def create_tables():
    """Create necessary tables"""
    try:
        conn = psycopg2.connect(**DATABASE_CONFIG)
        cursor = conn.cursor()
        
        # Drop existing table to recreate with updated schema
        cursor.execute("DROP TABLE IF EXISTS social_media_data CASCADE;")
        
        # Create social_media_data table with updated schema
        create_table_query = """
        CREATE TABLE IF NOT EXISTS social_media_data (
            id SERIAL PRIMARY KEY,
            title TEXT,
            url TEXT,
            summary TEXT,
            content TEXT,
            comment TEXT,
            comment_sentiment VARCHAR(20),
            author TEXT,
            combined_text TEXT,
            relevance_score DECIMAL,
            relevant_to_education_in_uae BOOLEAN,
            sentiment_negative DECIMAL,
            sentiment_neutral DECIMAL,
            sentiment_positive DECIMAL,
            sentiment_predicted VARCHAR(20),
            sentiment_confidence DECIMAL,
            date DATE,
            platform VARCHAR(50),
            platform_type VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        cursor.execute(create_table_query)
        
        # Create indexes for better performance
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_platform ON social_media_data(platform);",
            "CREATE INDEX IF NOT EXISTS idx_sentiment ON social_media_data(sentiment_predicted);",
            "CREATE INDEX IF NOT EXISTS idx_date ON social_media_data(date);",
            "CREATE INDEX IF NOT EXISTS idx_platform_date ON social_media_data(platform, date);",
            "CREATE INDEX IF NOT EXISTS idx_relevance ON social_media_data(relevant_to_education_in_uae);",
            "CREATE INDEX IF NOT EXISTS idx_sentiment_confidence ON social_media_data(sentiment_confidence);"
        ]
        
        for index in indexes:
            cursor.execute(index)
        
        # Create a trigger to update the updated_at timestamp
        trigger_query = """
        CREATE OR REPLACE FUNCTION update_updated_at_column()
        RETURNS TRIGGER AS $$
        BEGIN
            NEW.updated_at = CURRENT_TIMESTAMP;
            RETURN NEW;
        END;
        $$ language 'plpgsql';
        
        DROP TRIGGER IF EXISTS update_social_media_data_updated_at ON social_media_data;
        CREATE TRIGGER update_social_media_data_updated_at
            BEFORE UPDATE ON social_media_data
            FOR EACH ROW
            EXECUTE FUNCTION update_updated_at_column();
        """
        
        cursor.execute(trigger_query)
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info("Tables created successfully with updated schema")
        return True
        
    except Exception as e:
        logger.error(f"Error creating tables: {e}")
        return False

def setup_database():
    """Complete database setup"""
    logger.info("Starting database setup...")
    
    if create_database():
        if create_tables():
            logger.info("Database setup completed successfully!")
            return True
    
    logger.error("Database setup failed!")
    return False

if __name__ == "__main__":
    setup_database() 