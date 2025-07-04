import psycopg2
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import streamlit as st
from .config import DATABASE_CONFIG, DATABASE_URL
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self):
        self.engine = None
        self.connection = None
        
    def connect(self):
        """Establish database connection"""
        try:
            self.engine = create_engine(DATABASE_URL)
            self.connection = self.engine.connect()
            logger.info("Database connection established successfully")
            return True
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            st.error(f"Database connection failed: {e}")
            return False
    
    def disconnect(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
        if self.engine:
            self.engine.dispose()
        logger.info("Database connection closed")
    
    def execute_query(self, query, params=None):
        """Execute a query and return results"""
        try:
            if not self.connection:
                if not self.connect():
                    return None
            
            result = pd.read_sql(query, self.connection, params=params)
            return result
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            st.error(f"Query execution failed: {e}")
            return None
    
    def insert_data(self, df, table_name, if_exists='append'):
        """Insert DataFrame data into database table"""
        try:
            if not self.connection:
                if not self.connect():
                    return False
            
            df.to_sql(table_name, self.engine, if_exists=if_exists, index=False)
            logger.info(f"Data inserted successfully into {table_name}")
            return True
        except Exception as e:
            logger.error(f"Data insertion failed: {e}")
            st.error(f"Data insertion failed: {e}")
            return False
    
    def get_platform_data(self, platform=None, topic=None):
        """Get data filtered by platform and topic"""
        query = """
        SELECT * FROM social_media_data 
        WHERE 1=1
        """
        params = {}
        
        if platform and platform != 'All Platforms':
            query += " AND platform = %(platform)s"
            params['platform'] = platform
            
        if topic:
            query += " AND (title ILIKE %(topic)s OR content ILIKE %(topic)s OR summary ILIKE %(topic)s)"
            params['topic'] = f'%{topic}%'
        
        query += " ORDER BY date DESC"
        
        return self.execute_query(query, params)
    
    def get_sentiment_summary(self, platform=None, topic=None):
        """Get sentiment analysis summary"""
        query = """
        SELECT 
            sentiment_predicted,
            COUNT(*) as count,
            AVG(sentiment_confidence) as avg_confidence,
            platform
        FROM social_media_data 
        WHERE 1=1
        """
        params = {}
        
        if platform and platform != 'All Platforms':
            query += " AND platform = %(platform)s"
            params['platform'] = platform
            
        if topic:
            query += " AND (title ILIKE %(topic)s OR content ILIKE %(topic)s OR summary ILIKE %(topic)s)"
            params['topic'] = f'%{topic}%'
        
        query += " GROUP BY sentiment_predicted, platform ORDER BY count DESC"
        
        return self.execute_query(query, params)
    
    def get_platform_stats(self):
        """Get statistics by platform"""
        query = """
        SELECT 
            platform,
            COUNT(*) as total_posts,
            AVG(sentiment_confidence) as avg_confidence,
            COUNT(CASE WHEN sentiment_predicted = 'positive' THEN 1 END) as positive_count,
            COUNT(CASE WHEN sentiment_predicted = 'neutral' THEN 1 END) as neutral_count,
            COUNT(CASE WHEN sentiment_predicted = 'negative' THEN 1 END) as negative_count
        FROM social_media_data 
        GROUP BY platform
        ORDER BY total_posts DESC
        """
        
        return self.execute_query(query)
    
    def get_recent_posts(self, limit=10, platform=None):
        """Get recent posts"""
        query = """
        SELECT title, platform, sentiment_predicted, sentiment_confidence, date, url
        FROM social_media_data 
        WHERE 1=1
        """
        params = {}
        
        if platform and platform != 'All Platforms':
            query += " AND platform = %(platform)s"
            params['platform'] = platform
        
        query += " ORDER BY date DESC LIMIT %(limit)s"
        params['limit'] = limit
        
        return self.execute_query(query, params)
    
    def get_time_series_data(self, platform=None, days=30):
        """Get time series sentiment data"""
        query = """
        SELECT 
            DATE(date) as date,
            sentiment_predicted,
            COUNT(*) as count
        FROM social_media_data 
        WHERE date >= CURRENT_DATE - INTERVAL '%(days)s days'
        """
        params = {'days': days}
        
        if platform and platform != 'All Platforms':
            query += " AND platform = %(platform)s"
            params['platform'] = platform
        
        query += " GROUP BY DATE(date), sentiment_predicted ORDER BY date"
        
        return self.execute_query(query, params)

# Global database manager instance
db_manager = DatabaseManager()

# Utility functions for backward compatibility
@st.cache_data
def load_data_from_db(platform='All Platforms', topic='Education'):
    """Load data from database with caching"""
    return db_manager.get_platform_data(platform, topic)

def get_sentiment_stats(platform='All Platforms', topic='Education'):
    """Get sentiment statistics"""
    return db_manager.get_sentiment_summary(platform, topic)

def get_recent_posts_db(limit=10, platform='All Platforms'):
    """Get recent posts from database"""
    return db_manager.get_recent_posts(limit, platform) 