import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
import re
from datetime import datetime, timedelta
import os
from database.dashboard_db import DatabaseManager, load_data_from_db, get_sentiment_stats, get_recent_posts_db
from database.config import APP_CONFIG
from database.chatbot import render_chatbot_interface

# Set page config
st.set_page_config(
    page_title=APP_CONFIG['page_title'],
    page_icon=APP_CONFIG['page_icon'],
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS to match the dark theme from the screenshots
st.markdown("""
<style>
    /* Main background */
    .main {
        background-color: #1e2139;
    }
    
    /* Sidebar styling */
    .css-1d391kg {
        background-color: #252849;
    }
    
    /* Metric cards styling */
    [data-testid="metric-container"] {
        background-color: #2d3748;
        border: 1px solid #4a5568;
        padding: 1rem;
        border-radius: 10px;
        color: white;
    }
    
    /* Header styling */
    .dashboard-header {
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        padding: 2rem;
        border-radius: 15px;
        margin-bottom: 2rem;
        color: white;
    }
    
    /* Card styling */
    .sentiment-card {
        background-color: #2d3748;
        padding: 1.5rem;
        border-radius: 15px;
        border: 1px solid #4a5568;
        margin: 1rem 0;
    }
    
    /* Positive sentiment card */
    .positive-card {
        background: linear-gradient(135deg, #48bb78, #38a169);
        color: white;
    }
    
    /* Neutral sentiment card */
    .neutral-card {
        background: linear-gradient(135deg, #ed8936, #dd6b20);
        color: white;
    }
    
    /* Negative sentiment card */
    .negative-card {
        background: linear-gradient(135deg, #f56565, #e53e3e);
        color: white;
    }
    
    /* Database status */
    .db-status {
        padding: 0.5rem;
        border-radius: 5px;
        margin-bottom: 1rem;
    }
    
    .db-connected {
        background-color: #38a169;
        color: white;
    }
    
    .db-disconnected {
        background-color: #e53e3e;
        color: white;
    }
</style>
""", unsafe_allow_html=True)

# Initialize session state
if 'current_platform' not in st.session_state:
    st.session_state.current_platform = 'All Platforms'

# Initialize database manager
@st.cache_resource
def init_database():
    """Initialize database connection"""
    db_manager = DatabaseManager()
    return db_manager

def check_database_connection():
    """Check if database is connected"""
    try:
        db_manager = init_database()
        connected = db_manager.connect()
        return connected, db_manager
    except Exception as e:
        return False, None

def display_database_status():
    """Display database connection status"""
    connected, db_manager = check_database_connection()
    
    if connected:
        st.markdown("""
        <div class="db-status db-connected">
            ✅ PostgreSQL Database Connected
        </div>
        """, unsafe_allow_html=True)
        return True
    else:
        st.markdown("""
        <div class="db-status db-disconnected">
            ❌ Database Connection Failed - Please check your PostgreSQL setup
        </div>
        """, unsafe_allow_html=True)
        st.error("Please ensure PostgreSQL is running and database credentials are correct in .env file")
        return False

@st.cache_data(ttl=60)  # Cache for 60 seconds only
def load_data_from_database(platform='All Platforms', topic='Education'):
    """Load data from PostgreSQL database and clean HTML content"""
    try:
        db_manager = init_database()
        
        # Simple logic: 
        # - If "All Platforms" is selected, show ALL posts from ALL platforms (no filtering)
        # - If a specific platform is selected, show ALL posts from that platform
        
        if platform == 'All Platforms':
            # Show ALL posts from ALL platforms - no filtering at all
            df = db_manager.get_platform_data(platform=None, topic=None)
        else:
            # Specific platform selected - show all posts from that platform
            df = db_manager.get_platform_data(platform, topic=None)
            
        if df is None or df.empty:
            st.warning("No data found in database. Please run the migration script first.")
            return pd.DataFrame()
        
        # Convert date column if it exists
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
        
        # Clean HTML content from all text fields
        text_columns = ['title', 'content', 'summary', 'author']
        for col in text_columns:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: clean_text_for_display(x) if pd.notna(x) else '')
        
        # Special handling for comment field if it exists (for Reddit/Quora)
        if 'comment' in df.columns:
            df['comment'] = df['comment'].apply(lambda x: clean_text_for_display(x) if pd.notna(x) else '')
        
        return df
    
    except Exception as e:
        st.error(f"Error loading data from database: {e}")
        return pd.DataFrame()

def create_sidebar():
    """Create sidebar with navigation and filters"""
    with st.sidebar:
        st.markdown("""
        <div style="text-align: center; padding: 1rem;">
            <h1 style="color: #667eea; font-size: 2rem; margin-bottom: 0;">📊 Socio-Economic Platform</h1>
            <p style="color: #a0aec0; margin-top: 0;">Sentiment Analytics (PostgreSQL)</p>
        </div>
        """, unsafe_allow_html=True)
        
        # Database status
        db_connected = display_database_status()
        
        st.markdown("---")
        
        # Navigation
        pages = ["📊 Dashboard", "📝 Posts", "📈 Analytics", "👥 Audience", "🤖 AI Assistant", "⚙️ Settings"]
        selected_page = st.selectbox("Navigation", pages, index=0)
        
        st.markdown("---")
        
        # Platform filter
        platforms = ['All Platforms', 'Khaleej Times', 'LinkedIn', 'Reddit', 'Quora', 'Reuters']
        if 'current_platform' not in st.session_state:
            st.session_state.current_platform = 'All Platforms'
        
        selected_platform = st.selectbox("Platform:", platforms, 
                                        index=platforms.index(st.session_state.current_platform))
        st.session_state.current_platform = selected_platform
        
        # Show appropriate message based on selection
        if selected_platform == 'All Platforms':
            st.success("📄 Showing ALL posts from ALL platforms")
            
            # Show total count across all platforms
            try:
                db_manager = init_database()
                total_query = "SELECT COUNT(*) as count FROM social_media_data"
                result = db_manager.execute_query(total_query)
                if result is not None and not result.empty:
                    count = result.iloc[0]['count']
                    st.metric("Total Posts", f"{count:,}")
            except:
                pass  # Ignore errors in quick stats
        else:
            # For specific platforms, show info about what will be displayed
            st.success(f"📄 Showing ALL {selected_platform} posts")
            
            # Show quick stats for the selected platform
            try:
                db_manager = init_database()
                platform_query = f"SELECT COUNT(*) as count FROM social_media_data WHERE platform = '{selected_platform}'"
                result = db_manager.execute_query(platform_query)
                if result is not None and not result.empty:
                    count = result.iloc[0]['count']
                    st.metric("Total Posts", f"{count:,}")
            except:
                pass  # Ignore errors in quick stats
        
        # Topic filter is removed completely - no topic filtering anywhere
        selected_topic = 'Education'  # Default value, but not used for filtering
        
        st.markdown("---")
        
        # Database actions
        if db_connected:
            st.markdown("**Database Actions**")
            
            if st.button("🔄 Refresh Data"):
                st.cache_data.clear()
                st.rerun()
                
            if st.button("📊 Show Platform Breakdown"):
                try:
                    db_manager = init_database()
                    stats = db_manager.get_platform_stats()
                    if stats is not None and not stats.empty:
                        st.write("**Platform Statistics:**")
                        st.dataframe(stats, use_container_width=True)
                except Exception as e:
                    st.error(f"Error getting stats: {e}")
        
        return selected_page, selected_platform, selected_topic, db_connected

def create_header(total_posts, platform, topic):
    """Create dashboard header"""
    # Simple header display
    if platform == 'All Platforms':
        content_display = f"All platforms filtered by {topic}"
        filter_display = f"{topic} Topic"
    else:
        content_display = f"All posts from {platform}"
        filter_display = "All Content"
    
    st.markdown(f"""
    <div class="dashboard-header">
        <h1 style="margin: 0; font-size: 2.5rem;">Sentiment Analysis Dashboard</h1>
        <p style="margin: 0.5rem 0 0 0; font-size: 1.2rem; opacity: 0.9;">
            {content_display} (PostgreSQL)
        </p>
    </div>
    """, unsafe_allow_html=True)
    
    # Platform and user info
    col1, col2 = st.columns([3, 1])
    with col2:
        st.markdown(f"""
        <div style="text-align: right; padding: 1rem;">
            <p style="color: #a0aec0; margin: 0;">Content: <strong style="color: white;">{filter_display}</strong></p>
            <p style="color: #a0aec0; margin: 0;">Platform: <strong style="color: white;">{platform}</strong></p>
            <p style="color: #a0aec0; margin: 0;">Total Posts: <strong style="color: white;">{total_posts:,}</strong></p>
        </div>
        """, unsafe_allow_html=True)

def create_sentiment_cards(df):
    """Create sentiment summary cards"""
    if df.empty:
        st.warning("No data available for sentiment analysis")
        return
    
    # Calculate sentiment distribution
    sentiment_counts = df['sentiment_predicted'].value_counts()
    total_posts = len(df)
    
    # Get sentiment percentages
    positive_pct = (sentiment_counts.get('positive', 0) / total_posts * 100) if total_posts > 0 else 0
    neutral_pct = (sentiment_counts.get('neutral', 0) / total_posts * 100) if total_posts > 0 else 0
    negative_pct = (sentiment_counts.get('negative', 0) / total_posts * 100) if total_posts > 0 else 0
    
    # Create three columns for sentiment cards
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown(f"""
        <div class="sentiment-card positive-card">
            <h3 style="margin: 0; font-size: 1.2rem;">Positive Sentiment</h3>
            <h2 style="margin: 0.5rem 0; font-size: 2.5rem;">{positive_pct:.1f}%</h2>
            <p style="margin: 0; opacity: 0.9;">{sentiment_counts.get('positive', 0):,} posts</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown(f"""
        <div class="sentiment-card neutral-card">
            <h3 style="margin: 0; font-size: 1.2rem;">Neutral Sentiment</h3>
            <h2 style="margin: 0.5rem 0; font-size: 2.5rem;">{neutral_pct:.1f}%</h2>
            <p style="margin: 0; opacity: 0.9;">{sentiment_counts.get('neutral', 0):,} posts</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown(f"""
        <div class="sentiment-card negative-card">
            <h3 style="margin: 0; font-size: 1.2rem;">Negative Sentiment</h3>
            <h2 style="margin: 0.5rem 0; font-size: 2.5rem;">{negative_pct:.1f}%</h2>
            <p style="margin: 0; opacity: 0.9;">{sentiment_counts.get('negative', 0):,} posts</p>
        </div>
        """, unsafe_allow_html=True)

def create_charts(df):
    """Create sentiment analysis charts"""
    if df.empty:
        st.warning("No data available for charts")
        return
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📊 Sentiment Distribution")
        
        # Pie chart for sentiment distribution
        sentiment_counts = df['sentiment_predicted'].value_counts()
        
        colors = {
            'positive': '#48bb78',
            'neutral': '#ed8936', 
            'negative': '#f56565'
        }
        
        fig_pie = px.pie(
            values=sentiment_counts.values,
            names=sentiment_counts.index,
            color=sentiment_counts.index,
            color_discrete_map=colors,
            title="Overall Sentiment Distribution"
        )
        
        fig_pie.update_layout(
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            font_color='white'
        )
        
        st.plotly_chart(fig_pie, use_container_width=True)
    
    with col2:
        st.subheader("📈 Platform Comparison")
        
        # Bar chart by platform
        platform_sentiment = df.groupby(['platform', 'sentiment_predicted']).size().unstack(fill_value=0)
        
        fig_bar = px.bar(
            platform_sentiment,
            title="Sentiment by Platform",
            color_discrete_map=colors
        )
        
        fig_bar.update_layout(
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            font_color='white'
        )
        
        st.plotly_chart(fig_bar, use_container_width=True)
    
    # Time series chart if date data is available
    if 'date' in df.columns and not df['date'].isna().all():
        st.subheader("📅 Sentiment Over Time")
        
        # Group by date and sentiment
        daily_sentiment = df.groupby([df['date'].dt.date, 'sentiment_predicted']).size().unstack(fill_value=0)
        
        fig_time = px.line(
            daily_sentiment,
            title="Sentiment Trends Over Time",
            color_discrete_map=colors
        )
        
        fig_time.update_layout(
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            font_color='white'
        )
        
        st.plotly_chart(fig_time, use_container_width=True)

def clean_linkedin_title(text):
    """Clean LinkedIn titles by removing leading symbols and unwanted characters"""
    if not text or str(text).strip() == 'nan' or str(text).strip() == '':
        return 'LinkedIn Post'
    
    # Convert to string and strip whitespace
    clean_text = str(text).strip()
    
    # Enhanced cleaning: Remove leading symbols more aggressively
    # Include more symbols and handle them iteratively
    unwanted_symbols = ['?', '#', '!', '@', '*', '+', '-', '=', '|', '\\', '/', '^', '~', '`']
    
    # Keep removing leading symbols until we get to actual text
    original_length = len(clean_text)
    while clean_text and clean_text[0] in unwanted_symbols:
        clean_text = clean_text[1:].strip()
        # Prevent infinite loop
        if len(clean_text) >= original_length:
            break
        original_length = len(clean_text)
    
    # Additional pass for emoji and special characters at the start
    # Remove common problematic characters
    while clean_text and ord(clean_text[0]) < 48:  # Remove chars before '0' in ASCII
        clean_text = clean_text[1:].strip()
        if not clean_text:
            break
    
    # If text becomes empty after cleaning, use default
    if not clean_text or len(clean_text.strip()) == 0:
        return 'LinkedIn Post'
    
    # Ensure first character is alphanumeric or common punctuation
    if clean_text and not (clean_text[0].isalnum() or clean_text[0] in ['"', "'", '(', '[']):
        # Find first alphanumeric character
        for i, char in enumerate(clean_text):
            if char.isalnum():
                clean_text = clean_text[i:].strip()
                break
        else:
            # No alphanumeric character found
            return 'LinkedIn Post'
    
    # Final check if text is still empty
    if not clean_text or len(clean_text.strip()) == 0:
        return 'LinkedIn Post'
    
    # Capitalize first letter
    clean_text = clean_text[0].upper() + clean_text[1:] if len(clean_text) > 1 else clean_text.upper()
    
    # Truncate and clean ending punctuation
    if len(clean_text) > 100:
        truncated = clean_text[:100].rstrip('.,!?;: ')
        return truncated + '...'
    else:
        return clean_text.rstrip('.,!?;: ')

def clean_text_for_display(text):
    """Clean text for display by removing HTML tags and special characters"""
    if not text or str(text).strip() == '':
        return ""
    
    text_str = str(text)
    
    # First pass: Remove obvious HTML tags (including standalone closing tags)
    text_str = re.sub(r'<[^>]*>', '', text_str)
    text_str = re.sub(r'<[^>]*$', '', text_str)  # Remove incomplete tags at end
    text_str = re.sub(r'^[^<]*>', '', text_str)  # Remove incomplete tags at start
    
    # Specifically handle common standalone HTML artifacts
    text_str = re.sub(r'</p>\s*', ' ', text_str)  # Remove </p> tags
    text_str = re.sub(r'</div>\s*', ' ', text_str)  # Remove </div> tags
    text_str = re.sub(r'</span>\s*', ' ', text_str)  # Remove </span> tags
    
    # Replace HTML entities
    html_entities = {
        '&lt;': '<', '&gt;': '>', '&amp;': '&', '&quot;': '"', 
        '&#x27;': "'", '&nbsp;': ' ', '&#39;': "'", '&apos;': "'",
        '&ldquo;': '"', '&rdquo;': '"', '&lsquo;': "'", '&rsquo;': "'",
        '&hellip;': '...', '&mdash;': '—', '&ndash;': '–', '&copy;': '©',
        '&reg;': '®', '&trade;': '™', '&deg;': '°', '&plusmn;': '±'
    }
    
    for entity, replacement in html_entities.items():
        text_str = text_str.replace(entity, replacement)
    
    # Remove any remaining HTML entities
    text_str = re.sub(r'&[a-zA-Z0-9#]+;', '', text_str)
    
    # Remove CSS style attributes and other HTML attributes
    text_str = re.sub(r'style\s*=\s*["\'][^"\']*["\']', '', text_str)
    text_str = re.sub(r'\w+\s*=\s*["\'][^"\']*["\']', '', text_str)
    
    # If the text still contains HTML-like content, do aggressive cleaning
    if '<' in text_str or '>' in text_str or 'style=' in text_str.lower():
        # Split by angle brackets and keep only text content
        parts = re.split(r'[<>]', text_str)
        clean_parts = []
        
        for part in parts:
            part = part.strip()
            # Skip empty parts and HTML tag-like content
            if not part:
                continue
            if re.match(r'^/?[a-zA-Z][a-zA-Z0-9]*(\s|$)', part):  # Looks like a tag
                continue
            if 'style=' in part.lower() or 'class=' in part.lower():  # Contains attributes
                continue
            if part.count('=') > 1 and part.count('"') > 1:  # Looks like attributes
                continue
                
            clean_parts.append(part)
        
        text_str = ' '.join(clean_parts)
    
    # Final cleanup
    text_str = re.sub(r'\s+', ' ', text_str)
    text_str = re.sub(r'[\r\n\t]+', ' ', text_str)
    text_str = text_str.strip()
    
    # Remove any remaining problematic characters that might be HTML artifacts
    text_str = text_str.replace('nan', '').strip()
    
    return text_str

def merge_linkedin_posts_by_post_text(df):
    """Group LinkedIn posts by title (post_text) and collect comments for each post"""
    if df.empty:
        return df
    
    try:
        # For LinkedIn data from database:
        # - title contains the cleaned post_text (the main LinkedIn post)
        # - content contains the comment_text (comments on the post)
        
        merged_posts = []
        
        # Group by title (which contains the main post text)
        grouped = df.groupby('title')
        
        for post_title, group in grouped:
            # Get the first entry as the main post
            main_post = group.iloc[0].copy()
            
            # Collect all non-null comments for this post
            comments = []
            for _, row in group.iterrows():
                comment = row.get('content', '')  # content field contains comment_text
                if pd.notna(comment) and str(comment).strip() and str(comment).strip().lower() != 'nan':
                    clean_comment = clean_text_for_display(str(comment))
                    if clean_comment and len(clean_comment.strip()) > 5:
                        comments.append(clean_comment)
            
            # Create the merged post structure
            merged_post = {
                'title': post_title,  # Already cleaned title
                'content': post_title,  # Use the post title as the main content
                'platform': 'LinkedIn',
                'comments': comments,  # List of comments
                'comment_count': len(comments),
                'has_comments': len(comments) > 0,
                'sentiment_predicted': main_post.get('sentiment_predicted', 'neutral'),
                'sentiment_confidence': main_post.get('sentiment_confidence', 0.0),
                'date': main_post.get('date'),
                'relevance_score': main_post.get('relevance_score', 0.0),
                'relevant_to_education_in_uae': main_post.get('relevant_to_education_in_uae', False),
                'sentiment_negative': main_post.get('sentiment_negative', 0.0),
                'sentiment_neutral': main_post.get('sentiment_neutral', 0.0),
                'sentiment_positive': main_post.get('sentiment_positive', 0.0),
                'url': main_post.get('url', ''),
                'author': main_post.get('author', ''),
                'summary': main_post.get('summary', ''),
                'combined_text': main_post.get('combined_text', ''),
                'is_merged': True  # Flag to indicate this is a merged post
            }
            
            merged_posts.append(merged_post)
        
        merged_df = pd.DataFrame(merged_posts)
        
        # Restore the original index structure
        if not merged_df.empty:
            merged_df.index = range(len(merged_df))
        
        return merged_df
        
    except Exception as e:
        st.error(f"Error processing LinkedIn posts: {e}")
        # Fallback to original dataframe
        return df

def merge_duplicate_posts(df, platform):
    """Merge posts with same title for LinkedIn only (Quora handled separately)"""
    if platform not in ['LinkedIn'] or df.empty:
        return df
    
    # For LinkedIn, use the special post_text grouping
    return merge_linkedin_posts_by_post_text(df)

def create_recent_posts(df):
    """Create recent posts section with pagination"""
    if df.empty:
        st.warning("No posts available")
        return
    
    st.subheader("📝 All Posts")
    
    # Initialize session state for pagination
    if 'posts_per_page' not in st.session_state:
        st.session_state.posts_per_page = 20
    if 'current_page' not in st.session_state:
        st.session_state.current_page = 1
    
    # Sort by date if available, otherwise by index
    if 'date' in df.columns:
        sorted_posts = df.sort_values('date', ascending=False)
    else:
        sorted_posts = df.copy()
    
    total_posts = len(sorted_posts)
    total_pages = (total_posts + st.session_state.posts_per_page - 1) // st.session_state.posts_per_page
    
    # Posts per page selector and pagination info
    col1, col2 = st.columns([1, 3])
    with col1:
        posts_per_page_options = [10, 20, 50, 100]
        new_posts_per_page = st.selectbox("Posts per page:", posts_per_page_options, 
                                         index=posts_per_page_options.index(st.session_state.posts_per_page))
        if new_posts_per_page != st.session_state.posts_per_page:
            st.session_state.posts_per_page = new_posts_per_page
            st.session_state.current_page = 1  # Reset to first page
            st.rerun()
    
    with col2:
        st.write(f"📊 Showing {min(st.session_state.current_page * st.session_state.posts_per_page, total_posts)} of {total_posts} posts")
    
    # Get posts for current page
    start_idx = (st.session_state.current_page - 1) * st.session_state.posts_per_page
    end_idx = start_idx + st.session_state.posts_per_page
    recent_posts = sorted_posts.iloc[start_idx:end_idx]
    
    # Check if we need to merge duplicate titles (only LinkedIn, Quora handled separately)
    platform = recent_posts['platform'].iloc[0] if not recent_posts.empty else None
    if platform in ['LinkedIn']:
        recent_posts = merge_duplicate_posts(recent_posts, platform)
    
    for idx, post in recent_posts.iterrows():
        sentiment_color = {
            'positive': '#48bb78',
            'neutral': '#ed8936',
            'negative': '#f56565'
        }.get(post.get('sentiment_predicted', 'neutral'), '#ed8936')
        
        confidence = post.get('sentiment_confidence', 0) * 100 if pd.notna(post.get('sentiment_confidence')) else 0
        
        # Check if this is a platform with comments (Reddit or Quora)
        has_comment = post.get('comment', '') and str(post.get('comment', '')).strip() != ''
        platform = post.get('platform', 'Unknown')
        
        # Build the post display with proper text cleaning
        # For recent posts, treat all platforms the same - just show title and content/summary
        # Don't show complex comment processing here to avoid HTML issues
        
        # Clean title and content with platform-specific logic
        is_merged = post.get('is_merged', False)
        
        if platform == 'Reddit':
            # For Reddit: use title (question) and show response status
            raw_title = post.get('title', 'No Title')
            clean_title = clean_text_for_display(raw_title)
            
            # Check if this question has a response
            response = post.get('comment', '')
            if response and str(response).strip() and str(response).strip().lower() != 'nan':
                clean_content = "💬 1 response available - Click to view question and response"
            else:
                clean_content = "💭 No responses yet - Click to view question"
            
        elif platform == 'Quora':
            # For Quora: use title and show answer count
            raw_title = post.get('title', 'No Title')
            clean_title = clean_text_for_display(raw_title)
            
            # Get answer count for this question from database
            from database.dashboard_db import DatabaseManager
            db = DatabaseManager()
            answer_count = 1  # Default to 1
            
            if db.connect():
                count_query = """
                SELECT COUNT(*) as count 
                FROM social_media_data 
                WHERE platform = 'Quora' AND title = %(title)s
                """
                try:
                    result = db.execute_query(count_query, {'title': raw_title})
                    if result is not None and not result.empty:
                        total_count = result.iloc[0]['count']
                        # Subtract 1 because first answer contains question merged with answer
                        answer_count = max(0, total_count - 1)
                except:
                    pass
                db.disconnect()
            
            if answer_count > 0:
                clean_content = f"📚 {answer_count} answers available - Click to view all"
            else:
                raw_content = post.get('content', 'No answer available')
                if raw_content and str(raw_content).strip() and str(raw_content).strip().lower() != 'nan':
                    clean_content = clean_text_for_display(raw_content)
                    clean_content = clean_quora_comment(clean_content, clean_title)
                else:
                    clean_content = 'No answer available'
                
        elif platform == 'LinkedIn':
            # For LinkedIn: use post_text as title and show comment count
            raw_title = post.get('title', 'No Title')
            clean_title = clean_linkedin_title(raw_title)  # Use LinkedIn-specific cleaning
            
            # Check if this post has comments
            comment_count = post.get('comment_count', 0)
            if comment_count > 0:
                clean_content = f"💬 {comment_count} comments - Click to view post and all comments"
            else:
                clean_content = "📝 Click to view post content"
            
        else:
            # For other platforms (Khaleej Times, Reuters): use title and summary/content
            raw_title = post.get('title', 'No Title')
            clean_title = clean_text_for_display(raw_title)
            
            raw_content = post.get('summary', post.get('content', 'No content available'))
            clean_content = clean_text_for_display(raw_content)
        
        # Truncate title for display
        if len(clean_title) > 100:
            clean_title = clean_title[:100] + "..."
        
        # Truncate content for display (only if not merged)
        if not is_merged and len(clean_content) > 200:
            clean_content = clean_content[:200] + "..."
        
        # Create unique key for each post
        post_key = f"post_{idx}_{hash(str(clean_title))}"
        
        # Create clickable post card
        with st.container():
            if st.button(f"📖 Read Full Post", key=f"btn_{post_key}", help="Click to view full post details"):
                st.session_state[f'show_detail_{post_key}'] = True
            
            st.markdown(f"""
            <div style="
                background-color: #2d3748;
                border-left: 4px solid {sentiment_color};
                padding: 1rem;
                margin: 1rem 0;
                border-radius: 5px;
                cursor: pointer;
            ">
                <h4 style="margin: 0 0 0.5rem 0; color: white;">{clean_title}</h4>
                <p style="margin: 0.5rem 0; color: #a0aec0; font-size: 0.9rem;">
                    <strong>Platform:</strong> {platform} | 
                    <strong>Sentiment:</strong> {post.get('sentiment_predicted', 'Unknown').title()} ({confidence:.1f}%) |
                    <strong>Date:</strong> {post.get('date', 'Unknown')}
                </p>
                <p style="margin: 0; color: #e2e8f0; font-size: 0.9rem;">
                    {clean_content}
                </p>
            </div>
            """, unsafe_allow_html=True)
            
            # Show detailed view if button was clicked
            if st.session_state.get(f'show_detail_{post_key}', False):
                show_post_detail(post, platform, clean_title, clean_content)
                if st.button("❌ Close", key=f"close_{post_key}"):
                    st.session_state[f'show_detail_{post_key}'] = False
                    st.rerun()
    
    # Add pagination controls
    st.markdown("---")
    col1, col2, col3, col4, col5 = st.columns([1, 1, 2, 1, 1])
    
    with col1:
        if st.button("⬅️ Previous", disabled=st.session_state.current_page <= 1):
            st.session_state.current_page -= 1
            st.rerun()
    
    with col2:
        if st.button("⏮️ First", disabled=st.session_state.current_page <= 1):
            st.session_state.current_page = 1
            st.rerun()
    
    with col3:
        st.write(f"📄 Page {st.session_state.current_page} of {total_pages}")
    
    with col4:
        if st.button("⏭️ Last", disabled=st.session_state.current_page >= total_pages):
            st.session_state.current_page = total_pages
            st.rerun()
    
    with col5:
        if st.button("➡️ Next", disabled=st.session_state.current_page >= total_pages):
            st.session_state.current_page += 1
            st.rerun()
    
    # Add "Load More" button for infinite scroll-like experience
    if st.session_state.current_page < total_pages:
        col_center = st.columns([1, 2, 1])[1]
        with col_center:
            if st.button("📖 Load More Posts", key="load_more"):
                st.session_state.current_page += 1
                st.rerun()

def show_post_detail(post, platform, clean_title, full_content):
    """Show detailed view of a single post"""
    
    # Create detailed view
    st.markdown("---")
    st.markdown("### 📖 Full Post Details")
    
    # Show metadata
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Platform", platform)
    with col2:
        sentiment = post.get('sentiment_predicted', 'Unknown').title()
        confidence = post.get('sentiment_confidence', 0) * 100 if pd.notna(post.get('sentiment_confidence')) else 0
        st.metric("Sentiment", f"{sentiment} ({confidence:.1f}%)")
    with col3:
        st.metric("Date", str(post.get('date', 'Unknown')))
    
    # Show full title (not truncated) in post details
    original_title = post.get('title', 'No Title')
    platform_name = post.get('platform', '')
    if platform_name == 'LinkedIn':
        full_clean_title = clean_linkedin_title(original_title)
    else:
        full_clean_title = clean_text_for_display(original_title)
    st.markdown(f"**Title:** {full_clean_title}")
    
    # Show content - check if this is a merged post
    is_merged = post.get('is_merged', False)
    
    if platform == 'Quora':
        # Special handling for Quora - show question details and all answers
        from database.dashboard_db import DatabaseManager
        
        # Show question details if available
        question_details = post.get('summary', '')
        if question_details and str(question_details).strip() and str(question_details).strip().lower() not in ['nan', '']:
            st.markdown("**Question Details:**")
            clean_details = clean_text_for_display(str(question_details))
            st.write(clean_details)
            st.markdown("---")
        
        db = DatabaseManager()
        if db.connect():
            # Get all unique answers for this Quora question
            query = """
            SELECT DISTINCT content, sentiment_predicted, date, url, summary
            FROM social_media_data 
            WHERE platform = 'Quora' AND title = %(title)s
            ORDER BY date DESC
            """
            # Use the original full title for the query, not the truncated one
            original_title = post.get('title', 'No Title')
            answers = db.execute_query(query, {'title': original_title})
            
            if answers is not None and not answers.empty:
                # Skip the first answer as it contains the question merged with answer
                actual_answers = answers.iloc[1:] if len(answers) > 1 else pd.DataFrame()
                
                if not actual_answers.empty:
                    # Further filter to remove duplicate content
                    unique_answers = []
                    seen_content = set()
                    
                    for i, answer_row in actual_answers.iterrows():
                        answer_content = answer_row.get('content', 'No answer available')
                        if answer_content and str(answer_content).strip() and str(answer_content).strip().lower() != 'nan':
                            # Create a short hash of the content to check for duplicates
                            content_hash = str(answer_content).strip()[:100].lower()
                            if content_hash not in seen_content:
                                seen_content.add(content_hash)
                                unique_answers.append(answer_row)
                    
                    if unique_answers:
                        st.markdown(f"**📚 {len(unique_answers)} Answer(s):**")
                        for answer_num, answer_row in enumerate(unique_answers, 1):
                            answer_content = answer_row.get('content', 'No answer available')
                            answer_sentiment = answer_row.get('sentiment_predicted', 'Unknown')
                            answer_date = answer_row.get('date', 'Unknown')
                            
                            clean_answer = clean_text_for_display(str(answer_content))
                            # Don't call clean_quora_comment to avoid duplication
                            
                            # Create expandable box for each unique answer
                            with st.expander(f"💡 Answer {answer_num} - {answer_sentiment.title()} ({answer_date})", expanded=answer_num==1):
                                st.markdown(clean_answer)
                                if answer_row.get('url'):
                                    st.markdown(f"[View on Quora]({answer_row.get('url')})")
                    else:
                        st.markdown("**No unique answers available for this question.**")
                else:
                    st.markdown("**No clean answers available for this question.**")
            else:
                # Fallback to single answer
                single_answer = post.get('content', 'No answer available')
                if single_answer and str(single_answer).strip():
                    clean_answer = clean_text_for_display(str(single_answer))
                    st.markdown("**Answer:**")
                    st.write(clean_answer)
            
            db.disconnect()
            
    elif platform == 'Reddit':
        # Special handling for Reddit posts with responses
        st.markdown("**❓ Reddit Question:**")
        
        # Show the main question content
        question_content = post.get('content', 'No content available')
        if question_content and str(question_content).strip():
            clean_question_content = clean_text_for_display(str(question_content))
            st.markdown(clean_question_content)
            
            # Show main post sentiment (could be question or comment sentiment)
            main_sentiment = post.get('sentiment_predicted', '')
            if main_sentiment:
                sentiment_color = {
                    'positive': '#48bb78',
                    'neutral': '#ed8936', 
                    'negative': '#f56565'
                }.get(main_sentiment, '#ed8936')
                confidence = post.get('sentiment_confidence', 0) * 100 if pd.notna(post.get('sentiment_confidence')) else 0
                st.markdown(f"<span style='color: {sentiment_color}'>📊 Post Sentiment: {main_sentiment} ({confidence:.1f}%)</span>", unsafe_allow_html=True)
        
        # Show response if it exists (expandable like Quora)
        response = post.get('comment', '')
        if response and str(response).strip() and str(response).strip().lower() != 'nan':
            response_sentiment = post.get('comment_sentiment', '')
            response_date = post.get('date', 'Unknown')
            
            # Create expandable response like Quora answers
            st.markdown("**💬 Response:**")
            
            # Create sentiment display for expander title
            sentiment_display = f" - {response_sentiment.title()}" if response_sentiment and response_sentiment != main_sentiment else ""
            
            with st.expander(f"💬 Community Response{sentiment_display} ({response_date})", expanded=True):
                clean_response = clean_text_for_display(str(response))
                st.markdown(clean_response)
                
                # Show response sentiment if available (separate from main sentiment)
                if response_sentiment and response_sentiment != main_sentiment:
                    sentiment_color = {
                        'positive': '#48bb78',
                        'neutral': '#ed8936', 
                        'negative': '#f56565'
                    }.get(response_sentiment, '#ed8936')
                    st.markdown(f"<span style='color: {sentiment_color}'>📊 Response Sentiment: {response_sentiment}</span>", unsafe_allow_html=True)
        else:
            st.info("💭 No responses to this question yet")
            
    elif platform == 'LinkedIn':
        # Special handling for LinkedIn posts with comments
        st.markdown("**📝 Post Content:**")
        
        # Show the main post content
        post_content = post.get('content', 'No content available')
        if post_content and str(post_content).strip():
            clean_post_content = clean_text_for_display(str(post_content))
            st.markdown(clean_post_content)
        
        # Show comments if they exist
        comments = post.get('comments', [])
        if comments:
            st.markdown(f"**💬 Comments ({len(comments)}):**")
            for i, comment in enumerate(comments, 1):
                with st.expander(f"💬 Comment {i}", expanded=i<=3):  # Expand first 3 comments
                    st.markdown(comment)
        else:
            st.info("No comments on this post")
            
    elif is_merged and 'merged_content' in post:
        # Show each content item in a separate box for other platforms
        merged_content = post['merged_content']
        
        st.markdown(f"**💬 {len(merged_content)} Posts:**")
        for i, content in enumerate(merged_content, 1):
            with st.expander(f"📝 Post {i}", expanded=i==1):  # Expand first post by default
                st.markdown(content)
    else:
        # Show single content based on platform
        st.markdown("**Content:**")
        
        if platform == 'Reddit':
            full_content = post.get('content', 'No content available')  # Use the improved combined content
        else:
            full_content = post.get('summary', post.get('content', 'No content available'))
        
        # Clean and display content
        full_content = clean_text_for_display(str(full_content))
        st.write(full_content)
    
    # Show additional fields if available
    if platform == 'Quora' and post.get('url'):
        st.markdown(f"**Source:** [View on Quora]({post.get('url')})")
    elif platform == 'LinkedIn' and post.get('url'):
        st.markdown(f"**Source:** [View on LinkedIn]({post.get('url')})")
    elif post.get('url'):
        st.markdown(f"**Source:** [View Original]({post.get('url')})")
    
    st.markdown("---")

def clean_quora_comment(comment_text, question_text):
    """
    Clean Quora comment to remove question duplication and HTML content
    Args:
        comment_text: The comment/answer text 
        question_text: The question text to remove if present
    Returns:
        Cleaned comment text with just the answer
    """
    if not comment_text or str(comment_text).strip() == '':
        return 'No answer available'
    
    # First clean HTML from both comment and question
    comment_str = clean_text_for_display(str(comment_text))
    question_str = clean_text_for_display(str(question_text))
    
    # If the comment starts with the question, remove it
    if comment_str.lower().startswith(question_str.lower()):
        # Remove the question and any trailing punctuation/whitespace
        cleaned = comment_str[len(question_str):].strip()
        # Remove leading punctuation like "? " or ": "
        cleaned = cleaned.lstrip('?:. ')
        return cleaned if cleaned else 'No answer available'
    
    # Also check if the question is embedded within the comment
    # This handles cases where HTML formatting might have altered the structure
    question_words = question_str.lower().split()[:5]  # First 5 words of question
    if len(question_words) > 2:
        question_start = ' '.join(question_words)
        comment_lower = comment_str.lower()
        
        # Find the question within the comment
        question_index = comment_lower.find(question_start)
        if question_index != -1:
            # Find the end of the question (look for punctuation)
            search_start = question_index + len(question_start)
            punctuation_chars = ['?', '.', '!', ':']
            end_index = -1
            
            for char in punctuation_chars:
                char_index = comment_str.find(char, search_start)
                if char_index != -1 and (end_index == -1 or char_index < end_index):
                    end_index = char_index
            
            if end_index != -1:
                # Extract the answer part after the question
                answer = comment_str[end_index + 1:].strip()
                return answer if answer else 'No answer available'
    
    return comment_str

def prepare_posts_dataframe(df):
    """
    Prepare dataframe for Posts page display with cleaned comments
    """
    if df.empty:
        return df
    
    display_df = df.copy()
    
    # For Quora posts, clean up the comment field to remove question duplication
    if 'platform' in display_df.columns and 'comment' in display_df.columns and 'title' in display_df.columns:
        quora_mask = display_df['platform'] == 'Quora'
        if quora_mask.any():
            # Clean Quora comments
            display_df.loc[quora_mask, 'comment'] = display_df.loc[quora_mask].apply(
                lambda row: clean_quora_comment(row['comment'], row['title']), 
                axis=1
            )
    
    return display_df

def main():
    """Main dashboard function"""
    # Create sidebar and get selections
    selected_page, platform, topic, db_connected = create_sidebar()
    
    if not db_connected:
        st.error("Database connection required. Please check your PostgreSQL setup.")
        st.markdown("""
        ### Setup Instructions:
        1. Ensure PostgreSQL is installed and running
        2. Update the `.env` file with your database credentials
        3. Run the setup script: `python setup_database.py`
        4. Run the migration script: `python migrate_data.py`
        """)
        return
    
    if selected_page == "📊 Dashboard":
        # Load data from database
        df = load_data_from_database(platform, topic)
        
        if df.empty:
            st.warning("No data found. Please run the migration script to import your CSV/Excel data.")
            st.code("python migrate_data.py", language="bash")
            return
        
        # Additional cleaning for Quora data to ensure HTML is removed
        if 'comment' in df.columns:
            quora_mask = df['platform'] == 'Quora'
            if quora_mask.any():
                # Apply aggressive cleaning to Quora comments
                df.loc[quora_mask, 'comment'] = df.loc[quora_mask, 'comment'].apply(
                    lambda x: clean_text_for_display(str(x)) if pd.notna(x) and str(x).strip() != '' else ''
                )
        
        # Create header
        create_header(len(df), platform, topic)
        
        # Create sentiment cards
        create_sentiment_cards(df)
        
        # Create charts
        create_charts(df)
        
        # Create recent posts
        create_recent_posts(df)
        
    elif selected_page == "📝 Posts":
        st.header("📝 All Posts")
        
        # Posts page has its own platform dropdown
        col1, col2 = st.columns(2)
        
        with col1:
            # Platform selection dropdown for Posts page
            platforms = ['All Platforms', 'Khaleej Times', 'LinkedIn', 'Reddit', 'Quora', 'Reuters']
            posts_platform = st.selectbox("Select Platform:", platforms, key="posts_platform")
        
        with col2:
            # Optional sentiment filter
            sentiment_filter = st.selectbox("Filter by Sentiment (Optional)", 
                                          ['All', 'positive', 'neutral', 'negative'])
        
        # Load data based on Posts page platform selection
        df = load_data_from_database(posts_platform, topic)
        
        if not df.empty:
            # Show platform info
            if posts_platform == 'All Platforms':
                st.success(f"📄 Showing all {len(df):,} posts from all platforms")
                
                # Show platform breakdown for All Platforms
                if 'platform' in df.columns:
                    platform_counts = df['platform'].value_counts()
                    st.write("**Platform Breakdown:**")
                    for plt, count in platform_counts.items():
                        st.write(f"• {plt}: {count:,} posts")
            else:
                st.success(f"📄 Showing all {len(df):,} posts from {posts_platform}")
            
            # Apply sentiment filter if selected
            filtered_df = df.copy()
            
            if sentiment_filter != 'All':
                filtered_df = filtered_df[filtered_df['sentiment_predicted'] == sentiment_filter]
                st.info(f"📊 Filtered to {len(filtered_df):,} {sentiment_filter} posts")
            
            # Initialize pagination for Posts page
            posts_page_key = f"posts_page_{posts_platform}_{sentiment_filter}"
            if f'posts_current_page_{posts_page_key}' not in st.session_state:
                st.session_state[f'posts_current_page_{posts_page_key}'] = 1
            if f'posts_per_page_{posts_page_key}' not in st.session_state:
                st.session_state[f'posts_per_page_{posts_page_key}'] = 50
            
            # Display posts with pagination
            if not filtered_df.empty:
                total_posts = len(filtered_df)
                current_page = st.session_state[f'posts_current_page_{posts_page_key}']
                posts_per_page = st.session_state[f'posts_per_page_{posts_page_key}']
                total_pages = (total_posts + posts_per_page - 1) // posts_per_page
                
                # Posts per page selector
                col1, col2 = st.columns([1, 3])
                with col1:
                    posts_options = [25, 50, 100, 200]
                    new_posts_per_page = st.selectbox("Posts per page:", posts_options, 
                                                     index=posts_options.index(posts_per_page),
                                                     key=f"posts_per_page_selector_{posts_page_key}")
                    if new_posts_per_page != posts_per_page:
                        st.session_state[f'posts_per_page_{posts_page_key}'] = new_posts_per_page
                        st.session_state[f'posts_current_page_{posts_page_key}'] = 1
                        st.rerun()
                
                with col2:
                    st.write(f"📊 Showing {min(current_page * posts_per_page, total_posts)} of {total_posts} posts")
                
                # Get posts for current page
                start_idx = (current_page - 1) * posts_per_page
                end_idx = start_idx + posts_per_page
                paginated_df = filtered_df.iloc[start_idx:end_idx]
                
                # Prepare dataframe with cleaned comments for Quora
                display_df = prepare_posts_dataframe(paginated_df)
                
                # Select columns to display, including comment and comment_sentiment if they exist
                display_columns = ['title', 'platform', 'sentiment_predicted', 'sentiment_confidence']
                if 'comment' in display_df.columns:
                    display_columns.append('comment')
                if 'comment_sentiment' in display_df.columns:
                    display_columns.append('comment_sentiment')
                display_columns.append('date')  # Date at the end
                
                st.dataframe(
                    display_df[display_columns],  # Show paginated posts with cleaned comments
                    use_container_width=True,
                    height=600  # Set a reasonable height for scrolling
                )
                
                # Add pagination controls for Posts page
                st.markdown("---")
                col1, col2, col3, col4, col5 = st.columns([1, 1, 2, 1, 1])
                
                with col1:
                    if st.button("⬅️ Previous", disabled=current_page <= 1, key=f"prev_{posts_page_key}"):
                        st.session_state[f'posts_current_page_{posts_page_key}'] -= 1
                        st.rerun()
                
                with col2:
                    if st.button("⏮️ First", disabled=current_page <= 1, key=f"first_{posts_page_key}"):
                        st.session_state[f'posts_current_page_{posts_page_key}'] = 1
                        st.rerun()
                
                with col3:
                    st.write(f"📄 Page {current_page} of {total_pages}")
                
                with col4:
                    if st.button("⏭️ Last", disabled=current_page >= total_pages, key=f"last_{posts_page_key}"):
                        st.session_state[f'posts_current_page_{posts_page_key}'] = total_pages
                        st.rerun()
                
                with col5:
                    if st.button("➡️ Next", disabled=current_page >= total_pages, key=f"next_{posts_page_key}"):
                        st.session_state[f'posts_current_page_{posts_page_key}'] += 1
                        st.rerun()
                        
            else:
                st.warning("No posts match the selected sentiment filter.")
        else:
            st.warning("No data available for the selected platform.")
    
    elif selected_page == "🤖 AI Assistant":
        st.header("🤖 AI Assistant")
        st.markdown("Ask me anything about your dashboard data and social media analytics!")
        
        # Render the chatbot interface, passing the current platform for context
        render_chatbot_interface(platform)
    
    else:
        st.header(f"{selected_page}")
        st.info("This section is under development.")

if __name__ == "__main__":
    main() 