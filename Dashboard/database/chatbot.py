import streamlit as st
import pandas as pd
from typing import List, Dict, Any, Optional
import json
from together import Together
from database.config import TOGETHER_AI_CONFIG
from database.dashboard_db import DatabaseManager
import plotly.express as px
import plotly.graph_objects as go


class DashboardChatbot:
    """
    Intelligent chatbot for the dashboard that can analyze data and answer questions
    using Together AI's EXAONE 3.5 32B Instruct model
    """
    
    def __init__(self):
        self.client = None
        self.db_manager = None
        self.initialize_client()
    
    def initialize_client(self):
        """Initialize the Together AI client"""
        try:
            if TOGETHER_AI_CONFIG['api_key']:
                self.client = Together(api_key=TOGETHER_AI_CONFIG['api_key'])
                self.db_manager = DatabaseManager()
            else:
                st.error("Together AI API key not found. Please add TOGETHER_API_KEY to your .env file.")
        except Exception as e:
            st.error(f"Failed to initialize Together AI client: {e}")
    
    def get_data_context(self, platform: str = 'All Platforms') -> str:
        """Get current dashboard data context for the AI"""
        try:
            if not self.db_manager:
                return "Database not available."
            
            # Get basic stats
            if platform == 'All Platforms':
                df = self.db_manager.get_platform_data(platform=None, topic=None)
            else:
                df = self.db_manager.get_platform_data(platform, topic=None)
            
            if df is None or df.empty:
                return "No data available in the dashboard."
            
            # Create data summary
            total_posts = len(df)
            platforms = df['platform'].value_counts().to_dict() if 'platform' in df.columns else {}
            
            sentiment_stats = {}
            if 'sentiment_predicted' in df.columns:
                sentiment_stats = df['sentiment_predicted'].value_counts().to_dict()
            elif 'sentiment' in df.columns:
                sentiment_stats = df['sentiment'].value_counts().to_dict()
            
            # Get date range
            date_range = ""
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'])
                date_range = f"from {df['date'].min().strftime('%Y-%m-%d')} to {df['date'].max().strftime('%Y-%m-%d')}"
            
            # Store the full dataframe for content analysis
            self.current_data = df
            
            context = f"""
            Current Dashboard Data Summary:
            - Total Posts: {total_posts}
            - Date Range: {date_range}
            - Platforms: {platforms}
            - Sentiment Distribution: {sentiment_stats}
            - Available Columns: {list(df.columns)}
            
            I have access to the full content of all {total_posts} posts and can analyze specific topics, 
            search for keywords, summarize content, and provide detailed insights about the actual post content.
            """
            
            return context
            
        except Exception as e:
            return f"Error getting data context: {e}"
    
    def search_posts_content(self, keywords: List[str], max_posts: int = 20) -> str:
        """Search for posts containing specific keywords and return relevant content"""
        try:
            if not hasattr(self, 'current_data') or self.current_data is None or self.current_data.empty:
                return "No data available for content search."
            
            df = self.current_data.copy()
            
            # Combine title and content for searching
            search_columns = []
            if 'title' in df.columns:
                search_columns.append('title')
            if 'content' in df.columns:
                search_columns.append('content')
            if 'comment' in df.columns:
                search_columns.append('comment')
            
            if not search_columns:
                return "No searchable text content found in the data."
            
            # Create combined search text
            df['search_text'] = ''
            for col in search_columns:
                df['search_text'] += ' ' + df[col].fillna('').astype(str)
            
            # Search for keywords (case insensitive)
            mask = pd.Series([False] * len(df))
            for keyword in keywords:
                mask = mask | df['search_text'].str.contains(keyword, case=False, na=False)
            
            matching_posts = df[mask].head(max_posts)
            
            if matching_posts.empty:
                return f"No posts found containing the keywords: {', '.join(keywords)}"
            
            # Format the results
            results = []
            for idx, post in matching_posts.iterrows():
                result = f"POST {len(results) + 1}:\n"
                result += f"Platform: {post.get('platform', 'Unknown')}\n"
                if 'title' in post and pd.notna(post['title']):
                    result += f"Title: {post['title'][:200]}...\n" if len(str(post['title'])) > 200 else f"Title: {post['title']}\n"
                if 'content' in post and pd.notna(post['content']):
                    result += f"Content: {post['content'][:300]}...\n" if len(str(post['content'])) > 300 else f"Content: {post['content']}\n"
                if 'sentiment_predicted' in post:
                    result += f"Sentiment: {post['sentiment_predicted']}\n"
                elif 'sentiment' in post:
                    result += f"Sentiment: {post['sentiment']}\n"
                if 'date' in post and pd.notna(post['date']):
                    result += f"Date: {post['date']}\n"
                result += "---\n"
                results.append(result)
            
            summary = f"Found {len(matching_posts)} posts containing keywords: {', '.join(keywords)}\n\n"
            summary += '\n'.join(results)
            
            return summary
            
        except Exception as e:
            return f"Error searching posts: {e}"
    
    def generate_system_prompt(self, platform: str = 'All Platforms') -> str:
        """Generate system prompt with current dashboard context"""
        data_context = self.get_data_context(platform)
        
        return f"""You are an intelligent assistant for a social media sentiment analysis dashboard. 
        You help users understand their data, provide insights, and answer questions about social media analytics.

        {data_context}

        Your capabilities include:
        - Analyzing sentiment trends and patterns
        - Explaining data insights and metrics
        - Suggesting data analysis approaches
        - Helping with dashboard navigation
        - Providing social media analytics expertise

        Always be helpful, accurate, and provide actionable insights. If you need specific data that isn't in the summary above, let the user know what additional information would be helpful.
        
        Keep responses concise but informative. Use emojis occasionally to make responses more engaging.
        """
    
    def chat_with_ai(self, user_message: str, platform: str = 'All Platforms', chat_history: List[Dict] = None) -> str:
        """Send message to Together AI and get response"""
        if not self.client:
            return "❌ Chatbot not available. Please check your Together AI API key configuration."
        
        try:
            # Check if user is asking for content analysis
            content_keywords = ['summarize posts', 'posts about', 'find posts', 'search posts', 'posts containing', 'analyze posts']
            is_content_request = any(keyword in user_message.lower() for keyword in content_keywords)
            
            additional_context = ""
            if is_content_request:
                # Try to extract keywords from the user's question
                import re
                # Look for common patterns like "posts about X", "summarize posts about X"
                patterns = [
                    r'posts about (.+?)(?:\s|$)',
                    r'summarize posts about (.+?)(?:\s|$)',
                    r'find posts containing (.+?)(?:\s|$)',
                    r'posts containing (.+?)(?:\s|$)',
                    r'analyze posts about (.+?)(?:\s|$)'
                ]
                
                keywords = []
                for pattern in patterns:
                    match = re.search(pattern, user_message.lower())
                    if match:
                        # Extract and clean the keywords
                        keyword_text = match.group(1).strip()
                        # Remove common stop words and clean up
                        keyword_text = re.sub(r'\bthat\b|\bfound\b|\bin\b|\bthis\b|\bdashboard\b', '', keyword_text).strip()
                        if keyword_text:
                            keywords.extend([kw.strip() for kw in keyword_text.split() if len(kw.strip()) > 2])
                
                # If we found keywords, search for relevant posts
                if keywords:
                    # Remove duplicates and take first few keywords
                    keywords = list(set(keywords))[:5]
                    search_results = self.search_posts_content(keywords, max_posts=15)
                    additional_context = f"\n\nRELEVANT POSTS FOUND:\n{search_results}"
            
            # Prepare messages
            system_prompt = self.generate_system_prompt(platform) + additional_context
            messages = [
                {"role": "system", "content": system_prompt}
            ]
            
            # Add chat history if available
            if chat_history:
                messages.extend(chat_history[-10:])  # Keep last 10 messages for context
            
            # Add current user message
            messages.append({"role": "user", "content": user_message})
            
            # Make API call
            response = self.client.chat.completions.create(
                model=TOGETHER_AI_CONFIG['model'],
                messages=messages,
                max_tokens=TOGETHER_AI_CONFIG['max_tokens'],
                temperature=TOGETHER_AI_CONFIG['temperature']
            )
            
            return response.choices[0].message.content
            
        except Exception as e:
            return f"❌ Error communicating with AI: {e}"
    
    def get_suggested_questions(self, platform: str = 'All Platforms') -> List[str]:
        """Get suggested questions based on current data"""
        base_questions = [
            "📊 What insights can you provide about the current data?",
            "📈 What are the main sentiment trends?",
            "🔍 Which platform has the most engagement?",
            "💡 What patterns do you see in the data?",
            "📋 Can you summarize the key metrics?",
            "🎓 Summarize posts about education or school",
            "💰 Find posts about fees or costs",
            "📝 What are the main topics being discussed?",
        ]
        
        # Add platform-specific questions
        if platform != 'All Platforms':
            base_questions.extend([
                f"🎯 What's unique about {platform} data?",
                f"📊 How does {platform} compare to other platforms?",
            ])
        
        return base_questions


def render_chatbot_interface(platform: str = 'All Platforms'):
    """Render the chatbot interface in Streamlit"""
    
    # Initialize chatbot
    if 'chatbot' not in st.session_state:
        st.session_state.chatbot = DashboardChatbot()
    
    # Initialize chat history
    if 'chat_history' not in st.session_state:
        st.session_state.chat_history = []
    
    # Chatbot header
    st.markdown("""
    <div style="background: linear-gradient(90deg, #667eea 0%, #764ba2 100%); 
                padding: 1rem; border-radius: 10px; margin-bottom: 1rem;">
        <h3 style="color: white; margin: 0;">🤖 Dashboard AI Assistant</h3>
        <p style="color: white; margin: 0; opacity: 0.8;">Powered by Llama 3.3 70B (Free) 🆓</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Check if API key is configured
    if not TOGETHER_AI_CONFIG['api_key']:
        st.error("🔑 Together AI API key not configured. Please add TOGETHER_API_KEY to your .env file.")
        st.info("Get your API key from: https://api.together.xyz/")
        return
    
    # Suggested questions
    if not st.session_state.chat_history:
        st.subheader("💡 Suggested Questions")
        suggestions = st.session_state.chatbot.get_suggested_questions(platform)
        
        cols = st.columns(2)
        for i, question in enumerate(suggestions):
            col = cols[i % 2]
            if col.button(question, key=f"suggestion_{i}"):
                st.session_state.pending_message = question.split(' ', 1)[1]  # Remove emoji
                st.rerun()
    
    # Chat history display
    if st.session_state.chat_history:
        st.subheader("💬 Conversation")
        
        # Create a container for chat messages
        chat_container = st.container()
        
        with chat_container:
            for i, message in enumerate(st.session_state.chat_history):
                if message["role"] == "user":
                    st.markdown(f"""
                    <div style="background-color: #4a5568; padding: 0.75rem; border-radius: 10px; margin: 0.5rem 0; margin-left: 2rem;">
                        <strong style="color: #ffd700;">You:</strong> <span style="color: #ffffff;">{message["content"]}</span>
                    </div>
                    """, unsafe_allow_html=True)
                else:
                    st.markdown(f"""
                    <div style="background-color: #2d3748; padding: 0.75rem; border-radius: 10px; margin: 0.5rem 0; margin-right: 2rem; border-left: 3px solid #667eea;">
                        <strong style="color: #667eea;">🤖 AI Assistant:</strong> <span style="color: #ffffff;">{message["content"]}</span>
                    </div>
                    """, unsafe_allow_html=True)
    
    # Chat input
    user_input = st.chat_input("Ask me anything about your dashboard data...")
    
    # Handle pending message from suggestions
    if 'pending_message' in st.session_state:
        user_input = st.session_state.pending_message
        del st.session_state.pending_message
    
    # Process user input
    if user_input:
        # Add user message to history
        st.session_state.chat_history.append({"role": "user", "content": user_input})
        
        # Get AI response
        with st.spinner("🤔 Thinking..."):
            ai_response = st.session_state.chatbot.chat_with_ai(
                user_input, 
                platform, 
                st.session_state.chat_history[:-1]  # Exclude the current message
            )
        
        # Add AI response to history
        st.session_state.chat_history.append({"role": "assistant", "content": ai_response})
        
        # Rerun to update the display
        st.rerun()
    
    # Clear chat button
    if st.session_state.chat_history:
        if st.button("🗑️ Clear Chat", type="secondary"):
            st.session_state.chat_history = []
            st.rerun() 