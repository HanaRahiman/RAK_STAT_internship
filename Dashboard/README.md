# Dashboard Project

A Streamlit-based social media sentiment analysis dashboard with PostgreSQL backend and AI-powered chatbot assistant.

## Features

- 📊 **Interactive Dashboard**: Comprehensive sentiment analysis visualization
- 📝 **Data Management**: Browse and filter social media posts from multiple platforms
- 📈 **Analytics**: Real-time sentiment trends and platform comparisons  
- 🤖 **AI Assistant**: Intelligent chatbot powered by Llama 3.3 70B (Free) model
- 💾 **PostgreSQL Backend**: Robust data storage and retrieval
- 🎨 **Modern UI**: Beautiful dark theme with responsive design

## Project Structure

```
Dashboard/
├── dashboard_postgresql.py    # Main Streamlit application
├── requirements.txt          # Python dependencies
├── README.md                # This file
├── README_PostgreSQL_Setup.md # PostgreSQL setup instructions
├── database/                # Database-related modules
│   ├── __init__.py
│   ├── config.py           # Database and AI configuration
│   ├── dashboard_db.py     # Database manager and utilities
│   ├── chatbot.py          # AI chatbot integration (EXAONE 3.5 32B)
│   └── setup_database.py   # Database setup script
└── scripts/                # Utility scripts
    ├── __init__.py
    └── migrate_data_with_progress.py  # Data migration script
```

## Quick Start

1. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Set up PostgreSQL database:**
   ```bash
   python database/setup_database.py
   ```

3. **Run data migration (if needed):**
   ```bash
   python scripts/migrate_data_with_progress.py
   ```

4. **Set up your .env file:**
   ```bash
   # Copy and create your .env file with the configuration shown below
   ```

5. **Start the dashboard:**
   ```bash
   streamlit run dashboard_postgresql.py
   ```

## Configuration

Create a `.env` file in the root directory with your database credentials and Together AI API key:

```env
# Database Configuration
DB_HOST=localhost
DB_PORT=5432
DB_NAME=dashboard_db
DB_USER=postgres
DB_PASSWORD=the password that you created when you setup postgresql

# Together AI Configuration (for chatbot)
TOGETHER_API_KEY=your_together_ai_api_key_here
```

### Getting Together AI API Key

1. Visit [Together AI](https://api.together.xyz/)
2. Sign up for an account
3. Get your API key from the dashboard
4. Add it to your `.env` file as shown above

For detailed PostgreSQL setup instructions, see `README_PostgreSQL_Setup.md`. 