import pandas as pd
import os
from datetime import datetime, timedelta
from database.dashboard_db import DatabaseManager
from tqdm import tqdm
import time
from sqlalchemy import text
import numpy as np

def parse_date_safely(date_value, platform_name):
    """Safely parse date from various formats"""
    if pd.isna(date_value) or date_value == '' or str(date_value).lower() == 'nan':
        # Return current date if no date provided
        return datetime.now().date()
    
    date_str = str(date_value).strip()
    
    try:
        # Try standard date formats first
        if '-' in date_str and len(date_str) == 10:  # YYYY-MM-DD format
            return datetime.strptime(date_str, '%Y-%m-%d').date()
        elif '/' in date_str:  # MM/DD/YYYY or DD/MM/YYYY
            try:
                return datetime.strptime(date_str, '%m/%d/%Y').date()
            except:
                return datetime.strptime(date_str, '%d/%m/%Y').date()
        
        # Handle relative dates (LinkedIn)
        if 'ago' in date_str.lower():
            current_date = datetime.now()
            if 'month' in date_str:
                months = int(date_str.split()[0]) if date_str.split()[0].isdigit() else 1
                estimated_date = current_date - timedelta(days=months * 30)
                return estimated_date.date()
            elif 'week' in date_str:
                weeks = int(date_str.split()[0]) if date_str.split()[0].isdigit() else 1
                estimated_date = current_date - timedelta(weeks=weeks)
                return estimated_date.date()
            elif 'day' in date_str:
                days = int(date_str.split()[0]) if date_str.split()[0].isdigit() else 1
                estimated_date = current_date - timedelta(days=days)
                return estimated_date.date()
            elif 'year' in date_str:
                years = int(date_str.split()[0]) if date_str.split()[0].isdigit() else 1
                estimated_date = current_date - timedelta(days=years * 365)
                return estimated_date.date()
        
        # If it's already a datetime object
        if isinstance(date_value, datetime):
            return date_value.date()
        
        # Try pandas to_datetime as fallback
        parsed_date = pd.to_datetime(date_str, errors='coerce')
        if not pd.isna(parsed_date):
            return parsed_date.date()
        
    except Exception as e:
        print(f"   ⚠️  Warning: Could not parse date '{date_value}' for {platform_name}, using current date: {e}")
    
    # Fallback to current date
    return datetime.now().date()

def migrate_data_with_progress():
    """Migrate data from CSV and Excel files to PostgreSQL with progress tracking"""
    
    # Initialize database
    print("🔄 Initializing database connection...")
    db = DatabaseManager()
    
    if not db.connect():
        print("❌ Failed to connect to database")
        return
    
    print("✅ Connected to database successfully")
    
    # Clear existing data
    print("🧹 Clearing existing data...")
    try:
        with db.engine.connect() as conn:
            conn.execute(text("DELETE FROM social_media_data"))
            conn.commit()
        print("✅ Existing data cleared")
    except Exception as e:
        print(f"❌ Failed to clear existing data: {e}")
        return
    
    # File mapping with locations
    files_to_process = [
        {
            'name': 'Khaleej Times',
            'file': 'original_data_backup/Khaleej Times.csv',
            'type': 'csv'
        },
        {
            'name': 'LinkedIn',
            'file': 'original_data_backup/Linkedin.csv', 
            'type': 'csv'
        },
        {
            'name': 'Quora',
            'file': 'original_data_backup/Quora.xlsx',
            'type': 'excel'
        },
        {
            'name': 'Reddit',
            'file': 'original_data_backup/Reddit.xlsx',
            'type': 'excel'
        },
        {
            'name': 'Reuters',
            'file': 'original_data_backup/Reuters.xlsx',
            'type': 'excel'
        }
    ]
    
    total_records = 0
    successful_imports = 0
    
    for file_info in files_to_process:
        print(f"\n📊 Processing {file_info['name']}...")
        
        # Check if file exists
        if not os.path.exists(file_info['file']):
            print(f"❌ File not found: {file_info['file']}")
            continue
            
        try:
            # Load data with progress
            print(f"📖 Loading {file_info['name']} data...")
            
            if file_info['type'] == 'csv':
                df = pd.read_csv(file_info['file'])
            else:
                df = pd.read_excel(file_info['file'])
            
            print(f"✅ Loaded {len(df)} records from {file_info['name']}")
            
            # Process data based on platform
            processed_records = process_platform_data(df, file_info['name'])
            
            if processed_records is None or processed_records.empty:
                print(f"❌ No valid data to process for {file_info['name']}")
                continue
            
            # Insert data in batches with progress bar
            batch_size = 100
            total_batches = (len(processed_records) + batch_size - 1) // batch_size
            
            print(f"💾 Inserting {len(processed_records)} records in {total_batches} batches...")
            
            records_inserted = 0
            with tqdm(total=len(processed_records), desc=f"Inserting {file_info['name']}", unit="records") as pbar:
                for i in range(0, len(processed_records), batch_size):
                    batch = processed_records.iloc[i:i+batch_size]
                    
                    # Insert batch
                    if insert_batch(db, batch):
                        records_inserted += len(batch)
                        pbar.update(len(batch))
                    else:
                        print(f"❌ Failed to insert batch {i//batch_size + 1}")
                        break
                    
                    # Small delay to prevent overwhelming the database
                    time.sleep(0.1)
            
            print(f"✅ Successfully inserted {records_inserted}/{len(processed_records)} records for {file_info['name']}")
            total_records += records_inserted
            if records_inserted > 0:
                successful_imports += 1
                
        except Exception as e:
            print(f"❌ Error processing {file_info['name']}: {str(e)}")
            continue
    
    # Final summary
    print(f"\n🎉 Migration completed!")
    print(f"📈 Total records migrated: {total_records}")
    print(f"✅ Successful file imports: {successful_imports}/{len(files_to_process)}")
    
    # Verify data in database
    print("\n🔍 Verifying data in database...")
    verify_query = "SELECT platform, COUNT(*) as count FROM social_media_data GROUP BY platform ORDER BY platform"
    result = db.execute_query(verify_query)
    
    if result is not None and not result.empty:
        print("📊 Final data counts by platform:")
        for _, row in result.iterrows():
            print(f"   {row['platform']}: {row['count']} records")
    else:
        print("❌ Could not verify data counts")

def process_platform_data(df, platform_name):
    """Process data based on platform with specific column mapping and original date extraction"""
    
    print(f"🔧 Processing {platform_name} data structure...")
    
    try:
        if platform_name == 'Khaleej Times':
            print("   📝 Processing Khaleej Times data...")
            processed_df = pd.DataFrame()
            
            # Map Khaleej Times columns
            processed_df['title'] = df.get('title', '')
            processed_df['content'] = df.get('content', '')
            processed_df['comment'] = ''  # Khaleej Times doesn't have comments
            processed_df['comment_sentiment'] = ''
            processed_df['author'] = df.get('author', '')
            processed_df['url'] = df.get('url', '')
            processed_df['summary'] = df.get('summary', '')
            processed_df['sentiment_predicted'] = df.get('sentiment_predicted', '')
            processed_df['sentiment_confidence'] = df.get('sentiment_confidence', 0.0)
            processed_df['relevance_score'] = df.get('Relevance_Score', 0.0)
            processed_df['relevant_to_education_in_uae'] = df.get('Relevant_to_Education_in_UAE', False)
            processed_df['sentiment_negative'] = df.get('sentiment_negative', 0.0)
            processed_df['sentiment_neutral'] = df.get('sentiment_neutral', 0.0)
            processed_df['sentiment_positive'] = df.get('sentiment_positive', 0.0)
            processed_df['combined_text'] = df.get('combined_text', '')
            processed_df['platform'] = 'Khaleej Times'
            
            # Extract original dates
            print("   📅 Extracting original dates...")
            processed_df['date'] = df['date'].apply(lambda x: parse_date_safely(x, platform_name))
            
        elif platform_name == 'LinkedIn':
            print("   📝 Processing LinkedIn data...")
            
            # Filter out non-education job posts, keep education jobs and discussions
            def is_education_relevant_post(row):
                """Check if LinkedIn post is education-related (keep education jobs, remove other jobs)"""
                post_text = str(row.get('post_text', '')).lower()
                comment_text = str(row.get('comment_text', '')).lower()
                combined_text = f"{post_text} {comment_text}"
                
                # Education-related keywords (including education jobs and discussions)
                education_keywords = [
                    # Education discussions
                    'education', 'educational', 'school', 'university', 'college', 'academic', 'learning',
                    'teaching', 'teacher', 'professor', 'instructor', 'tutor', 'faculty', 'student',
                    'curriculum', 'course', 'class', 'lesson', 'training', 'workshop', 'seminar',
                    'degree', 'diploma', 'certificate', 'graduation', 'study', 'studies', 'research',
                    'scholarship', 'grant', 'academy', 'institute', 'educational technology', 'edtech',
                    'online learning', 'e-learning', 'distance education', 'higher education',
                    'early childhood education', 'special education', 'vocational training',
                    'literacy', 'numeracy', 'pedagogy', 'assessment', 'evaluation',
                    
                    # UAE education specific
                    'uae education', 'dubai education', 'abu dhabi education', 'ministry of education',
                    'emirates education', 'adek', 'khda', 'siac', 'educational standards',
                    
                    # Education jobs (these are OK to keep)
                    'teacher position', 'teaching position', 'professor position', 'instructor position',
                    'tutor position', 'faculty position', 'academic position', 'education job',
                    'school job', 'university job', 'college job', 'principal position',
                    'head teacher', 'coordinator position', 'administrator position', 'academic role'
                ]
                
                # Non-education job keywords to exclude (jobs NOT related to education)
                non_education_job_keywords = [
                    # Business/Commercial jobs
                    'sales executive', 'marketing manager', 'business development', 'account manager',
                    'finance manager', 'hr manager', 'operations manager', 'project manager',
                    'software engineer', 'data analyst', 'graphic designer', 'social media manager',
                    'customer service', 'admin assistant', 'receptionist', 'driver', 'cleaner',
                    'security guard', 'warehouse', 'logistics', 'supply chain', 'procurement',
                    'real estate', 'construction', 'engineering', 'manufacturing', 'retail',
                    'restaurant', 'hospitality', 'tourism', 'banking', 'insurance', 'investment',
                    
                    # General non-education job terms (when NOT combined with education)
                    'sales position', 'marketing position', 'it position', 'finance position',
                    'business analyst', 'consultant position', 'manager position', 'executive position',
                    'coordinator position', 'specialist position', 'officer position'
                ]
                
                # Check if it contains education keywords
                has_education_content = any(keyword in combined_text for keyword in education_keywords)
                
                # Check if it's a non-education job posting
                has_non_education_job = any(keyword in combined_text for keyword in non_education_job_keywords)
                
                # Keep if:
                # 1. Contains education keywords (including education jobs), OR
                # 2. Doesn't contain non-education job keywords (general educational discussions)
                # But exclude if it's clearly a non-education job
                return has_education_content or (not has_non_education_job)
            
            # Filter the DataFrame
            print("   🔍 Filtering for education-relevant posts...")
            original_count = len(df)
            df_filtered = df[df.apply(is_education_relevant_post, axis=1)]
            filtered_count = len(df_filtered)
            print(f"   📊 Kept {filtered_count}/{original_count} education-relevant posts ({filtered_count/original_count*100:.1f}%)")
            
            processed_df = pd.DataFrame()
            
            # Map LinkedIn columns: comment_text as content, avoid keywords to prevent duplication
            # Create proper titles from post_text without truncation artifacts and clean prefixes
            def clean_linkedin_title(text):
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
            
            post_texts = df_filtered.get('post_text', '').astype(str)
            processed_df['title'] = post_texts.apply(clean_linkedin_title)
            processed_df['content'] = df_filtered.get('comment_text', '')  # comment_text as main content
            processed_df['comment'] = ''  # No separate comment field needed
            processed_df['comment_sentiment'] = ''  # Will be populated if needed
            processed_df['author'] = ''  # LinkedIn doesn't have author info
            processed_df['url'] = ''  # LinkedIn doesn't have URLs in this format
            processed_df['summary'] = ''
            processed_df['sentiment_predicted'] = df_filtered.get('sentiment_predicted', '')
            processed_df['sentiment_confidence'] = df_filtered.get('sentiment_confidence', 0.0)
            processed_df['relevance_score'] = df_filtered.get('Relevance_Score', 0.0)
            processed_df['relevant_to_education_in_uae'] = df_filtered.get('Relevant_to_Education_in_UAE', False)
            processed_df['sentiment_negative'] = df_filtered.get('sentiment_negative', 0.0)
            processed_df['sentiment_neutral'] = df_filtered.get('sentiment_neutral', 0.0)
            processed_df['sentiment_positive'] = df_filtered.get('sentiment_positive', 0.0)
            processed_df['combined_text'] = df_filtered.get('combined_text', '')
            processed_df['platform'] = 'LinkedIn'
            
            # Extract dates from post_date column (convert "1 month ago" format)
            print("   📅 Extracting dates from post_date column...")
            if 'post_date' in df_filtered.columns:
                processed_df['date'] = df_filtered['post_date'].apply(lambda x: parse_date_safely(x, platform_name))
            elif 'date' in df_filtered.columns:
                processed_df['date'] = df_filtered['date'].apply(lambda x: parse_date_safely(x, platform_name))
            else:
                processed_df['date'] = datetime.now().date()
            
        elif platform_name == 'Quora':
            print("   📝 Processing Quora data...")
            processed_df = pd.DataFrame()
            
            # Map Quora columns - keep question and answer separate
            processed_df['title'] = df.get('Title', '')  # Question title
            processed_df['content'] = df.get('Answer', '')  # Answer content  
            processed_df['comment'] = ''  # Don't duplicate the answer in comment
            processed_df['comment_sentiment'] = df.get('sentiment_predicted', '')
            processed_df['author'] = ''  # Quora doesn't have author info
            processed_df['url'] = df.get('URL', '')
            processed_df['summary'] = df.get('Question Details', '')  # Question details as summary
            processed_df['sentiment_predicted'] = df.get('sentiment_predicted', '')
            processed_df['sentiment_confidence'] = df.get('sentiment_confidence', 0.0)
            processed_df['relevance_score'] = 0.0  # Quora doesn't have relevance score
            processed_df['relevant_to_education_in_uae'] = df.get('Relevant_to_Education_in_UAE', False)
            processed_df['sentiment_negative'] = df.get('sentiment_negative', 0.0)
            processed_df['sentiment_neutral'] = df.get('sentiment_neutral', 0.0)
            processed_df['sentiment_positive'] = df.get('sentiment_positive', 0.0)
            processed_df['combined_text'] = (df.get('Title', '') + ' ' + df.get('Answer', '')).str.strip()
            processed_df['platform'] = 'Quora'
            
            # Extract original dates
            print("   📅 Extracting original dates...")
            processed_df['date'] = df['date'].apply(lambda x: parse_date_safely(x, platform_name))
            
        elif platform_name == 'Reddit':
            print("   📝 Processing Reddit data...")
            processed_df = pd.DataFrame()
            
            # Map Reddit columns with improved content handling
            processed_df['title'] = df.get('question', '')  # question -> title
            
            # Keep question and response separate (like LinkedIn and Quora)
            processed_df['content'] = df.get('question', '')  # Question content only
            processed_df['comment'] = df.get('comment_text', '')  # Keep original comment for reference
            processed_df['comment_sentiment'] = df.get('comment_sentiment_predicted', '')
            processed_df['author'] = df.get('subreddit', '')  # subreddit -> author
            processed_df['url'] = ''  # Reddit doesn't have URLs in this format
            processed_df['summary'] = ''  # No summary field
            
            # Use question sentiment as main sentiment, fallback to comment sentiment if question sentiment is missing
            def get_reddit_sentiment(row):
                question_sentiment = row.get('question_sentiment_predicted', '')
                comment_sentiment = row.get('comment_sentiment_predicted', '')
                
                # Priority: question sentiment first, then comment sentiment
                if question_sentiment and str(question_sentiment).strip() and str(question_sentiment).strip().lower() != 'nan':
                    return question_sentiment
                elif comment_sentiment and str(comment_sentiment).strip() and str(comment_sentiment).strip().lower() != 'nan':
                    return comment_sentiment
                else:
                    return ''
            
            processed_df['sentiment_predicted'] = df.apply(get_reddit_sentiment, axis=1)
            
            # Use question confidence, fallback to comment confidence
            def get_reddit_confidence(row):
                question_confidence = row.get('question_sentiment_confidence', 0.0)
                comment_confidence = row.get('comment_sentiment_confidence', 0.0)
                
                # If question sentiment is used, use question confidence
                question_sentiment = row.get('question_sentiment_predicted', '')
                if question_sentiment and str(question_sentiment).strip() and str(question_sentiment).strip().lower() != 'nan':
                    return float(question_confidence) if pd.notna(question_confidence) else 0.0
                else:
                    return float(comment_confidence) if pd.notna(comment_confidence) else 0.0
            
            processed_df['sentiment_confidence'] = df.apply(get_reddit_confidence, axis=1)
            processed_df['relevance_score'] = 0.0  # Reddit doesn't have relevance score
            processed_df['relevant_to_education_in_uae'] = df.get('Relevant_to_Education_in_UAE', True)  # Use actual column
            # Use question sentiment scores as priority, fallback to comment sentiment scores
            def get_sentiment_score(row, sentiment_type):
                question_score = row.get(f'question_sentiment_{sentiment_type}', 0.0)
                comment_score = row.get(f'comment_sentiment_{sentiment_type}', 0.0)
                
                # If question sentiment is used, use question scores
                question_sentiment = row.get('question_sentiment_predicted', '')
                if question_sentiment and str(question_sentiment).strip() and str(question_sentiment).strip().lower() != 'nan':
                    return float(question_score) if pd.notna(question_score) else 0.0
                else:
                    return float(comment_score) if pd.notna(comment_score) else 0.0
            
            processed_df['sentiment_negative'] = df.apply(lambda row: get_sentiment_score(row, 'negative'), axis=1)
            processed_df['sentiment_neutral'] = df.apply(lambda row: get_sentiment_score(row, 'neutral'), axis=1)
            processed_df['sentiment_positive'] = df.apply(lambda row: get_sentiment_score(row, 'positive'), axis=1)
            processed_df['combined_text'] = (df.get('question', '') + ' ' + df.get('comment_text', '')).str.strip()
            processed_df['platform'] = 'Reddit'
            
            # Extract original dates (check if date column exists)
            print("   📅 Extracting original dates...")
            if 'date' in df.columns:
                processed_df['date'] = df['date'].apply(lambda x: parse_date_safely(x, platform_name))
            else:
                print("   ⚠️  No date column found in Reddit data, using current date")
                processed_df['date'] = datetime.now().date()
            
        elif platform_name == 'Reuters':
            print("   📝 Processing Reuters data...")
            processed_df = pd.DataFrame()
            
            # Map Reuters columns (similar to standard structure)
            processed_df['title'] = df.get('title', df.get('Title', ''))
            processed_df['content'] = df.get('content', df.get('Content', ''))
            processed_df['comment'] = ''  # Reuters doesn't have comments
            processed_df['comment_sentiment'] = ''
            processed_df['author'] = df.get('author', df.get('Author', ''))
            processed_df['url'] = df.get('url', df.get('URL', ''))
            processed_df['summary'] = df.get('summary', df.get('Summary', ''))
            processed_df['sentiment_predicted'] = df.get('sentiment_predicted', df.get('Sentiment_Predicted', ''))
            processed_df['sentiment_confidence'] = df.get('sentiment_confidence', df.get('Sentiment_Confidence', 0.0))
            processed_df['relevance_score'] = df.get('Relevance_Score', 0.0)
            processed_df['relevant_to_education_in_uae'] = df.get('Relevant_to_Education_in_UAE', False)
            processed_df['sentiment_negative'] = df.get('sentiment_negative', 0.0)
            processed_df['sentiment_neutral'] = df.get('sentiment_neutral', 0.0)
            processed_df['sentiment_positive'] = df.get('sentiment_positive', 0.0)
            processed_df['combined_text'] = df.get('combined_text', '')
            processed_df['platform'] = 'Reuters'
            
            # Extract original dates
            print("   📅 Extracting original dates...")
            if 'date' in df.columns:
                processed_df['date'] = df['date'].apply(lambda x: parse_date_safely(x, platform_name))
            elif 'Date' in df.columns:
                processed_df['date'] = df['Date'].apply(lambda x: parse_date_safely(x, platform_name))
            else:
                print("   ⚠️  No date column found in Reuters data, using current date")
                processed_df['date'] = datetime.now().date()
        
        else:
            # Fallback for unknown platforms
            print(f"   📝 Using standard column mapping for {platform_name}...")
            processed_df = pd.DataFrame()
            
            processed_df['title'] = df.get('title', df.get('Title', ''))
            processed_df['content'] = df.get('content', df.get('Content', ''))
            processed_df['comment'] = ''  # Fallback platforms don't have comments
            processed_df['comment_sentiment'] = ''
            processed_df['author'] = df.get('author', df.get('Author', ''))
            processed_df['url'] = df.get('url', df.get('URL', ''))
            processed_df['summary'] = df.get('summary', df.get('Summary', ''))
            processed_df['sentiment_predicted'] = df.get('sentiment_predicted', df.get('Sentiment_Predicted', ''))
            processed_df['sentiment_confidence'] = df.get('sentiment_confidence', df.get('Sentiment_Confidence', 0.0))
            processed_df['relevance_score'] = df.get('Relevance_Score', 0.0)
            processed_df['relevant_to_education_in_uae'] = df.get('Relevant_to_Education_in_UAE', False)
            processed_df['sentiment_negative'] = df.get('sentiment_negative', 0.0)
            processed_df['sentiment_neutral'] = df.get('sentiment_neutral', 0.0)
            processed_df['sentiment_positive'] = df.get('sentiment_positive', 0.0)
            processed_df['combined_text'] = df.get('combined_text', '')
            processed_df['platform'] = platform_name
            
            # Extract original dates
            print("   📅 Extracting original dates...")
            if 'date' in df.columns:
                processed_df['date'] = df['date'].apply(lambda x: parse_date_safely(x, platform_name))
            elif 'Date' in df.columns:
                processed_df['date'] = df['Date'].apply(lambda x: parse_date_safely(x, platform_name))
            else:
                processed_df['date'] = datetime.now().date()
        
        # Clean the data
        processed_df = processed_df.fillna('')
        
        # Ensure numeric columns are properly formatted
        numeric_columns = ['sentiment_confidence', 'relevance_score', 'sentiment_negative', 
                          'sentiment_neutral', 'sentiment_positive']
        for col in numeric_columns:
            if col in processed_df.columns:
                processed_df[col] = pd.to_numeric(processed_df[col], errors='coerce').fillna(0.0)
        
        # Ensure boolean columns are properly formatted
        if 'relevant_to_education_in_uae' in processed_df.columns:
            processed_df['relevant_to_education_in_uae'] = processed_df['relevant_to_education_in_uae'].astype(bool)
        
        # Show date range for verification
        if not processed_df.empty and 'date' in processed_df.columns:
            date_range = f"{processed_df['date'].min()} to {processed_df['date'].max()}"
            print(f"   📅 Date range: {date_range}")
        
        print(f"   ✅ Processed {len(processed_df)} {platform_name} records")
        
        return processed_df
        
    except Exception as e:
        print(f"❌ Error processing {platform_name} data: {str(e)}")
        return None

def insert_batch(db, batch_df):
    """Insert a batch of records into the database"""
    try:
        with db.engine.connect() as conn:
            for _, row in batch_df.iterrows():
                insert_query = text("""
                INSERT INTO social_media_data 
                (title, url, summary, content, comment, comment_sentiment, author, combined_text, relevance_score, 
                 relevant_to_education_in_uae, sentiment_negative, sentiment_neutral, 
                 sentiment_positive, sentiment_predicted, sentiment_confidence, date, platform)
                VALUES (:title, :url, :summary, :content, :comment, :comment_sentiment, :author, :combined_text, :relevance_score,
                        :relevant_to_education_in_uae, :sentiment_negative, :sentiment_neutral,
                        :sentiment_positive, :sentiment_predicted, :sentiment_confidence, :date, :platform)
                """)
                
                values = {
                    'title': str(row['title'])[:500],  # Limit title length
                    'url': str(row['url'])[:500],    # Limit URL length  
                    'summary': str(row['summary'])[:1000], # Limit summary length
                    'content': str(row['content'])[:2000], # Limit content length
                    'comment': str(row.get('comment', ''))[:2000], # Limit comment length
                    'comment_sentiment': str(row.get('comment_sentiment', ''))[:50],
                    'author': str(row['author'])[:200],   # Limit author length
                    'combined_text': str(row['combined_text'])[:3000], # Limit combined text length
                    'relevance_score': float(row.get('relevance_score', 0.0)),
                    'relevant_to_education_in_uae': bool(row.get('relevant_to_education_in_uae', False)),
                    'sentiment_negative': float(row.get('sentiment_negative', 0.0)),
                    'sentiment_neutral': float(row.get('sentiment_neutral', 0.0)),
                    'sentiment_positive': float(row.get('sentiment_positive', 0.0)),
                    'sentiment_predicted': str(row['sentiment_predicted'])[:50],
                    'sentiment_confidence': float(row['sentiment_confidence']),
                    'date': row['date'],
                    'platform': str(row['platform'])
                }
                
                conn.execute(insert_query, values)
            conn.commit()
        
        return True
        
    except Exception as e:
        print(f"❌ Error inserting batch: {str(e)}")
        return False

if __name__ == "__main__":
    migrate_data_with_progress() 