import praw
import pandas as pd
from datetime import datetime
import time

# Reddit API Setup
reddit = praw.Reddit(
    client_id="bpYCk-R5HBSM6ZrfkkXvFw",
    client_secret="1JSX89z80Qb3XMlQD-lAktAP_vKKIQ",
    user_agent="windows:UAE_EDUCATION_SCRAPER:v2.0 (by /u/Funny_Adogio2480)"
)

# ===== EXPANDED KEYWORD LIST (CATEGORIZED) =====
EDU_KEYWORDS = [
    # General Education
    "UAE education system", "Best schools in Dubai", "Best schools in Abu Dhabi",
    "UAE school rankings", "KHDA ratings", "ADEK inspections",
    "UAE public vs private schools", "UAE school holidays", "UAE school calendar",
    
    # Higher Education & Universities
    "Best universities in UAE", "UAE university rankings", "Study in Dubai",
    "Study in Abu Dhabi", "UAE scholarships", "UAE student visa",
    "Masters degree UAE", "PhD in UAE", "MBBS in UAE", "Engineering universities UAE",
    "Business schools UAE",
    
    # School Fees & Costs
    "Dubai school fees", "Abu Dhabi school fees", "Most expensive schools UAE",
    "Cheapest schools UAE", "UAE education cost", "School fee increase UAE",
    "School payment plans UAE", "Is UAE education worth it?",
    
    # Curriculum & Teaching
    "IB schools UAE", "British curriculum UAE", "American schools UAE",
    "CBSE schools UAE", "Indian schools Dubai", "UAE MoE curriculum",
    "UAE homeschool options", "UAE online learning", "UAE teacher salaries",
    "UAE tutoring services",
    
    # Parent & Student Concerns
    "UAE school admissions", "Waiting lists Dubai schools", "School transfers UAE",
    "Bullying in UAE schools", "UAE school bus services", "UAE school uniforms",
    "After-school activities UAE", "Best nurseries in Dubai", "UAE parent reviews",
    
    # Education Policies & Reforms
    "UAE education law", "New school regulations UAE", "UAE Emiratization in schools",
    "UAE education future", "UAE 2030 education plan", "AI in UAE schools",
    "UAE coding in schools", "UAE STEM education",
    
    # Exams & Assessments
    "UAE standardized testing", "PISA results UAE", "TIMSS UAE", "UAE board exams",
    "IGCSE UAE", "A-levels Dubai", "SAT centers UAE", "IELTS UAE",
    
    # Specialized Education
    "UAE special needs schools", "Autism schools Dubai", "Gifted programs UAE",
    "Vocational training UAE", "UAE adult education", "UAE part-time degrees"
]

# ===== EXPANDED SUBREDDITS =====
SUBREDDITS = [
    'dubai', 'UAE', 'abudhabi', 'Internationalteachers', 'expats', 
    'parenting', 'Teachers', 'HigherEducation'
]

def scrape_reddit_education(keywords, subreddits, limit_per_keyword=15):
    posts_data = []
    total_requests = 0
    
    for keyword in keywords:
        for subreddit in subreddits:
            try:
                print(f"Scraping: '{keyword}' in r/{subreddit}...")
                
                # Search posts with error handling
                for submission in reddit.subreddit(subreddit).search(
                    query=keyword, 
                    limit=limit_per_keyword, 
                    time_filter="year"  # Focus on recent posts
                ):
                    # Avoid rate limits (pause every 10 requests)
                    total_requests += 1
                    if total_requests % 10 == 0:
                        time.sleep(2)  # Respect API limits
                    
                    # Get top 3 comments (skip if comments are disabled)
                    comments = []
                    if not submission.comments_disabled:
                        submission.comments.replace_more(limit=0)
                        comments = [
                            {"author": str(c.author), "text": c.body, "upvotes": c.score}
                            for c in submission.comments[:3]
                        ]
                    
                    # Store post data
                    posts_data.append({
                        "keyword": keyword,
                        "subreddit": subreddit,
                        "title": submission.title,
                        "author": str(submission.author),
                        "upvotes": submission.score,
                        "url": submission.url,
                        "created_utc": datetime.fromtimestamp(submission.created_utc),
                        "text": submission.selftext,
                        "comments": comments
                    })
                
            except Exception as e:
                print(f"ERROR in r/{subreddit} for '{keyword}': {str(e)}")
                time.sleep(5)  # Pause on errors
    
    return pd.DataFrame(posts_data)

# Run scraper
print("Starting Reddit scrape...")
df = scrape_reddit_education(EDU_KEYWORDS, SUBREDDITS)

# Save to CSV (with timestamp)
timestamp = datetime.now().strftime("%Y%m%d_%H%M")
filename = f"uae_education_reddit3_{timestamp}.csv"
df.to_csv(filename, index=False)
print(f"Saved {len(df)} posts to '{filename}'")

# Preview
print("\nSample Data:")
print(df[['keyword', 'subreddit', 'title', 'upvotes']].head(10))
