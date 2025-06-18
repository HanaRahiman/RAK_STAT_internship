import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
from datetime import datetime
import re
import json
from urllib.parse import urlparse, urlunparse

# Updated headers with modern browser information
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'same-origin',
    'Sec-Fetch-User': '?1',
    'Cache-Control': 'max-age=0',
    'TE': 'trailers',
    'DNT': '1',  # Do Not Track
}

# Rotating user agents to reduce blocking risk
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1'
]

def normalize_url(url):
    """Remove fragment identifiers and clean up URL"""
    parsed = urlparse(url)
    # Remove fragment and query parameters added by text fragments
    if parsed.fragment.startswith(':~:text='):
        parsed = parsed._replace(fragment="")
    return urlunparse(parsed)

def get_headers():
    """Return headers with a random user agent"""
    headers = HEADERS.copy()
    headers['User-Agent'] = random.choice(USER_AGENTS)
    return headers

def extract_json_ld(soup):
    """Extract structured data from JSON-LD script tags"""
    try:
        script = soup.find('script', type='application/ld+json')
        if script:
            data = json.loads(script.string)
            return data
    except:
        pass
    return None

def extract_title(soup, json_data=None):
    """Extract article title with multiple fallback methods"""
    # Try from JSON-LD first
    if json_data and 'headline' in json_data:
        return json_data['headline']
    
    # Method 1: Data attribute selector
    title = soup.find('h1', attrs={'data-testid': re.compile('Heading|title', re.I)})
    if title:
        return title.get_text(strip=True)
    
    # Method 2: Class-based selector
    title_selectors = [
        'h1.article-header__title__3A1_h',
        'h1.text__text__1FZLe',  # PLUS articles
        'h1.heading__base__2T28j',
        'h1.article-heading',  # New format
        'h1.headline__heading',  # Alternate format
        'h1.company-profile__company-name__1H1u4'  # Company profiles
    ]
    
    for selector in title_selectors:
        title = soup.select_one(selector)
        if title:
            return title.get_text(strip=True)
    
    # Method 3: Generic h1 as last resort
    title = soup.find('h1')
    return title.get_text(strip=True) if title else "Title not found"

def extract_date(soup, json_data=None):
    """Extract article date with multiple fallback methods"""
    # Try from JSON-LD first
    if json_data and 'datePublished' in json_data:
        return json_data['datePublished']
    
    # Method 1: Time element with datetime attribute
    time_tag = soup.find('time', attrs={'datetime': True})
    if time_tag:
        return time_tag['datetime']
    
    # Method 2: DateLine element
    date_selectors = [
        'div[data-testid="DateLine"]',
        'div.date-line__date__23Ge-',
        'div.article-header__date__1r1v9',
        'div.date__date__1th6T',  # PLUS articles
        'div.ArticleHeader_date',
        'div.article-date',  # New format
        'div.date-line__date',  # Alternate format
        'span.article-date'  # Some articles
    ]
    
    for selector in date_selectors:
        date_div = soup.select_one(selector)
        if date_div:
            return date_div.get_text(strip=True)
    
    # Method 3: Meta tags
    meta_tags = [
        {'property': 'article:published_time'},
        {'name': 'pub_date'},
        {'name': 'date'},
        {'itemprop': 'datePublished'}
    ]
    
    for meta in meta_tags:
        meta_date = soup.find('meta', meta)
        if meta_date and meta_date.get('content'):
            return meta_date['content']
    
    return "Date not found"

def extract_author(soup, json_data=None):
    """Extract author information with multiple fallback methods"""
    # Try from JSON-LD first
    if json_data and 'author' in json_data:
        if isinstance(json_data['author'], list) and len(json_data['author']) > 0:
            return json_data['author'][0].get('name', '')
        elif isinstance(json_data['author'], dict):
            return json_data['author'].get('name', '')
    
    # Method 1: Author byline
    author_selectors = [
        'div[data-testid="AuthorByline"]',
        'div.article-header__author-name__3F3Qp',
        'a.author-name',
        'span.author',
        'div.byline__byline__1rqDg',
        'div.author',  # New format
        'span.author-name',  # Alternate format
        'div.byline__author'  # Some articles
    ]
    
    for selector in author_selectors:
        author_div = soup.select_one(selector)
        if author_div:
            return author_div.get_text(strip=True)
    
    # Method 2: Meta author tag
    meta_tags = [
        {'name': 'author'},
        {'property': 'article:author'},
        {'name': 'byl'},
        {'itemprop': 'author'}
    ]
    
    for meta in meta_tags:
        meta_author = soup.find('meta', meta)
        if meta_author and meta_author.get('content'):
            return meta_author['content']
    
    return "Author not found"

def extract_summary(soup, json_data=None):
    """Extract article summary/description"""
    # Try from JSON-LD first
    if json_data and 'description' in json_data:
        return json_data['description']
    
    # Method 1: Standfirst/description element
    summary_selectors = [
        'p[data-testid="paragraph-1"]',  # First paragraph often serves as summary
        'div.article-header__description__2XART',
        'div.standfirst',
        'div.article-body__intro__2zlsF',
        'div.summary',
        'div.article-summary',  # New format
        'p.article-dek'  # Alternate format
    ]
    
    for selector in summary_selectors:
        summary_div = soup.select_one(selector)
        if summary_div:
            return summary_div.get_text(strip=True)
    
    # Method 2: Meta description
    meta_tags = [
        {'name': 'description'},
        {'property': 'og:description'},
        {'name': 'twitter:description'}
    ]
    
    for meta in meta_tags:
        meta_desc = soup.find('meta', meta)
        if meta_desc and meta_desc.get('content'):
            return meta_desc['content']
    
    # Method 3: First meaningful paragraph
    first_paragraph = soup.select_one('div[data-testid="article-body"] p')
    if first_paragraph:
        return first_paragraph.get_text(strip=True)[:300] + "..."
    
    return "Summary not found"

def extract_content(soup):
    """Extract article content with multiple fallback methods"""
    # Standard news articles
    body_selectors = [
        'div[data-testid="article-body"]',  # Primary selector
        'div.article-body',  # New format
        'div.article-content',  # Alternate format
        'div.body',  # Generic
        'div.article-body__content__17Yit',  # PLUS articles
        'div.company-profile__body__1ZzC6'  # Company profiles
    ]
    
    for selector in body_selectors:
        body = soup.select_one(selector)
        if body:
            # Extract paragraphs
            paragraphs = body.find_all(['p', 'h2', 'h3', 'h4'])
            content = []
            for p in paragraphs:
                # Skip empty paragraphs
                if p.get_text(strip=True):
                    content.append(p.get_text(strip=True))
            return '\n\n'.join(content)
    
    # Generic fallback: Try to get all paragraphs in main content area
    main_content = soup.find('main') or soup.find('article')
    if main_content:
        paragraphs = main_content.find_all(['p', 'h2', 'h3', 'h4'])
        if paragraphs:
            content = []
            for p in paragraphs:
                if p.get_text(strip=True):
                    content.append(p.get_text(strip=True))
            return '\n\n'.join(content)
    
    return "Content not found"

def scrape_reuters_article(url):
    """Scrape individual Reuters article with enhanced error handling"""
    clean_url = normalize_url(url)
    print(f"Scraping: {clean_url}")
    
    try:
        # Create session to maintain cookies
        session = requests.Session()
        session.headers.update(get_headers())
        
        # First request to get cookies
        session.get("https://www.reuters.com/", timeout=5)
        
        # Request article with cookies
        response = session.get(clean_url, timeout=15)
        
        if response.status_code != 200:
            print(f"Failed to retrieve {clean_url} - Status code: {response.status_code}")
            return None
            
        soup = BeautifulSoup(response.content, 'html.parser')
        json_data = extract_json_ld(soup)
        
        # Extract each field with error handling
        title = extract_title(soup, json_data)
        date = extract_date(soup, json_data)
        author = extract_author(soup, json_data)
        summary = extract_summary(soup, json_data)
        content = extract_content(soup)
        
        # Clean date format
        date_match = re.search(r'\d{4}-\d{2}-\d{2}', date)
        clean_date = date_match.group(0) if date_match else date
        
        return {
            'title': title,
            'url': clean_url,
            'date': clean_date,
            'summary': summary,
            'content': content[:100000],  # Limit content length
            'author': author
        }
        
    except Exception as e:
        print(f"Error scraping {clean_url}: {str(e)}")
        return None

# List of specific UAE education articles to scrape
ARTICLE_URLS = [
    "https://www.reuters.com/article/business/uae-to-disable-all-schools-and-high-educational-institutions-for-four-weeks-wam-idUSKBN20Q2OX/",
    "https://www.reuters.com/article/world/middle-east/uae-israeli-educational-institutions-sign-artificial-intelligence-mou-wam-idUSKBN26408S/",
    "https://www.reuters.com/world/middle-east/cambridge-university-uae-talks-partnership-2021-07-07/",
    "https://www.reuters.com/world/middle-east/dubai-school-operator-taaleem-preliminary-talks-ipo-sources-2022-04-08/",
    "https://www.reuters.com/world/us-firm-safanad-co-invest-200-million-mena-education-2022-10-26/",
    "https://www.reuters.com/business/dubai-school-operator-taaleem-seeks-raise-20421-million-ipo-document-2022-10-31/",
    "https://www.reuters.com/markets/brookfield-led-consortium-invest-gems-education-2024-06-18/",
    "https://www.reuters.com/world/middle-east/uae-cabinet-approves-12-spending-increase-2025-budget-2024-10-08/",
    "https://www.reuters.com/world/middle-east/amanat-holdings-hires-bank-list-education-business-riyadh-sources-say-2024-11-27/",
    "https://www.reuters.com/world/middle-east/dubais-gems-education-plans-300-million-spend-boost-growth-2025-02-28/",
    "https://www.reuters.com/business/abu-dhabis-mubadala-buy-600-million-stake-uk-school-operator-nord-anglia-2025-04-17/"
]

def scrape_all_articles():
    """Scrape all articles from the predefined list"""
    scraped_articles = []
    failed_urls = []
    
    print(f"\n{'='*50}")
    print(f"Scraping {len(ARTICLE_URLS)} UAE education articles")
    print(f"{'='*50}")
    
    for idx, url in enumerate(ARTICLE_URLS):
        print(f"\nProcessing article {idx+1}/{len(ARTICLE_URLS)}")
        article = scrape_reuters_article(url)
        if article:
            scraped_articles.append(article)
            print(f"Scraped: {article['title'][:60]}...")
        else:
            failed_urls.append(url)
            print(f"Failed to scrape: {url}")
        
        # Random delay to avoid detection
        if idx < len(ARTICLE_URLS) - 1:
            delay = random.uniform(3, 8)
            print(f"Waiting {delay:.1f} seconds before next request...")
            time.sleep(delay)
    
    # Retry failed URLs once
    if failed_urls:
        print("\nRetrying failed URLs...")
        for url in failed_urls:
            print(f"\nRetrying: {url}")
            article = scrape_reuters_article(url)
            if article:
                scraped_articles.append(article)
                print(f"Successfully scraped on retry: {article['title'][:60]}...")
                failed_urls.remove(url)
            else:
                print(f"Still failed: {url}")
            time.sleep(5)
    
    return scraped_articles

# Main execution
if __name__ == "__main__":
    print("Starting Reuters UAE Education Article Scraper...")
    
    # Scrape all articles from the predefined list
    scraped_articles = scrape_all_articles()
    
    # Save to CSV
    if scraped_articles:
        df = pd.DataFrame(scraped_articles)
        
        # Reorder columns
        df = df[['title', 'url', 'date', 'summary', 'content', 'author']]
        
        # Generate filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"uae_education_articles_{timestamp}.csv"
        
        df.to_csv(filename, index=False)
        print(f"\nSuccessfully scraped {len(df)} articles")
        print(f"Saved to: {filename}")
        
        # Show sample output
        print("\nSample of scraped articles:")
        print(df[['title', 'date', 'author']].head())
    else:
        print("No articles scraped")
