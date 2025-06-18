import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
import logging
from urllib.parse import urljoin

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("kt_education_scraper_log.txt"),
        logging.StreamHandler()
    ]
)

class KhaleejtimesScraper:
    def __init__(self):
        """Initialize the Khaleej Times education scraper."""
        self.base_url = "https://www.khaleejtimes.com"
        self.education_url = "https://www.khaleejtimes.com/education"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Referer': 'https://www.khaleejtimes.com/'
        }
        self.session = requests.Session()
        self.articles = []
    
    def get_page(self, url, params=None):
        """Fetch the HTML content of a page."""
        try:
            # Add random delay to avoid overloading the server and getting blocked
            time.sleep(random.uniform(1.5, 3))
            
            response = self.session.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            return response.text
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching {url}: {e}")
            return None
    
    def parse_page(self, html):
        """Parse the HTML content using BeautifulSoup."""
        if not html:
            return None
        
        return BeautifulSoup(html, 'html.parser')
    
    def search_education_articles(self, num_pages=15):
        """
        Search for education-related articles in UAE.
        
        Args:
            num_pages: Number of pages to scrape for each search term
        """
        all_articles = []
        
        # First scrape the Education section
        logging.info(f"Scraping education section: {self.education_url}")
        html = self.get_page(self.education_url)
        soup = self.parse_page(html)
        
        if soup:
            articles = self.extract_articles(soup)
            all_articles.extend(articles)
        
        # Try to scrape archive pages of education section if they exist
        for page in range(2, num_pages + 1):
            archive_url = f"{self.education_url}/page/{page}"
            logging.info(f"Scraping education archive: {archive_url}")
            html = self.get_page(archive_url)
            if html:
                soup = self.parse_page(html)
                if soup:
                    articles = self.extract_articles(soup)
                    all_articles.extend(articles)
            else:
                # If we get an error, the archive might not exist, so break
                break
                
        # Define search terms
        search_terms = [
            "UAE education",
            "Dubai school",
            "Abu Dhabi school",
            "UAE university",
            "Dubai university",
            "Abu Dhabi university",
            "UAE student",
            "UAE teacher",
            "UAE curriculum",
            "UAE academic",
            "Sharjah education",
            "UAE college",
            "UAE scholarship",
            "UAE school fees",
            "UAE education ministry"
        ]
        
        # Search for each term
        search_url = f"{self.base_url}/search"
        
        for search_term in search_terms:
            logging.info(f"Starting search for term: {search_term}")
            
            for page in range(1, num_pages + 1):
                params = {
                    'q': search_term,
                    'page': page
                }
                
                logging.info(f"Searching for '{search_term}' - Page {page}")
                html = self.get_page(search_url, params=params)
                soup = self.parse_page(html)
                
                if soup:
                    articles = self.extract_articles(soup)
                    if not articles and page > 1:
                        # If no articles found and we're past page 1, we might be at the end
                        break
                    all_articles.extend(articles)
        
        # Remove duplicates based on URL
        unique_articles = []
        seen_urls = set()
        
        for article in all_articles:
            if article['url'] not in seen_urls:
                seen_urls.add(article['url'])
                unique_articles.append(article)
        
        logging.info(f"Found total of {len(unique_articles)} unique education articles")
        self.articles = unique_articles
        return unique_articles
    
    def extract_articles(self, soup):
        """Extract article information from the page."""
        articles = []
        
        # Looking for article containers - these selectors might need adjustment
        # based on the actual structure of Khaleej Times website
        article_containers = soup.select('article') or soup.select('.article-item') or soup.select('.story-card')
        
        if not article_containers:
            # Try alternative selectors if the above don't work
            article_containers = soup.select('.news-item') or soup.select('.card') or soup.select('.kt-article')
            
        if not article_containers:
            # Try more generic selectors as a last resort
            article_containers = soup.select('.list-item') or soup.select('.item') or soup.select('.row')
        
        logging.info(f"Found {len(article_containers)} potential articles")
        
        for container in article_containers:
            # Extract article data
            article_data = {}
            
            # Find title
            title_elem = (container.select_one('h1') or container.select_one('h2') or 
                          container.select_one('h3') or container.select_one('h4') or 
                          container.select_one('.title') or container.select_one('.headline'))
            if title_elem:
                article_data['title'] = title_elem.get_text(strip=True)
            else:
                continue  # Skip if no title found
            
            # Find link
            link_elem = title_elem.find_parent('a') if title_elem else None
            if not link_elem:
                link_elem = container.select_one('a')
                
            if link_elem and link_elem.get('href'):
                article_url = link_elem.get('href')
                if not article_url.startswith('http'):
                    article_url = urljoin(self.base_url, article_url)
                article_data['url'] = article_url
            else:
                continue  # Skip if no link found
            
            # Find date if available
            date_elem = (container.select_one('.date') or container.select_one('.time') or 
                         container.select_one('.published') or container.select_one('.timestamp'))
            if date_elem:
                article_data['date'] = date_elem.get_text(strip=True)
            else:
                article_data['date'] = None
            
            # Find summary/description if available
            summary_elem = (container.select_one('.summary') or container.select_one('.description') or 
                           container.select_one('.excerpt') or container.select_one('p'))
            if summary_elem:
                article_data['summary'] = summary_elem.get_text(strip=True)
            else:
                article_data['summary'] = None
            
            # Check if article is related to UAE education
            is_education_related = False
            is_uae_related = False
            
            # Check title and summary for education-related keywords
            education_keywords = [
                'education', 'school', 'university', 'student', 'teacher', 'academic', 
                'learning', 'study', 'scholarship', 'course', 'degree', 'classroom',
                'college', 'curriculum', 'exam', 'graduation', 'campus', 'tuition',
                'professor', 'faculty', 'kindergarten', 'nursery', 'institute', 'seminar',
                'lecture', 'workshop', 'syllabus', 'enrollment', 'dissertation', 'thesis',
                'alumni', 'graduate', 'undergraduate', 'phd', 'doctorate', 'master'
            ]
            
            uae_keywords = [
                'uae', 'dubai', 'abu dhabi', 'sharjah', 'ajman', 'rak', 'umm al quwain',
                'fujairah', 'emirates', 'ministry of education', 'adek', 'khda', 'emirati'
            ]
            
            title_lower = article_data['title'].lower()
            summary_lower = article_data['summary'].lower() if article_data['summary'] else ""
            combined_text = title_lower + " " + summary_lower
            
            for keyword in education_keywords:
                if keyword in combined_text:
                    is_education_related = True
                    break
            
            for keyword in uae_keywords:
                if keyword in combined_text:
                    is_uae_related = True
                    break
            
            # Include only if it's related to education in the UAE
            if is_education_related and is_uae_related:
                articles.append(article_data)
                logging.info(f"Found education article: {article_data['title']}")
            
        return articles
    
    def get_article_details(self):
        """Get detailed content for each article."""
        for i, article in enumerate(self.articles):
            logging.info(f"Getting details for article {i+1}/{len(self.articles)}: {article['title']}")
            
            html = self.get_page(article['url'])
            soup = self.parse_page(html)
            
            if not soup:
                continue
            
            # Find article content
            content_elem = (soup.select_one('.article-content') or soup.select_one('.story-content') or 
                           soup.select_one('.content') or soup.select_one('.article-body'))
            
            if content_elem:
                # Extract paragraphs
                paragraphs = content_elem.select('p')
                content = ' '.join([p.get_text(strip=True) for p in paragraphs])
                article['content'] = content
            else:
                article['content'] = None
            
            # Find published date if not already found
            if not article['date']:
                date_elem = (soup.select_one('.published-date') or soup.select_one('.date') or 
                            soup.select_one('.timestamp') or soup.select_one('.article-date'))
                if date_elem:
                    article['date'] = date_elem.get_text(strip=True)
            
            # Find author if available
            author_elem = soup.select_one('.author') or soup.select_one('.byline')
            if author_elem:
                article['author'] = author_elem.get_text(strip=True)
            else:
                article['author'] = None
                
            # Save incrementally after every 10 articles to avoid losing data
            if (i + 1) % 10 == 0:
                self.save_to_csv(f"khaleej_times_education_articles_partial_{i+1}.csv")
    
    def save_to_csv(self, filename="khaleej_times_education_articles.csv"):
        """Save the scraped data to a CSV file."""
        if not self.articles:
            logging.warning("No articles to save!")
            return
            
        df = pd.DataFrame(self.articles)
        df.to_csv(filename, index=False, encoding='utf-8')
        logging.info(f"Data saved to {filename}")
        
    def save_to_excel(self, filename="khaleej_times_education_articles.xlsx"):
        """Save the scraped data to an Excel file."""
        if not self.articles:
            logging.warning("No articles to save!")
            return
            
        df = pd.DataFrame(self.articles)
        df.to_excel(filename, index=False)
        logging.info(f"Data saved to {filename}")


if __name__ == "__main__":
    # Initialize the Khaleej Times scraper
    scraper = KhaleejtimesScraper()
    
    # Search for education articles (increased to 15 pages per search term)
    scraper.search_education_articles(num_pages=15)
    
    # Get detailed content for each article
    scraper.get_article_details()
    
    # Save the results
    scraper.save_to_csv()
    scraper.save_to_excel() 