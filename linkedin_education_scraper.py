import time
import json
import csv
import signal
import sys
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import pandas as pd
import os

class LinkedInEducationScraper:
    def __init__(self):
        self.driver = None
        self.posts_data = []
        self.interrupted = False
        
        # Set up signal handler for graceful interruption
        signal.signal(signal.SIGINT, self.signal_handler)
    
    def signal_handler(self, signum, frame):
        """Handle Ctrl+C interruption gracefully"""
        print(f"\n\n[INTERRUPTION DETECTED]")
        print("=" * 50)
        print("Saving collected data before exit...")
        
        self.interrupted = True
        
        # Try to save data from multiple possible sources
        data_to_save = []
        
        # Check instance variable first
        if hasattr(self, 'posts_data') and self.posts_data:
            data_to_save = self.posts_data
            print(f"Found {len(data_to_save)} posts in instance variable")
        
        # Always try to create a CSV file, even if data appears empty
        try:
            # Save emergency data regardless of content
            emergency_filename = self.save_emergency_data_force(data_to_save)
            print(f"Emergency save completed: {emergency_filename}")
            print(f"Attempted to save {len(data_to_save)} posts")
        except Exception as e:
            print(f"Error during emergency save: {e}")
            print(f"Raw data length: {len(data_to_save) if data_to_save else 0}")
            # Create diagnostic file
            try:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                error_filename = f"emergency_save_ERROR_{timestamp}.txt"
                with open(error_filename, 'w') as f:
                    f.write(f"Emergency save error at {datetime.now()}\n")
                    f.write(f"Error: {e}\n")
                    f.write(f"Data length: {len(data_to_save) if data_to_save else 0}\n")
                    f.write(f"Sample data: {data_to_save[:1] if data_to_save else 'No data'}\n")
                print(f"Error diagnostic created: {error_filename}")
            except Exception as diagnostic_error:
                print(f"Could not create diagnostic file: {diagnostic_error}")
        
        # Close browser if it exists
        if hasattr(self, 'driver') and self.driver:
            try:
                print("Closing browser...")
                self.driver.quit()
            except:
                pass
        
        print("Exiting gracefully...")
        sys.exit(0)
    
    def save_emergency_data(self, posts_data):
        """Save data immediately when interrupted"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        emergency_filename = f"emergency_save_{timestamp}.csv"
        
        if posts_data:
            try:
                # Try pandas first
                df = pd.DataFrame(posts_data)
                df.to_csv(emergency_filename, index=False, encoding='utf-8')
                print(f"CSV created using pandas: {emergency_filename}")
            except Exception as pd_error:
                print(f"Warning ({pd_error}), using manual CSV creation...")
                # Fallback: Manual CSV creation
                try:
                    with open(emergency_filename, 'w', newline='', encoding='utf-8') as csvfile:
                        if posts_data:
                            # Get all possible field names
                            fieldnames = set()
                            for post in posts_data:
                                fieldnames.update(post.keys())
                            fieldnames = sorted(list(fieldnames))
                            
                            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                            writer.writeheader()
                            writer.writerows(posts_data)
                    print(f"CSV created manually: {emergency_filename}")
                except Exception as csv_error:
                    print(f"Manual CSV creation failed: {csv_error}")
            
            # Also save as JSON for backup
            try:
                json_filename = f"emergency_save_{timestamp}.json"
                with open(json_filename, 'w', encoding='utf-8') as f:
                    json.dump(posts_data, f, indent=2, ensure_ascii=False)
                print(f"JSON backup created: {json_filename}")
            except Exception as json_error:
                print(f"Warning JSON backup failed: {json_error}")
            
            print(f"Emergency files completed!")
            print(f"   CSV: {emergency_filename}")
            if 'json_filename' in locals():
                print(f"   JSON: {json_filename}")
        
        return emergency_filename
    
    def save_emergency_data_force(self, posts_data):
        """Force save data immediately when interrupted - always creates CSV"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        emergency_filename = f"emergency_save_{timestamp}.csv"
        
        print(f"Debug: Attempting to save {len(posts_data) if posts_data else 0} posts")
        
        # Always create CSV file, even if empty
        try:
            with open(emergency_filename, 'w', newline='', encoding='utf-8') as csvfile:
                if posts_data and len(posts_data) > 0:
                    # Get all possible field names from all posts
                    fieldnames = set()
                    valid_posts = []
                    
                    for i, post in enumerate(posts_data):
                        if isinstance(post, dict):
                            fieldnames.update(post.keys())
                            valid_posts.append(post)
                        else:
                            print(f"Warning Post {i} is not a dictionary: {type(post)}")
                    
                    if valid_posts:
                        fieldnames = sorted(list(fieldnames))
                        print(f"Debug: Found {len(valid_posts)} valid posts with fields: {fieldnames}")
                        
                        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                        writer.writeheader()
                        writer.writerows(valid_posts)
                        print(f"CSV created with {len(valid_posts)} valid posts: {emergency_filename}")
                    else:
                        # Create empty CSV with standard headers
                        standard_headers = ['keyword', 'scraped_at', 'content_type', 'post_id', 'author_name', 'author_title', 
                                          'post_text', 'post_url', 'likes_count', 'comments_count', 
                                          'reposts_count', 'post_date', 'comment_author', 'comment_text', 'comment_date']
                        writer = csv.DictWriter(csvfile, fieldnames=standard_headers)
                        writer.writeheader()
                        print(f"Warning CSV created with headers only (no valid posts): {emergency_filename}")
                else:
                    # Create empty CSV with standard headers
                    standard_headers = ['keyword', 'scraped_at', 'content_type', 'post_id', 'author_name', 'author_title', 
                                      'post_text', 'post_url', 'likes_count', 'comments_count', 
                                      'reposts_count', 'post_date', 'comment_author', 'comment_text', 'comment_date']
                    writer = csv.DictWriter(csvfile, fieldnames=standard_headers)
                    writer.writeheader()
                    print(f"Empty CSV created (no posts collected): {emergency_filename}")
            
        except Exception as csv_error:
            print(f"CSV creation failed: {csv_error}")
            # Create a simple text file with the data dump
            try:
                text_filename = f"emergency_save_{timestamp}.txt"
                with open(text_filename, 'w', encoding='utf-8') as f:
                    f.write(f"Emergency save data dump at {datetime.now()}\n")
                    f.write("=" * 50 + "\n")
                    f.write(f"Posts data length: {len(posts_data) if posts_data else 0}\n")
                    if posts_data:
                        for i, post in enumerate(posts_data):
                            f.write(f"\nPost {i+1}:\n")
                            f.write(str(post))
                            f.write("\n" + "-" * 30 + "\n")
                print(f"Emergency text dump created: {text_filename}")
                emergency_filename = text_filename
            except Exception as text_error:
                print(f"Warning Even text dump failed: {text_error}")
        
        # Also try JSON backup
        try:
            json_filename = f"emergency_save_{timestamp}.json"
            with open(json_filename, 'w', encoding='utf-8') as f:
                json.dump(posts_data if posts_data else [], f, indent=2, ensure_ascii=False)
            print(f"JSON backup created: {json_filename}")
        except Exception as json_error:
            print(f"Warning JSON backup failed: {json_error}")
        
        return emergency_filename
        
    def setup_driver(self):
        """Setup Chrome driver with appropriate options"""
        chrome_options = Options()
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        
        try:
            self.driver = webdriver.Chrome(options=chrome_options)
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            print("Chrome driver initialized successfully")
        except Exception as e:
            print(f"Error initializing Chrome driver: {e}")
            print("Please make sure ChromeDriver is installed and in your PATH")
            return False
        return True
    
    def login_wait(self):
        """Navigate to LinkedIn login and wait for user to log in"""
        try:
            print("Navigating to LinkedIn login page...")
            self.driver.get("https://www.linkedin.com/login")
            
            print("\n" + "="*50)
            print("Please log in to your LinkedIn account")
            print("You have 2 minutes to complete the login process")
            print("The script will automatically continue after login")
            print("="*50 + "\n")
            
            # Wait for 2 minutes or until login is detected
            max_wait_time = 120  # 2 minutes
            start_time = time.time()
            
            while time.time() - start_time < max_wait_time:
                try:
                    # Check if we're on the feed page (successful login)
                    if "feed" in self.driver.current_url or "linkedin.com/in/" in self.driver.current_url:
                        print("Login detected! Continuing with scraping...")
                        return True
                    
                    # Check if we're redirected to home page
                    if self.driver.current_url == "https://www.linkedin.com/" or "linkedin.com/feed" in self.driver.current_url:
                        print("Login successful! Redirected to home page")
                        return True
                        
                except Exception:
                    pass
                
                time.sleep(2)
                remaining_time = int(max_wait_time - (time.time() - start_time))
                if remaining_time > 0:
                    print(f"Waiting for login... {remaining_time} seconds remaining", end="\r")
            
            print("\nWarning Login timeout reached. Please make sure you're logged in and try again.")
            return False
            
        except Exception as e:
            print(f"Error during login process: {e}")
            return False
    
    def search_keyword(self, keyword):
        """Search for a specific keyword on LinkedIn"""
        try:
            print(f"\nSearching for keyword: '{keyword}'")
            
            # Method 1: Try using the search box directly
            try:
                # Go to LinkedIn home/feed first
                self.driver.get("https://www.linkedin.com/feed/")
                time.sleep(2)
                
                # Find and click the search box
                search_box = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, "input[placeholder*='Search'], .search-global-typeahead__input, input[aria-label*='Search']"))
                )
                search_box.clear()
                search_box.send_keys(keyword)
                search_box.send_keys(Keys.RETURN)
                time.sleep(3)
                
                # Click on "Posts" tab if available
                try:
                    posts_tab = WebDriverWait(self.driver, 5).until(
                        EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Posts') or contains(text(), 'posts')]"))
                    )
                    posts_tab.click()
                    time.sleep(2)
                except:
                    # Try alternative selector for posts tab
                    try:
                        posts_tab = self.driver.find_element(By.CSS_SELECTOR, "button[aria-label*='Posts']")
                        posts_tab.click()
                        time.sleep(2)
                    except:
                        print("Warning Could not find Posts tab, continuing with current results")
                
            except Exception as search_box_error:
                print(f"Warning Search box method failed: {search_box_error}")
                # Fallback: Direct URL method
                search_url = f"https://www.linkedin.com/search/results/content/?keywords={keyword.replace(' ', '%20')}"
                print(f"Trying direct URL: {search_url}")
                self.driver.get(search_url)
                time.sleep(3)
            
            # Wait for any posts to load with multiple possible selectors
            post_selectors = [
                ".feed-shared-update-v2",
                ".update-components-update-v2",
                "[data-urn*='activity']",
                ".feed-shared-update-v2__description-wrapper",
                ".occludable-update",
                ".feed-shared-update-v2__content"
            ]
            
            posts_found = False
            for selector in post_selectors:
                try:
                    WebDriverWait(self.driver, 5).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                    )
                    posts_found = True
                    print(f"Posts found using selector: {selector}")
                    break
                except TimeoutException:
                    continue
            
            if not posts_found:
                print(f"Warning No posts found with standard selectors, trying alternative approach")
                # Last resort: check if any content is loaded
                time.sleep(3)
                page_content = self.driver.page_source
                if "No results" in page_content or "0 results" in page_content:
                    print(f"No search results found for '{keyword}'")
                    return False
                else:
                    print(f"Page loaded, will attempt to extract posts")
            
            print(f"Search results loaded for '{keyword}'")
            return True
            
        except Exception as e:
            print(f"Error searching for '{keyword}': {e}")
            return False
    
    def extract_post_data(self, post_element, keyword):
        """Extract data from a single post element"""
        try:
            post_data = {
                'keyword': keyword,
                'scraped_at': datetime.now().isoformat(),
                'content_type': 'post',  # New field to distinguish posts from comments
                'post_id': '',  # Will help group comments with posts
                'author_name': '',
                'author_title': '',
                'post_text': '',
                'post_url': '',
                'likes_count': '',
                'comments_count': '',
                'reposts_count': '',
                'post_date': '',
                'comment_author': '',  # For comments
                'comment_text': '',    # For comments
                'comment_date': ''     # For comments
            }
            
            print(f"     Extracting from post element: {post_element.tag_name if hasattr(post_element, 'tag_name') else 'unknown'}")  # Debug
            
            # Extract author name with multiple selectors
            author_selectors = [
                ".update-components-actor__name",
                ".feed-shared-actor__name",
                ".feed-shared-actor__name a",
                ".feed-shared-actor__name span",
                "[data-control-name='actor'] span",
                ".update-components-actor__name a span"
            ]
            
            for selector in author_selectors:
                try:
                    author_element = post_element.find_element(By.CSS_SELECTOR, selector)
                    author_text = author_element.text.strip()
                    if author_text:
                        post_data['author_name'] = author_text
                        print(f"     Author found with '{selector}': {author_text[:30]}")  # Debug
                        break
                    else:
                        print(f"     Warning Author selector '{selector}' found element but no text")  # Debug
                except NoSuchElementException:
                    print(f"     Author selector '{selector}' not found")  # Debug
                    continue
            
            if not post_data['author_name']:
                print(f"     Warning No author name found with any selector")  # Debug
            
            # Extract author title with multiple selectors
            title_selectors = [
                ".update-components-actor__description",
                ".feed-shared-actor__description",
                ".feed-shared-actor__sub-description",
                ".update-components-actor__meta .update-components-actor__description"
            ]
            
            for selector in title_selectors:
                try:
                    title_element = post_element.find_element(By.CSS_SELECTOR, selector)
                    post_data['author_title'] = title_element.text.strip()
                    if post_data['author_title']:
                        break
                except NoSuchElementException:
                    continue
            
            # Extract post text with multiple selectors
            text_selectors = [
                ".feed-shared-update-v2__description",
                ".update-components-text",
                ".feed-shared-text",
                ".feed-shared-update-v2__description .feed-shared-text",
                ".update-components-update-v2__commentary .update-components-text",
                ".feed-shared-inline-show-more-text",
                ".attributed-text-segment-list__content"
            ]
            
            for selector in text_selectors:
                try:
                    text_element = post_element.find_element(By.CSS_SELECTOR, selector)
                    text_content = text_element.text.strip()
                    if text_content and len(text_content) > 10:  # Only use if substantial content
                        post_data['post_text'] = text_content
                        print(f"     Post text found with '{selector}': {text_content[:50]}...")  # Debug
                        break
                    elif text_content:
                        print(f"     Warning Text selector '{selector}' found short text: '{text_content}'")  # Debug
                    else:
                        print(f"     Warning Text selector '{selector}' found element but no text")  # Debug
                except NoSuchElementException:
                    print(f"     Author selector '{selector}' not found")  # Debug
                    continue
            
            if not post_data['post_text']:
                print(f"     Warning No post text found with any selector")  # Debug
            
            # Try to expand "See more" if text is truncated
            if post_data['post_text'] and ("..." in post_data['post_text'] or "see more" in post_data['post_text'].lower()):
                try:
                    see_more_button = post_element.find_element(By.CSS_SELECTOR, ".feed-shared-inline-show-more-text__see-more-less-toggle, .see-more")
                    see_more_button.click()
                    time.sleep(0.5)
                    # Re-extract text after expanding
                    for selector in text_selectors:
                        try:
                            text_element = post_element.find_element(By.CSS_SELECTOR, selector)
                            expanded_text = text_element.text.strip()
                            if len(expanded_text) > len(post_data['post_text']):
                                post_data['post_text'] = expanded_text
                                break
                        except NoSuchElementException:
                            continue
                except:
                    pass
            
            # Extract post URL with multiple approaches
            url_selectors = [
                "a[data-control-name='overlay']",
                "a[href*='/posts/']",
                "a[href*='activity-']",
                ".feed-shared-control-menu__trigger"
            ]
            
            for selector in url_selectors:
                try:
                    url_element = post_element.find_element(By.CSS_SELECTOR, selector)
                    href = url_element.get_attribute('href')
                    if href and ('posts' in href or 'activity' in href):
                        post_data['post_url'] = href
                        break
                except NoSuchElementException:
                    continue
            
            # Extract engagement metrics with multiple selectors
            reaction_selectors = [
                ".social-details-social-counts__reactions-count",
                ".feed-shared-social-action-bar__reaction-count",
                "[data-control-name='reactions_count']",
                ".reactions-count"
            ]
            
            for selector in reaction_selectors:
                try:
                    reactions_element = post_element.find_element(By.CSS_SELECTOR, selector)
                    post_data['likes_count'] = reactions_element.text.strip()
                    if post_data['likes_count']:
                        break
                except NoSuchElementException:
                    continue
            
            comment_selectors = [
                ".social-details-social-counts__comments",
                ".feed-shared-social-action-bar__comment-count",
                "[data-control-name='comments_count']",
                ".comments-count"
            ]
            
            for selector in comment_selectors:
                try:
                    comments_element = post_element.find_element(By.CSS_SELECTOR, selector)
                    post_data['comments_count'] = comments_element.text.strip()
                    if post_data['comments_count']:
                        break
                except NoSuchElementException:
                    continue
            
            # Extract post date with multiple selectors
            date_selectors = [
                ".update-components-actor__sub-description",
                ".feed-shared-actor__sub-description",
                ".update-components-actor__meta time",
                ".feed-shared-actor__meta time",
                "time[datetime]"
            ]
            
            for selector in date_selectors:
                try:
                    date_element = post_element.find_element(By.CSS_SELECTOR, selector)
                    post_data['post_date'] = date_element.text.strip()
                    if post_data['post_date']:
                        break
                except NoSuchElementException:
                    continue
            
            # Final debug summary
            extracted_fields = sum(1 for field in ['author_name', 'post_text', 'author_title', 'post_url'] 
                                  if post_data.get(field, '').strip())
            print(f"     Extraction summary: {extracted_fields}/4 fields extracted")
            
            # If nothing meaningful was extracted, save HTML for debugging
            if extracted_fields == 0:
                try:
                    html_snippet = post_element.get_attribute('outerHTML')[:500]
                    print(f"     Extracted HTML snippet: {html_snippet}...")
                except:
                    print(f"     Warning Could not get HTML snippet")
            
            # Extract post ID for comment grouping
            try:
                post_urn = post_element.get_attribute('data-urn') or post_element.get_attribute('data-chameleon-result-urn')
                if post_urn:
                    post_data['post_id'] = post_urn.split(':')[-1] if ':' in post_urn else post_urn[:10]
                else:
                    post_data['post_id'] = f"post_{datetime.now().strftime('%H%M%S%f')}"
            except:
                post_data['post_id'] = f"post_{datetime.now().strftime('%H%M%S%f')}"
            
            return post_data
            
        except Exception as e:
            print(f"Warning Error extracting post data: {e}")
            # Return basic error post
            return {
                'keyword': keyword,
                'scraped_at': datetime.now().isoformat(),
                'author_name': 'EXTRACTION_ERROR',
                'author_title': '',
                'post_text': f'Error: {str(e)}',
                'post_url': '',
                'likes_count': '',
                'comments_count': '',
                'reposts_count': '',
                'post_date': ''
                         }
    
    def extract_comments(self, post_element, keyword, post_data):
        """Extract comments from a post and return them as separate entries"""
        comments = []
        
        try:
            # Try to expand comments if they're collapsed
            try:
                # Look for "Show more comments" or similar buttons
                show_comments_selectors = [
                    ".comments-comments-list__show-more-comments-button",
                    "[aria-label*='comments']",
                    "button[data-control-name*='comment']",
                    ".feed-shared-social-action-bar__action-button--comment"
                ]
                
                for selector in show_comments_selectors:
                    try:
                        comments_button = post_element.find_element(By.CSS_SELECTOR, selector)
                        if "comment" in comments_button.text.lower() or "comment" in comments_button.get_attribute('aria-label').lower():
                            self.driver.execute_script("arguments[0].click();", comments_button)
                            time.sleep(2)
                            print(f"     Clicked comments button")
                            break
                    except:
                        continue
            except Exception as comment_expand_error:
                print(f"     Warning Could not expand comments: {comment_expand_error}")
            
            # Find comment elements
            comment_selectors = [
                ".comments-comment-item",
                ".feed-shared-comments-list .comment",
                ".comments-comment-entity",
                ".feed-shared-comment",
                "[data-test-id='comment']"
            ]
            
            comment_elements = []
            for selector in comment_selectors:
                try:
                    elements = post_element.find_elements(By.CSS_SELECTOR, selector)
                    if elements:
                        comment_elements = elements
                        print(f"     Found {len(elements)} comments using selector: {selector}")
                        break
                except:
                    continue
            
            # Extract data from each comment
            for i, comment_element in enumerate(comment_elements[:10]):  # Limit to 10 comments per post
                try:
                    comment_data = post_data.copy()  # Start with post data
                    comment_data['content_type'] = 'comment'
                    
                    # Extract comment author
                    comment_author_selectors = [
                        ".comments-post-meta__name-text",
                        ".feed-shared-actor__name",
                        ".comment-author-name",
                        ".feed-shared-comment__actor-name"
                    ]
                    
                    for selector in comment_author_selectors:
                        try:
                            author_element = comment_element.find_element(By.CSS_SELECTOR, selector)
                            comment_data['comment_author'] = self.clean_text(author_element.text.strip())
                            if comment_data['comment_author']:
                                break
                        except:
                            continue
                    
                    # Extract comment text
                    comment_text_selectors = [
                        ".comments-comment-item__main-content",
                        ".feed-shared-text",
                        ".comment-text",
                        ".attributed-text-segment-list__content"
                    ]
                    
                    for selector in comment_text_selectors:
                        try:
                            text_element = comment_element.find_element(By.CSS_SELECTOR, selector)
                            comment_text = self.clean_text(text_element.text.strip())
                            if comment_text and len(comment_text) > 3:
                                comment_data['comment_text'] = comment_text
                                break
                        except:
                            continue
                    
                    # Extract comment date
                    comment_date_selectors = [
                        ".comments-comment-item__timestamp",
                        ".feed-shared-actor__sub-description time",
                        ".comment-timestamp"
                    ]
                    
                    for selector in comment_date_selectors:
                        try:
                            date_element = comment_element.find_element(By.CSS_SELECTOR, selector)
                            comment_data['comment_date'] = self.clean_text(date_element.text.strip())
                            if comment_data['comment_date']:
                                break
                        except:
                            continue
                    
                    # Only add comment if we got meaningful data
                    if comment_data.get('comment_author') or comment_data.get('comment_text'):
                        comments.append(comment_data)
                        print(f"       Comment {i+1}: {comment_data.get('comment_author', 'Unknown')[:20]} - {comment_data.get('comment_text', '')[:30]}...")
                    
                except Exception as comment_error:
                    print(f"       Warning Error extracting comment {i+1}: {comment_error}")
                    continue
            
        except Exception as e:
            print(f"     Warning Error extracting comments: {e}")
        
        print(f"     Extracted {len(comments)} comments for this post")
        return comments
    
    def clean_text(self, text):
        """Clean text to fix encoding issues"""
        if not text:
            return text
        
        try:
            # Fix common encoding issues
            text = text.encode('utf-8').decode('utf-8')
            
            # Replace problematic characters
            replacements = {
                'â€™': "'",
                'â€œ': '"',
                'â€': '"',
                'â€¦': '...',
                'â€"': '—',
                'â€"': '–'
            }
            
            for old, new in replacements.items():
                text = text.replace(old, new)
            
            # Remove or replace any remaining problematic characters
            text = text.encode('ascii', 'ignore').decode('ascii') if any(ord(c) > 127 for c in text) else text
            
        except Exception as e:
            print(f"     Warning Text cleaning error: {e}")
            # Fallback: just remove non-ASCII characters
            text = ''.join(char for char in text if ord(char) < 128)
        
        return text
     
    def scrape_posts(self, keyword, max_posts=40):
        """Scrape posts for a specific keyword"""
        posts_collected = []
        scroll_pause_time = 3
        last_height = self.driver.execute_script("return document.body.scrollHeight")
        no_new_posts_count = 0
        
        print(f"Starting to scrape {max_posts} posts for '{keyword}'...")
        
        while len(posts_collected) < max_posts and not self.interrupted:
            try:
                # Multiple selectors to find post elements
                post_selectors = [
                    ".feed-shared-update-v2",
                    ".update-components-update-v2",
                    "[data-urn*='activity']",
                    ".occludable-update",
                    "[data-chameleon-result-urn]"
                ]
                
                post_elements = []
                for selector in post_selectors:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    if elements:
                        post_elements = elements
                        print(f"Found {len(elements)} posts using selector: {selector}")
                        break
                
                if not post_elements:
                    print(f"Warning No post elements found with any selector")
                    print(f"Current URL: {self.driver.current_url}")
                    print(f"Page title: {self.driver.title}")
                    
                    # Take a screenshot and save HTML for debugging
                    try:
                        debug_timestamp = datetime.now().strftime('%H%M%S')
                        screenshot_name = f"debug_{keyword.replace(' ', '_')[:20]}_{debug_timestamp}.png"
                        self.driver.save_screenshot(screenshot_name)
                        print(f"Screenshot saved: {screenshot_name}")
                        
                        # Also save page HTML
                        html_name = f"debug_{keyword.replace(' ', '_')[:20]}_{debug_timestamp}.html"
                        with open(html_name, 'w', encoding='utf-8') as f:
                            f.write(self.driver.page_source)
                        print(f"HTML saved: {html_name}")
                    except Exception as debug_error:
                        print(f"Warning Debug file save failed: {debug_error}")
                    
                    print(f"Warning Scrolling and trying again...")
                    self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    time.sleep(scroll_pause_time)
                    continue
                
                previous_count = len(posts_collected)
                
                # Extract data from new posts
                for i, post_element in enumerate(post_elements):
                    if len(posts_collected) >= max_posts:
                        break
                    
                    # Skip posts we've already processed
                    if i < previous_count:
                        continue
                    
                    try:
                        post_data = self.extract_post_data(post_element, keyword)
                        print(f"   Raw post data: {post_data}")  # Debug line
                        
                        if post_data:
                            # Clean all text fields to fix encoding issues
                            text_fields = ['author_name', 'author_title', 'post_text']
                            for field in text_fields:
                                if post_data.get(field):
                                    post_data[field] = self.clean_text(post_data[field])
                            
                            # Always collect post data, even if minimal
                            # Check for any meaningful content
                            has_content = (
                                post_data.get('post_text', '').strip() or 
                                post_data.get('author_name', '').strip() or
                                post_data.get('author_title', '').strip() or
                                post_data.get('post_url', '').strip()
                            )
                            
                            if has_content:
                                # Avoid duplicates by checking if we already have this post
                                is_duplicate = False
                                for existing_post in posts_collected:
                                    if (existing_post.get('post_text', '') == post_data.get('post_text', '') and 
                                        existing_post.get('author_name', '') == post_data.get('author_name', '') and
                                        post_data.get('post_text', '')):  # Only check for duplicates if there's actual text
                                        is_duplicate = True
                                        break
                                
                                if not is_duplicate:
                                    posts_collected.append(post_data)
                                    author_display = post_data.get('author_name', 'Unknown')[:30] or 'No author'
                                    text_preview = post_data.get('post_text', 'No text')[:50] or 'No text'
                                    print(f"   Post {len(posts_collected)}/{max_posts} - Author: {author_display} | Text: {text_preview}...")
                                    
                                    # Extract comments for this post
                                    try:
                                        comments = self.extract_comments(post_element, keyword, post_data)
                                        if comments:
                                            posts_collected.extend(comments)
                                            print(f"   Added {len(comments)} comments to collection")
                                    except Exception as comment_error:
                                        print(f"   Warning Comment extraction failed: {comment_error}")
                                else:
                                    print(f"   Skipped duplicate post")
                            else:
                                # Still collect it but mark as empty
                                posts_collected.append(post_data)
                                print(f"   Post {len(posts_collected)}/{max_posts} - EMPTY POST (collected anyway)")
                    except Exception as post_error:
                        print(f"   Warning Error processing individual post: {post_error}")
                        # Create empty post entry to maintain count
                        empty_post = {
                            'keyword': keyword,
                            'scraped_at': datetime.now().isoformat(),
                            'content_type': 'post',
                            'post_id': f'error_{datetime.now().strftime("%H%M%S%f")}',
                            'author_name': 'ERROR',
                            'author_title': '',
                            'post_text': f'Error extracting post: {post_error}',
                            'post_url': '',
                            'likes_count': '',
                            'comments_count': '',
                            'reposts_count': '',
                            'post_date': '',
                            'comment_author': '',
                            'comment_text': '',
                            'comment_date': ''
                        }
                        posts_collected.append(empty_post)
                        print(f"   Post {len(posts_collected)}/{max_posts} - ERROR POST (collected for debugging)")
                        continue
                
                # Check if we got new posts
                if len(posts_collected) == previous_count:
                    no_new_posts_count += 1
                    print(f"   No new posts found (attempt {no_new_posts_count}/3)")
                    if no_new_posts_count >= 3:
                        print(f"Warning No new posts after 3 attempts for '{keyword}'. Collected {len(posts_collected)} posts.")
                        break
                else:
                    no_new_posts_count = 0
                
                # Scroll down to load more posts
                if len(posts_collected) < max_posts:
                    print(f"   Scrolling to load more posts...")
                    self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    time.sleep(scroll_pause_time)
                    
                    # Check if we've reached the bottom
                    new_height = self.driver.execute_script("return document.body.scrollHeight")
                    if new_height == last_height:
                        print(f"Warning Reached end of page for '{keyword}'. Collected {len(posts_collected)} posts.")
                        break
                    last_height = new_height
                    
            except Exception as e:
                print(f"Error during scraping for '{keyword}': {e}")
                break
        
        print(f"Completed scraping for '{keyword}': {len(posts_collected)} posts collected")
        return posts_collected
    
    def save_data(self, all_posts_data, filename_prefix="uae_education_data"):
        """Save scraped data to multiple formats"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Save as JSON
        json_filename = f"{filename_prefix}_{timestamp}.json"
        with open(json_filename, 'w', encoding='utf-8') as f:
            json.dump(all_posts_data, f, indent=2, ensure_ascii=False)
        print(f"Data saved to JSON: {json_filename}")
        
        # Save as CSV
        csv_filename = f"{filename_prefix}_{timestamp}.csv"
        if all_posts_data:
            df = pd.DataFrame(all_posts_data)
            df.to_csv(csv_filename, index=False, encoding='utf-8-sig')  # utf-8-sig fixes Excel encoding
            print(f"Data saved to CSV: {csv_filename}")
        
        # Save summary
        summary_filename = f"{filename_prefix}_summary_{timestamp}.txt"
        with open(summary_filename, 'w', encoding='utf-8') as f:
            f.write(f"LinkedIn Education Data Scraping Summary\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Total entries collected: {len(all_posts_data)}\n\n")
            
            # Count posts vs comments
            posts_count = len([item for item in all_posts_data if item.get('content_type') == 'post'])
            comments_count = len([item for item in all_posts_data if item.get('content_type') == 'comment'])
            
            f.write(f"Posts collected: {posts_count}\n")
            f.write(f"Comments collected: {comments_count}\n\n")
            
            # Group by keyword
            keyword_counts = {}
            for item in all_posts_data:
                keyword = item['keyword']
                if keyword not in keyword_counts:
                    keyword_counts[keyword] = {'posts': 0, 'comments': 0}
                if item.get('content_type') == 'post':
                    keyword_counts[keyword]['posts'] += 1
                else:
                    keyword_counts[keyword]['comments'] += 1
            
            f.write("Data per keyword:\n")
            for keyword, counts in keyword_counts.items():
                f.write(f"  - {keyword}: {counts['posts']} posts, {counts['comments']} comments\n")
        
        print(f"Summary saved: {summary_filename}")
        return json_filename, csv_filename, summary_filename
    
    def cleanup_temp_files(self):
        """Clean up temporary progress files"""
        try:
            temp_files = [f for f in os.listdir('.') if f.startswith('temp_progress_')]
            if temp_files:
                print(f"\nCleaning up {len(temp_files)} temporary files...")
                for temp_file in temp_files:
                    os.remove(temp_file)
                    print(f"    Removed: {temp_file}")
                print("Cleanup completed")
        except Exception as e:
            print(f"Warning Cleanup warning: {e}")
    
    def run_scraping(self, keywords):
        """Main method to run the complete scraping process"""
        if not self.setup_driver():
            return False
        
        try:
            # Step 1: Login wait
            if not self.login_wait():
                print("Warning Login failed or timed out")
                return False
            
            # Initialize data storage in instance variable for signal handler access
            self.posts_data = []
            
            # Step 2: Process each keyword with progress tracking
            total_keywords = len(keywords)
            print(f"\nStarting to process {total_keywords} keywords")
            print(f"Target: {total_keywords * 40} total posts (40 per keyword)")
            print(f"Warning Press Ctrl+C at any time to stop and save collected data")
            print(f"Debug: Instance variable initialized with {len(self.posts_data)} posts")
            
            for i, keyword in enumerate(keywords, 1):
                # Check if we've been interrupted
                if self.interrupted:
                    break
                print(f"\n{'='*70}")
                print(f"Searching for keyword {i}/{total_keywords}: '{keyword}'")
                print(f"Progress: {((i-1)/total_keywords)*100:.1f}% complete")
                print(f"Posts collected so far: {len(self.posts_data)}")
                print(f"{'='*70}")
                
                if self.search_keyword(keyword):
                    posts = self.scrape_posts(keyword, max_posts=40)
                    self.posts_data.extend(posts)
                    
                    print(f"Completed keyword '{keyword}': {len(posts)} posts added")
                    print(f"Posts collected: {len(self.posts_data)}")
                    
                    # Debug: Confirm data is being stored
                    if len(self.posts_data) > 0:
                        print(f"Debug: Last post author: {self.posts_data[-1].get('author_name', 'Unknown')[:30]}...")
                    
                    # Check for interruption after each keyword
                    if self.interrupted:
                        break
                    
                    # Longer delay for larger batches to avoid rate limiting
                    if i < len(keywords):
                        delay_time = 5 if total_keywords > 30 else 3
                        print(f"Waiting {delay_time} seconds before next keyword...")
                        time.sleep(delay_time)
                else:
                    print(f"Warning Skipping keyword '{keyword}' due to search error")
                    
                # Save intermediate progress every 10 keywords for large batches
                if total_keywords > 20 and i % 10 == 0:
                    temp_filename = f"temp_progress_{i}keywords_{datetime.now().strftime('%H%M%S')}.json"
                    with open(temp_filename, 'w', encoding='utf-8') as f:
                        json.dump(self.posts_data, f, indent=2, ensure_ascii=False)
                    print(f"Intermediate progress saved: {temp_filename}")
                    
                    # Also save CSV backup every 10 keywords
                    temp_csv_filename = f"temp_progress_{i}keywords_{datetime.now().strftime('%H%M%S')}.csv"
                    if self.posts_data:
                        df = pd.DataFrame(self.posts_data)
                        df.to_csv(temp_csv_filename, index=False, encoding='utf-8')
                        print(f"CSV backup saved: {temp_csv_filename}")
            
            # Step 3: Save all collected data
            if self.interrupted:
                print(f"\n{'='*70}")
                print("Warning SCRAPING INTERRUPTED BY USER")
                print(f"{'='*70}")
            else:
                print(f"\n{'='*70}")
                print("Completed!")
                print(f"{'='*70}")
            
            print(f"Final Results:")
            print(f"   • Keywords processed: {i if 'i' in locals() else 0}")
            print(f"   • Total posts collected: {len(self.posts_data)}")
            if len(self.posts_data) > 0 and 'i' in locals() and i > 0:
                print(f"   • Average posts per keyword: {len(self.posts_data)/i:.1f}")
            
            if self.posts_data:
                if self.interrupted:
                    print(f"\nSaving interrupted session data...")
                    emergency_filename = self.save_emergency_data(self.posts_data)
                    print(f"\nFiles created (due to interruption):")
                    print(f"   CSV: {emergency_filename}")
                else:
                    json_file, csv_file, summary_file = self.save_data(self.posts_data)
                    print(f"\nFiles created:")
                    print(f"   JSON: {json_file}")
                    print(f"   CSV: {csv_file}")
                    print(f"   Summary: {summary_file}")
                
                # Clean up temporary files
                self.cleanup_temp_files()
            else:
                print("Warning No data was collected")
            
            return True
            
        except Exception as e:
            print(f"Warning Unexpected error during scraping: {e}")
            return False
        
        finally:
            # Cleanup
            if self.driver:
                print("\nClosing browser...")
                self.driver.quit()

def main():
    """Main function to run the scraper with user-defined keywords"""
    print("LinkedIn UAE Education Data Scraper")
    print("="*50)
    print("Warning  IMPORTANT: Press Ctrl+C at any time to stop and save collected data")
    print("="*50)
    
    # Comprehensive keywords for UAE education
    default_keywords = [
        # Education System & Schools
        "UAE education system", "Best schools in Dubai", "Best schools in Abu Dhabi",
        "UAE school rankings", "KHDA ratings", "ADEK inspections", "KHDA",
        "UAE public vs private schools", "UAE school holidays", "UAE school calendar",
        
        # Higher Education & Universities
        "Best universities in UAE", "UAE university rankings", "Study in Dubai",
        "Study in Abu Dhabi", "UAE scholarships", "UAE student visa",
        "Masters degree UAE", "PhD in UAE", "MBBS in UAE", "Engineering universities UAE",
        "Business schools UAE",
        
        # School Fees & Costs
        "Dubai school fees", "Abu Dhabi school fees", "Most expensive schools UAE",
        "Cheapest schools UAE", "UAE education cost", "School fee increase UAE",
        "School payment plans UAE", "Is UAE education worth it?", "School fees expensive",
        
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
    
    print(f"Comprehensive UAE education keywords ({len(default_keywords)} total):")
    print("Categories: School Rankings, Universities, Fees, Curriculum, Policies, Exams, etc.")
    
    print("\nFirst 10 keywords preview:")
    for i, keyword in enumerate(default_keywords[:10], 1):
        print(f"  {i}. {keyword}")
    print(f"  ... and {len(default_keywords) - 10} more")
    
    print("\nOptions:")
    print("1. Use all comprehensive keywords (recommended)")
    print("2. Use first 20 keywords only (faster)")
    print("3. Enter custom keywords")
    
    choice = input("\nEnter your choice (1, 2, or 3): ").strip()
    
    if choice == "2":
        keywords = default_keywords[:20]
        print(f"Using first 20 keywords for faster processing")
    elif choice == "3":
        print("\nEnter your keywords (one per line, press Enter twice when done):")
        keywords = []
        while True:
            keyword = input().strip()
            if keyword == "":
                break
            keywords.append(keyword)
        
        if not keywords:
            print("No keywords provided. Using all comprehensive keywords.")
            keywords = default_keywords
    else:
        keywords = default_keywords
    
    print(f"\nWill scrape 40 posts for each of these {len(keywords)} keywords:")
    for i, keyword in enumerate(keywords, 1):
        print(f"  {i}. {keyword}")
    
    print(f"\nExpected total posts: {len(keywords) * 40}")
    print("Warning  Remember: Press Ctrl+C anytime to stop and save progress!")
    
    input("\nPress Enter to start scraping...")
    
    # Run the scraper
    scraper = LinkedInEducationScraper()
    success = scraper.run_scraping(keywords)
    
    if success:
        print("\nCompleted successfully!")
    else:
        print("\nWarning Scraping failed or was interrupted.")

if __name__ == "__main__":
    main() 
