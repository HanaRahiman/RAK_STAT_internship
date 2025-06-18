import requests
from bs4 import BeautifulSoup
import csv
import time
import random
import re
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import os
import json
from selenium.webdriver.common.keys import Keys

def setup_driver():
    """
    Set up and return a Chrome WebDriver instance
    """
    chrome_options = Options()
    # Run in non-headless mode so you can see the browser and log in manually
    # chrome_options.add_argument("--headless")  # Run in headless mode
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-notifications")
    chrome_options.add_argument("--disable-popup-blocking")
    chrome_options.add_argument("--disable-infobars")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--ignore-certificate-errors")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    
    # Set language to English explicitly
    chrome_options.add_argument("--lang=en-US")
    chrome_options.add_experimental_option('prefs', {'intl.accept_languages': 'en-US,en'})
    
    # Add a realistic user agent
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36")
    
    # Initialize the driver
    try:
        driver = webdriver.Chrome(options=chrome_options)
        return driver
    except Exception as e:
        print(f"Error setting up Chrome WebDriver: {str(e)}")
        print("Make sure you have Chrome and ChromeDriver installed.")
        return None

def clean_text(text):
    """Clean text by removing extra whitespace, user attribution patterns, and dates"""
    if not text:
        return ""
    
    # Remove user attribution patterns like "User's answer to..."
    text = re.sub(r"[A-Za-z0-9\s]+'s answer to ", "", text)
    
    # Remove date patterns (various formats)
    text = re.sub(r'\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]* \d{1,2}(?:,? \d{4})?', '', text)
    text = re.sub(r'\d{1,2}/\d{1,2}/\d{2,4}', '', text)
    text = re.sub(r'\d{4}-\d{2}-\d{2}', '', text)
    
    # Remove common author indicators
    text = re.sub(r'(?:Written|Answered) by:? [A-Za-z\s\.]+', '', text)
    text = re.sub(r'(?:Updated|Posted|Published):? [A-Za-z0-9\s,\.]+', '', text)
    text = re.sub(r'(?:Author|Writer):? [A-Za-z\s\.]+', '', text)
    
    # Remove extra whitespace
    text = re.sub(r'\s+', ' ', text).strip()
    
    return text

def extract_clean_question(url):
    """Extract clean question from URL or title"""
    # Try to extract the question from the URL
    match = re.search(r'/([^/]+)(?:/answer/|$)', url)
    if match:
        question = match.group(1)
        # Replace hyphens with spaces and decode URL encoding
        question = question.replace('-', ' ')
        question = re.sub(r'\b[A-Z][a-z]*\b-\d+$', '', question)  # Remove username pattern at end
        
        # Clean up common URL patterns
        question = question.replace('q ', '')
        
        # Replace URL encoding
        question = question.replace('%20', ' ')
        question = question.replace('%27', "'")
        question = question.replace('%22', '"')
        question = question.replace('%3F', '?')
        
        return question.strip()
    return None

def get_question_content(driver):
    """Extract the question title and full content separately"""
    title = ""
    details = ""
    
    try:
        # First try to get the question title (usually in h1)
        title_element = None
        try:
            title_element = WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.TAG_NAME, "h1"))
            )
        except:
            # Try other heading tags if h1 not found
            for tag in ["h2", "h3"]:
                try:
                    title_element = driver.find_element(By.TAG_NAME, tag)
                    break
                except:
                    pass
        
        if title_element:
            title = title_element.text.strip()
            
            # Try to find question details/description that might follow the title
            try:
                # Look for question details container
                details_element = None
                
                # Method 1: Look for elements with question-related classes
                try:
                    details_element = driver.find_element(By.CSS_SELECTOR, 
                        "div[class*='question-details'], div[class*='question_text'], div[class*='question-text']")
                except:
                    pass
                
                # Method 2: Look for paragraphs near the title
                if not details_element:
                    try:
                        # Find the parent of the title
                        parent = driver.execute_script("return arguments[0].parentNode;", title_element)
                        # Look for paragraphs within this parent or its next siblings
                        paragraphs = parent.find_elements(By.TAG_NAME, "p")
                        if paragraphs:
                            details_text = " ".join([p.text.strip() for p in paragraphs if p.text.strip()])
                            if details_text:
                                details = details_text
                    except:
                        pass
                
                # If we found a details element, add its text
                if details_element and details_element.text.strip():
                    details = details_element.text.strip()
                    
            except Exception as e:
                print(f"Error getting question details: {str(e)}")
    
    except Exception as e:
        print(f"Error extracting question content: {str(e)}")
    
    return clean_text(title), clean_text(details)

def clean_answer(answer_text):
    """Clean answer text to remove author information, dates, and other metadata"""
    if not answer_text:
        return ""
    
    # Remove lines likely to contain author info or dates
    lines = answer_text.split('\n')
    cleaned_lines = []
    
    for line in lines:
        # Skip lines that likely contain author info or dates
        if (re.search(r'\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]* \d{1,2}(?:,? \d{4})?', line) or
            re.search(r'\d{1,2}/\d{1,2}/\d{2,4}', line) or
            re.search(r'\d{4}-\d{2}-\d{2}', line) or
            re.search(r'(?:Written|Answered) by:? [A-Za-z\s\.]+', line) or
            re.search(r'(?:Updated|Posted|Published):? [A-Za-z0-9\s,\.]+', line) or
            re.search(r'(?:Author|Writer):? [A-Za-z\s\.]+', line) or
            re.search(r'\d+ views', line, re.IGNORECASE) or
            re.search(r'\d+ upvotes?', line, re.IGNORECASE) or
            len(line.strip()) < 15):  # Skip very short lines that might be metadata
            continue
        
        cleaned_lines.append(line)
    
    return '\n'.join(cleaned_lines).strip()

def click_more_answers_button(driver):
    """Try to click on 'More Answers' or similar buttons to load additional answers"""
    try:
        # Look for buttons that might load more answers
        more_buttons = driver.find_elements(By.XPATH, 
            "//button[contains(text(), 'More') or contains(text(), 'more') or contains(text(), 'Show') or contains(text(), 'Load')]")
        
        for button in more_buttons:
            try:
                if button.is_displayed() and button.is_enabled():
                    print("  - Clicking button to load more answers...")
                    driver.execute_script("arguments[0].click();", button)
                    time.sleep(2)  # Wait for content to load
            except:
                pass
                
        # Also try clicking on "View more answers" links
        more_links = driver.find_elements(By.XPATH, 
            "//a[contains(text(), 'More') or contains(text(), 'more') or contains(text(), 'Show') or contains(text(), 'Load') or contains(text(), 'View')]")
        
        for link in more_links:
            try:
                if link.is_displayed():
                    print("  - Clicking link to load more answers...")
                    driver.execute_script("arguments[0].click();", link)
                    time.sleep(2)  # Wait for content to load
            except:
                pass
    except Exception as e:
        print(f"  - Error clicking more answers button: {str(e)}")

def extract_all_answers(driver):
    """Extract all answers from the page using multiple methods"""
    all_answers = []
    
    # Method 1: Look for answer containers with specific classes
    try:
        # First try to find elements with class containing "answer"
        answer_elements = driver.find_elements(By.CSS_SELECTOR, 
            "div[class*='Answer'], div[class*='answer'], div[class*='AnswerBase']")
        
        for element in answer_elements:
            # Skip very small elements that might be UI components
            if element.size['height'] < 50:
                continue
                
            answer_text = clean_answer(element.text)
            if answer_text and len(answer_text) > 50:  # Minimum length to be considered an answer
                all_answers.append(answer_text)
    except Exception as e:
        print(f"Method 1 error: {str(e)}")
    
    # Method 2: Look for answer content by structure
    try:
        # Find all divs that might contain answers
        potential_answers = driver.find_elements(By.XPATH, 
            "//div[contains(@class, 'q-box') and contains(@class, 'qu-pt--medium')]")
        
        for div in potential_answers:
            # Skip elements that are too small
            if div.size['height'] < 100:
                continue
                
            answer_text = clean_answer(div.text)
            if answer_text and len(answer_text) > 100:  # More strict length requirement
                if answer_text not in all_answers:  # Avoid duplicates
                    all_answers.append(answer_text)
    except Exception as e:
        print(f"Method 2 error: {str(e)}")
    
    # Method 3: Look for substantial paragraphs
    try:
        # Find all paragraphs
        paragraphs = driver.find_elements(By.TAG_NAME, "p")
        
        # Group consecutive paragraphs that might form a single answer
        current_answer = []
        
        for p in paragraphs:
            p_text = p.text.strip()
            if p_text and len(p_text) > 30:  # Substantial paragraph
                current_answer.append(p_text)
            elif current_answer:  # End of an answer group
                full_answer = " ".join(current_answer)
                if len(full_answer) > 100:  # Minimum length for grouped paragraphs
                    clean_full_answer = clean_answer(full_answer)
                    if clean_full_answer not in all_answers:  # Avoid duplicates
                        all_answers.append(clean_full_answer)
                current_answer = []
        
        # Add the last answer if there is one
        if current_answer:
            full_answer = " ".join(current_answer)
            if len(full_answer) > 100:
                clean_full_answer = clean_answer(full_answer)
                if clean_full_answer not in all_answers:  # Avoid duplicates
                    all_answers.append(clean_full_answer)
    except Exception as e:
        print(f"Method 3 error: {str(e)}")
    
    # Method 4: Look for comments
    try:
        # Find comment sections
        comment_sections = driver.find_elements(By.CSS_SELECTOR, 
            "div[class*='comment'], div[class*='Comment']")
        
        for section in comment_sections:
            comment_text = clean_answer(section.text)
            if comment_text and len(comment_text) > 30:  # Comments can be shorter
                if comment_text not in all_answers:  # Avoid duplicates
                    all_answers.append(comment_text)
    except Exception as e:
        print(f"Method 4 error: {str(e)}")
    
    return all_answers

def handle_login_wall(driver, disable_screenshots=False):
    """Try to bypass or handle Quora's login wall"""
    print("Attempting to handle login wall...")
    
    # Take a screenshot to help diagnose what's on the page
    if not disable_screenshots:
        driver.save_screenshot("current_page.png")
        print("Saved screenshot to current_page.png for diagnosis")
    
    # More specific check for login elements
    login_elements = False
    try:
        # Look for specific login-related elements
        login_buttons = driver.find_elements(By.XPATH, 
            "//button[contains(text(), 'Log In') or contains(text(), 'Sign Up') or contains(text(), 'Continue with Google')]")
        login_inputs = driver.find_elements(By.XPATH, 
            "//input[@type='email' or @type='password']")
        
        if login_buttons or login_inputs:
            login_elements = True
            print("Login elements detected on the page")
    except:
        pass
    
    # Check if we're on a login page
    if (login_elements or 
        "login" in driver.current_url.lower() or 
        "signup" in driver.current_url.lower()):
        
        print("\n" + "="*50)
        print("LOGIN PAGE DETECTED!")
        print("Please log in manually in the browser window.")
        print("The script will wait for 3 minutes (180 seconds) for you to complete the login.")
        print("If this is NOT a login page, press Ctrl+C in the terminal to stop the script.")
        print("="*50 + "\n")
        
        # Wait for manual login (180 seconds)
        try:
            # Wait for 180 seconds for manual login
            for i in range(18):  # 18 x 10 seconds = 180 seconds
                print(f"Waiting for login... {(i+1)*10} seconds passed (press Ctrl+C to stop)")
                time.sleep(10)
            
            # Check if still on login page
            if "login" in driver.current_url.lower() or "signup" in driver.current_url.lower():
                print("Still on login page. Login might not have been completed.")
                return False
            else:
                print("URL changed. Login appears successful!")
                time.sleep(5)  # Give some time for the page to load after login
                return True
                
        except Exception as e:
            print(f"Error during manual login wait: {str(e)}")
            return False
    else:
        print("No login page detected, continuing with scraping...")
    
    # If not on login page, try the automatic methods
    # Method 1: Try to close any login modals
    try:
        close_buttons = driver.find_elements(By.XPATH, 
            "//button[contains(@class, 'close') or contains(@aria-label, 'Close') or contains(@class, 'modal-close')]")
        for button in close_buttons:
            if button.is_displayed():
                print("  Clicking close button on modal...")
                driver.execute_script("arguments[0].click();", button)
                time.sleep(2)
                return True
    except:
        pass
    
    # Method 2: Try to click "X" buttons
    try:
        x_buttons = driver.find_elements(By.XPATH, 
            "//*[text()='×' or text()='X' or text()='x']")
        for button in x_buttons:
            if button.is_displayed():
                print("  Clicking X button...")
                driver.execute_script("arguments[0].click();", button)
                time.sleep(2)
                return True
    except:
        pass
    
    # Method 3: Try pressing ESC key
    try:
        print("  Pressing ESC key...")
        webdriver.ActionChains(driver).send_keys(Keys.ESCAPE).perform()
        time.sleep(2)
        return True
    except:
        pass
    
    # Method 4: Try to add "?share=1" to the URL which sometimes bypasses login walls
    try:
        current_url = driver.current_url
        if "?share=1" not in current_url:
            new_url = current_url + ("&" if "?" in current_url else "?") + "share=1"
            print(f"  Trying URL with share parameter: {new_url}")
            driver.get(new_url)
            time.sleep(5)
            return True
    except:
        pass
    
    print("  Could not bypass login wall")
    return False

def scrape_quora_topic(topic_url, max_questions=30, driver=None, disable_screenshots=False):
    """
    Scrape questions and answers from a Quora topic page using Selenium
    
    Args:
        topic_url: URL of the Quora topic to scrape
        max_questions: Maximum number of questions to scrape
        driver: Existing WebDriver instance (optional)
        disable_screenshots: Whether to disable saving screenshots
    
    Returns:
        List of dictionaries containing questions and answers
    """
    qa_data = []
    
    # Set up the Chrome WebDriver if not provided
    should_close_driver = False
    if not driver:
        driver = setup_driver()
        should_close_driver = True
        
    if not driver:
        return qa_data
    
    try:
        if topic_url != driver.current_url:
            print(f"Accessing topic page: {topic_url}")
            driver.get(topic_url)
            
            # Wait for the page to load
            print("Waiting for page to load...")
            time.sleep(10)  # Increased wait time
            
            # Check if we're on an English page or redirected
            if "ar.quora.com" in driver.current_url:
                print("Redirected to Arabic version. Trying to switch to English...")
                # Try to find language selector and switch to English
                try:
                    # Click on language selector
                    lang_buttons = driver.find_elements(By.XPATH, 
                        "//*[contains(text(), 'Languages') or contains(text(), 'اللغات')]")
                    for button in lang_buttons:
                        if button.is_displayed():
                            button.click()
                            time.sleep(2)
                            
                            # Try to find English option
                            eng_options = driver.find_elements(By.XPATH, 
                                "//*[contains(text(), 'English') or contains(text(), 'الإنجليزية')]")
                            for option in eng_options:
                                if option.is_displayed():
                                    option.click()
                                    time.sleep(5)
                                    break
                except Exception as e:
                    print(f"Error switching language: {str(e)}")
            
            # Try to handle login wall
            handle_login_wall(driver, disable_screenshots)
            
            # Accept cookies if the dialog appears
            try:
                cookie_buttons = driver.find_elements(By.XPATH, 
                    "//button[contains(text(), 'Accept') or contains(text(), 'accept') or contains(text(), 'Agree') or contains(text(), 'agree')]")
                for button in cookie_buttons:
                    if button.is_displayed():
                        print("Accepting cookies...")
                        button.click()
                        time.sleep(2)
                        break
            except Exception as e:
                print(f"No cookie dialog found or error: {str(e)}")
        
        # Scroll down several times to load more content
        print("Scrolling to load content...")
        for i in range(10):  # Increased from 5 to 10
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            print(f"  Scroll {i+1}/10")
            time.sleep(3)  # Increased wait time
        
        # Save the page source for debugging
        if not disable_screenshots:
            with open("quora_page_source.html", "w", encoding="utf-8") as f:
                f.write(driver.page_source)
            print("Saved page source to quora_page_source.html for debugging")
            
            # Take a screenshot for debugging
            driver.save_screenshot("quora_screenshot.png")
            print("Saved screenshot to quora_screenshot.png for debugging")
        
        # Get the page source after JavaScript has rendered the content
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, 'html.parser')
        
        # Debug: Print all links on the page
        print("\nDEBUG: All links found on page:")
        all_links = driver.find_elements(By.TAG_NAME, "a")
        for i, link in enumerate(all_links[:20]):  # Print first 20 links
            try:
                href = link.get_attribute('href')
                text = link.text[:30] + "..." if len(link.text) > 30 else link.text
                print(f"  Link {i+1}: {text} -> {href}")
            except:
                pass
        
        # Find question links
        question_links = []
        
        # Look for links containing question indicators
        for link in soup.find_all('a', href=True):
            href = link['href']
            # Common patterns for Quora question URLs
            if ('/q/' in href or '/question/' in href or '/answer/' in href) and href not in question_links:
                # Make sure it's an absolute URL
                if href.startswith('/'):
                    href = 'https://www.quora.com' + href
                question_links.append(href)
        
        print(f"Found {len(question_links)} question links")
        
        # If we still don't have links, try to find them directly in the driver
        if not question_links:
            print("Trying alternate method to find question links...")
            try:
                # Wait for some links to appear
                WebDriverWait(driver, 10).until(
                    EC.presence_of_all_elements_located((By.TAG_NAME, "a"))
                )
                
                # Find all links
                links = driver.find_elements(By.TAG_NAME, "a")
                for link in links:
                    try:
                        href = link.get_attribute('href')
                        if href and ('/q/' in href or '/question/' in href or '/answer/' in href) and href not in question_links:
                            question_links.append(href)
                    except:
                        continue
                
                print(f"Found {len(question_links)} question links using alternate method")
            except Exception as e:
                print(f"Error finding links: {str(e)}")
        
        # If still no links, try another approach for Quora search results
        if not question_links:
            print("Trying search results specific approach...")
            try:
                # Look for main content area
                main_content = driver.find_elements(By.CSS_SELECTOR, "div.q-box")
                print(f"Found {len(main_content)} potential content boxes")
                
                # Look for question titles in search results
                question_elements = driver.find_elements(By.CSS_SELECTOR, 
                    "div.q-box span.qu-dynamicFontSize--large, div.q-text")
                
                print(f"Found {len(question_elements)} potential question elements")
                
                for element in question_elements[:10]:  # Print first 10 for debugging
                    print(f"  Question element text: {element.text[:50]}")
                    
                    # Try to find the parent link
                    try:
                        parent = element
                        for _ in range(5):  # Look up to 5 levels up
                            parent = driver.execute_script("return arguments[0].parentNode;", parent)
                            parent_links = parent.find_elements(By.TAG_NAME, "a")
                            if parent_links:
                                href = parent_links[0].get_attribute('href')
                                if href and ('/q/' in href or '/question/' in href or '/answer/' in href) and href not in question_links:
                                    question_links.append(href)
                                    print(f"    Found link: {href}")
                                break
                    except:
                        pass
            except Exception as e:
                print(f"Error with search approach: {str(e)}")
        
        # If still no links, try a direct search for UAE schools questions
        if not question_links:
            print("Trying direct search for questions...")
            # List of search queries related to UAE universities
            search_queries = [
                "best universities in UAE",
                "UAE university admission",
                "Dubai universities",
                "Abu Dhabi universities",
                "UAE higher education"
            ]
            
            for query in search_queries:
                encoded_query = query.replace(" ", "%20")
                search_url = f"https://www.quora.com/search?q={encoded_query}"
                print(f"Searching for: {query}")
                
                driver.get(search_url)
                time.sleep(5)
                
                # Handle login wall again if needed
                handle_login_wall(driver, disable_screenshots)
                
                # Scroll to load content
                for i in range(3):
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    time.sleep(2)
                
                # Look for question links
                links = driver.find_elements(By.TAG_NAME, "a")
                for link in links:
                    try:
                        href = link.get_attribute('href')
                        if href and ('/q/' in href or '/question/' in href or '/answer/' in href) and href not in question_links:
                            question_links.append(href)
                    except:
                        continue
                
                print(f"  Found {len(question_links)} question links so far")
                
                # If we found some links, we can stop searching
                if len(question_links) >= 5:
                    break
        
        # Limit the number of questions to scrape
        question_links = question_links[:min(max_questions, len(question_links))]
        
        # Now scrape each question page
        for i, question_url in enumerate(question_links):
            try:
                print(f"Scraping question {i+1}/{len(question_links)}: {question_url}")
                
                # Navigate to the question page
                driver.get(question_url)
                
                # Wait for the page to load
                time.sleep(3)
                
                # Handle login wall if needed
                handle_login_wall(driver, disable_screenshots)
                
                # Get the question title and details separately
                question_title, question_details = get_question_content(driver)
                
                # If that fails, try to extract from URL
                if not question_title:
                    question_title = extract_clean_question(question_url)
                    question_details = ""
                
                if not question_title:
                    # Last resort
                    question_title = f"Question from {question_url}"
                    question_details = ""
                
                # Scroll down multiple times to load more answers
                for _ in range(8):  # Increased from 5 to 8 for more thorough scrolling
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    time.sleep(2)
                
                # Try to click buttons that might load more answers - do this multiple times
                for _ in range(3):  # Try clicking "more" buttons several times
                    click_more_answers_button(driver)
                    # Scroll again after clicking buttons
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    time.sleep(2)
                
                # Extract all answers using our comprehensive method
                answers = extract_all_answers(driver)
                
                # Add to our data
                qa_data.append({
                    'title': question_title,
                    'details': question_details,
                    'answers': answers,
                    'url': question_url
                })
                
                print(f"  - Found question: '{question_title[:50]}...' with {len(answers)} answers")
                
                # Add delay to avoid rate limiting
                time.sleep(random.uniform(2, 4))
                    
            except Exception as e:
                print(f"Error scraping question: {str(e)}")
    
    except Exception as e:
        print(f"Error during scraping: {str(e)}")
    
    finally:
        # Close the driver only if we created it
        if should_close_driver:
            driver.quit()
    
    return qa_data

def save_to_csv(data, filename="uae_school_qa.csv"):
    """
    Save scraped questions and answers to CSV file
    
    Args:
        data: List of dictionaries containing questions and answers
        filename: Output CSV filename
    """
    with open(filename, 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['Title', 'Question Details', 'Answer', 'URL'])
        
        for item in data:
            title = item['title']
            details = item['details']
            answers = item['answers']
            url = item['url']
            
            if answers:
                # Write each answer as a separate row with the same question
                for answer in answers:
                    writer.writerow([title, details, answer, url])
            else:
                # Write question with empty answer if no answers found
                writer.writerow([title, details, '', url])
    
    print(f"Data saved to {filename}")

def save_to_json(data, filename="uae_education_qa.json"):
    """
    Save scraped questions and answers to JSON file
    
    Args:
        data: List of dictionaries containing questions and answers
        filename: Output JSON filename
    """
    with open(filename, 'w', encoding='utf-8') as file:
        json.dump(data, file, indent=2, ensure_ascii=False)
    
    print(f"Data saved to {filename}")

def main():
    # Start directly from Arabic Quora main page
    start_url = "https://ar.quora.com/"
    
    print("Starting scraper for Quora UAE universities...")
    
    # Set up the Chrome WebDriver
    driver = setup_driver()
    if not driver:
        print("Failed to set up driver")
        return
    
    try:
        # First, go to the main page
        print(f"Accessing Quora main page: {start_url}")
        driver.get(start_url)
        
        # Wait for the page to load
        print("Waiting for page to load...")
        time.sleep(10)
        
        print("\n" + "="*50)
        print("PLEASE LOG IN MANUALLY NOW")
        print("The script will wait for 3 minutes (180 seconds) for you to complete the login.")
        print("Press Ctrl+C in the terminal to stop the script if needed.")
        print("="*50 + "\n")
        
        # Wait for manual login (180 seconds)
        for i in range(18):  # 18 x 10 seconds = 180 seconds
            print(f"Waiting for login... {(i+1)*10} seconds passed (press Ctrl+C to stop)")
            time.sleep(10)
        
        # Now search for UAE universities
        print("Searching for 'UAE universities' after login...")
        
        # Try to find the search box
        try:
            # Try different selector patterns for the search box
            search_selectors = [
                "input[placeholder*='Search'], input[type='search']",
                "input[placeholder*='بحث'], input[type='text']",
                "//input[contains(@placeholder, 'Search') or contains(@placeholder, 'بحث')]"
            ]
            
            search_box = None
            for selector in search_selectors:
                try:
                    if selector.startswith("//"):
                        # XPath selector
                        search_box = WebDriverWait(driver, 5).until(
                            EC.presence_of_element_located((By.XPATH, selector))
                        )
                    else:
                        # CSS selector
                        search_box = WebDriverWait(driver, 5).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                        )
                    if search_box:
                        print(f"Found search box using selector: {selector}")
                        break
                except:
                    continue
            
            if not search_box:
                print("Could not find search box. Trying to click on search icon first...")
                # Try to find and click on search icon/button
                search_icons = driver.find_elements(By.XPATH, 
                    "//button[contains(@aria-label, 'Search') or contains(@aria-label, 'بحث')]")
                
                for icon in search_icons:
                    if icon.is_displayed():
                        print("Found search icon, clicking it...")
                        icon.click()
                        time.sleep(3)
                        
                        # Try again to find search box
                        for selector in search_selectors:
                            try:
                                if selector.startswith("//"):
                                    # XPath selector
                                    search_box = WebDriverWait(driver, 5).until(
                                        EC.presence_of_element_located((By.XPATH, selector))
                                    )
                                else:
                                    # CSS selector
                                    search_box = WebDriverWait(driver, 5).until(
                                        EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                                    )
                                if search_box:
                                    print(f"Found search box after clicking icon using selector: {selector}")
                                    break
                            except:
                                continue
                        
                        break
            
            if search_box:
                # Try both "UAE universities" and Arabic equivalent
                search_terms = ["UAE universities", "جامعات الإمارات"]
                
                for term in search_terms:
                    search_box.clear()
                    search_box.send_keys(term)
                    search_box.send_keys(Keys.RETURN)
                    
                    print(f"Search submitted for '{term}', waiting for results...")
                    time.sleep(10)
                    
                    # Try to scrape results
                    qa_data = scrape_quora_topic(driver.current_url, max_questions=100, driver=driver, disable_screenshots=True)
                    
                    if qa_data and len(qa_data) > 0:
                        print(f"Found {len(qa_data)} questions with term '{term}'")
                        save_to_csv(qa_data, filename=f"uae_universities_{term.replace(' ', '_')}.csv")
                        save_to_json(qa_data, filename=f"uae_universities_{term.replace(' ', '_')}.json")
                        break
                    else:
                        print(f"No results found for '{term}', trying next term...")
                
            else:
                print("Could not find search box after multiple attempts")
                print("Falling back to direct search URL...")
                
                # Try direct search URL
                search_url = "https://www.quora.com/search?q=UAE%20universities"
                driver.get(search_url)
                time.sleep(10)
                
                # Now scrape the search results
                qa_data = scrape_quora_topic(driver.current_url, max_questions=100, driver=driver, disable_screenshots=True)
                
                print(f"Scraped {len(qa_data)} questions with their answers")
                if qa_data:
                    save_to_csv(qa_data, filename="uae_universities.csv")
                    save_to_json(qa_data, filename="uae_universities.json")
        
        except Exception as e:
            print(f"Error performing search: {str(e)}")
            print("Falling back to direct search URL...")
            
            # Try direct search URL
            search_url = "https://www.quora.com/search?q=UAE%20universities"
            driver.get(search_url)
            time.sleep(10)
            
            # Now scrape the search results
            qa_data = scrape_quora_topic(driver.current_url, max_questions=100, driver=driver, disable_screenshots=True)
            
            print(f"Scraped {len(qa_data)} questions with their answers")
            if qa_data:
                save_to_csv(qa_data, filename="uae_universities.csv")
                save_to_json(qa_data, filename="uae_universities.json")
        
        if not qa_data or len(qa_data) == 0:
            print("No data was scraped. Check if the website structure has changed or if Selenium is properly installed.")
            
    except Exception as e:
        print(f"Error during main execution: {str(e)}")
    
    finally:
        # Close the driver
        driver.quit()

if __name__ == "__main__":
    main()
