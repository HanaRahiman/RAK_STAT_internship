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

def setup_driver():
    """
    Set up and return a Chrome WebDriver instance
    """
    chrome_options = Options()
    chrome_options.add_argument("--headless")  # Run in headless mode
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

def scrape_quora_topic(topic_url, max_questions=30):
    """
    Scrape questions and answers from a Quora topic page using Selenium
    
    Args:
        topic_url: URL of the Quora topic to scrape
        max_questions: Maximum number of questions to scrape
    
    Returns:
        List of dictionaries containing questions and answers
    """
    qa_data = []
    
    # Set up the Chrome WebDriver
    driver = setup_driver()
    if not driver:
        return qa_data
    
    try:
        print(f"Accessing topic page: {topic_url}")
        driver.get(topic_url)
        
        # Wait for the page to load
        time.sleep(5)
        
        # Scroll down several times to load more content
        for _ in range(5):
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)
        
        # Get the page source after JavaScript has rendered the content
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, 'html.parser')
        
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
        # Close the driver
        driver.quit()
    
    return qa_data

def save_to_csv(data, filename="uae_education_qa.csv"):
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
    # UAE Education topic URL
    topic_url = "https://www.quora.com/topic/Education-in-the-United-Arab-Emirates"
    
    print("Starting scraper for UAE Education topic...")
    qa_data = scrape_quora_topic(topic_url, max_questions=30)
    
    print(f"Scraped {len(qa_data)} questions with their answers")
    if qa_data:
        save_to_csv(qa_data)
        save_to_json(qa_data)  # Also save as JSON for better structure preservation
    else:
        print("No data was scraped. Check if the website structure has changed or if Selenium is properly installed.")

if __name__ == "__main__":
    main()
