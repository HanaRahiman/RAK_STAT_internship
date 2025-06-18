# RAK_STAT_internship
# UAE Socio-Economic Sentiment Analysis on Education

This project focuses on collecting and analyzing public sentiment on education in the UAE. It gathers text data from multiple platforms—ranging from social media and professional networks to news outlets—to understand challenges, affordability, policy opinions, and student experiences.

The goal is to support data-driven education policy recommendations by capturing real, diverse opinions from different segments of society.

---

## 🔧 Project Components

| File Name                                   | Description                                                        | Audience / Source Type                                           |
|---------------------------------------------|--------------------------------------------------------------------|------------------------------------------------------------------|
| `linkedin_education_scraper.py`             | Scrapes posts and comments on LinkedIn using Selenium.            | Educators, policymakers, private-school operators, professionals |
| `reddit-education.py`                       | Uses Reddit API (via PRAW) to gather threads and comments.        | Students, expatriate parents, anonymous community feedback       |
| `khaleej_times_education_scraper.py`        | Scrapes UAE-focused education news articles from Khaleej Times.   | General public, journalists, parents, policy-aware readers       |
| **`reuters.py`**                            | Scrapes UAE-education news from Reuters for an international view. | International news consumers, global policy watchers             |
| `berta.py`                                   | Applies a classification model (Berta) to filter UAE-education content. | All platforms                                                   |
| `filtered.py`                                | Applies additional rules to refine and label the data.             | Processed data pipeline                                          |
| `clean_data.py`                              | Cleans raw scraped text by removing noise, special characters, etc. | Preprocessing utility                                            |

---

## 📥 Data Source Summary

| Platform         | Type of Data                                 | Represents                                                      |
|------------------|----------------------------------------------|-----------------------------------------------------------------|
| **LinkedIn**     | Professional posts and comments             | School staff, edu-tech firms, policymakers                      |
| **Reddit**       | Anonymous threads, complaints, opinions     | Students, parents, expats, whistleblowers                       |
| **Khaleej Times**| Official news stories on policy & reforms   | Government initiatives, tuition hikes, social reactions         |
| **Reuters**      | International news articles on UAE education| Global education commentary, international policy perspective   |



---


