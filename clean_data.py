import pandas as pd
import re
import unicodedata
import html
import os

def clean_text(text):
    if pd.isna(text):
        return ""
    
    # Convert to string if not already
    text = str(text)
    
    # Decode HTML entities
    text = html.unescape(text)
    
    # Normalize unicode characters
    text = unicodedata.normalize('NFKD', text)
    
    # Replace common encoding errors
    text = text.replace('\u2019', "'")  # Smart quotes
    text = text.replace('\u2018', "'")
    text = text.replace('\u201c', '"')  # Smart double quotes
    text = text.replace('\u201d', '"')
    text = text.replace('\u2013', '-')  # En dash
    text = text.replace('\u2014', '--')  # Em dash
    text = text.replace('\u2026', '...')  # Ellipsis
    
    # Remove non-breaking spaces and replace with regular spaces
    text = text.replace('\xa0', ' ')
    
    # Fix specific issues in this dataset
    text = re.sub(r'I\s+m\s+', "I'm ", text)  # Fix "I m" to "I'm"
    text = re.sub(r'don\s+t\s+', "don't ", text)  # Fix "don t" to "don't"
    text = re.sub(r'hasn\s+t\s+', "hasn't ", text)  # Fix "hasn t" to "hasn't"
    text = re.sub(r'isn\s+t\s+', "isn't ", text)  # Fix "isn t" to "isn't"
    text = re.sub(r'wasn\s+t\s+', "wasn't ", text)  # Fix "wasn t" to "wasn't"
    text = re.sub(r'aren\s+t\s+', "aren't ", text)  # Fix "aren t" to "aren't"
    text = re.sub(r'didn\s+t\s+', "didn't ", text)  # Fix "didn t" to "didn't"
    text = re.sub(r'won\s+t\s+', "won't ", text)  # Fix "won t" to "won't"
    text = re.sub(r'can\s+t\s+', "can't ", text)  # Fix "can t" to "can't"
    text = re.sub(r'it\s+s\s+', "it's ", text)  # Fix "it s" to "it's"
    text = re.sub(r'that\s+s\s+', "that's ", text)  # Fix "that s" to "that's"
    text = re.sub(r'there\s+s\s+', "there's ", text)  # Fix "there s" to "there's"
    
    # Fix other common contractions
    text = re.sub(r'(\w+)\s+s\s+', r"\1's ", text)  # Fix "word s" to "word's"
    text = re.sub(r'(\w+)\s+ve\s+', r"\1've ", text)  # Fix "word ve" to "word've"
    text = re.sub(r'(\w+)\s+ll\s+', r"\1'll ", text)  # Fix "word ll" to "word'll"
    text = re.sub(r'(\w+)\s+re\s+', r"\1're ", text)  # Fix "word re" to "word're"
    
    # Fix specific issues with dashes and hyphens
    text = re.sub(r'(\d+)-(\d+)', r'\1–\2', text)  # Fix number ranges
    
    # Fix specific issues with question/answer format
    text = re.sub(r'([?])\s*([A-Z][a-z])', r'\1\n\2', text)  # Add line break after question mark followed by capital letter
    
    # Fix specific issues with URLs
    text = re.sub(r'([a-z])([A-Z])', r'\1 \2', text)  # Add space between lowercase and uppercase letters
    
    # Remove control characters
    text = re.sub(r'[\x00-\x1F\x7F-\x9F]', '', text)
    
    # Replace multiple spaces with a single space
    text = re.sub(r'\s+', ' ', text)
    
    return text.strip()

def main():
    # Path to the input file
    input_file = 'Dataset/labeled_uae_education_qa.csv'
    
    # Path to the output file
    output_file = 'final_cleaned_uae_education_qa.csv'  # Changed filename to avoid permission issues
    
    try:
        # Try different encodings
        encodings = ['utf-8', 'latin1', 'cp1252', 'iso-8859-1']
        df = None
        
        for encoding in encodings:
            try:
                print(f"Trying to read with {encoding} encoding...")
                df = pd.read_csv(input_file, encoding=encoding)
                print(f"Successfully read with {encoding} encoding.")
                break
            except UnicodeDecodeError:
                print(f"Failed to read with {encoding} encoding.")
                continue
        
        if df is None:
            print("Failed to read the file with any of the encodings.")
            return
        
        # Print column names to verify
        print(f"Columns in the dataset: {df.columns.tolist()}")
        
        # Check for and handle duplicate rows
        duplicate_count = df.duplicated().sum()
        if duplicate_count > 0:
            print(f"Found {duplicate_count} duplicate rows. Removing duplicates...")
            df = df.drop_duplicates()
        
        # Clean each column
        for column in df.columns:
            df[column] = df[column].apply(clean_text)
        
        # Remove the "Question Details" column
        if 'Question Details' in df.columns:
            print("Removing 'Question Details' column...")
            df = df.drop(columns=['Question Details'])
        
        # Save the cleaned data
        df.to_csv(output_file, index=False, encoding='utf-8')
        print(f"Cleaned data saved to {output_file}")
        
        # Display some sample rows to verify
        print("\nSample of cleaned data:")
        print(df.head(3).to_string())
        
        # Count rows
        print(f"\nTotal rows in cleaned dataset: {len(df)}")
        
        # Check for missing values
        missing_values = df.isnull().sum()
        if missing_values.sum() > 0:
            print("\nMissing values in each column:")
            print(missing_values)
        
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main() 