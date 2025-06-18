import pandas as pd
import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from torch.utils.data import Dataset, DataLoader
from tqdm import tqdm
import numpy as np

# === Load model and tokenizer ===
model_path = r"C:\Users\seifs\OneDrive\Desktop\BerTA\local_models\mdeberta"
tokenizer = AutoTokenizer.from_pretrained(model_path)
model = AutoModelForSequenceClassification.from_pretrained(model_path)

# Use GPU if available
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
model = model.to(device)
model.eval()  # Set to evaluation mode

# === Get correct index for 'entailment' ===
label_map = model.config.label2id
entailment_index = label_map.get("entailment", 2)  # Default to index 2 if not found

# === Load your CSV file ===
df = pd.read_csv("C:/Users/seifs/OneDrive/Desktop/BerTA/uae_education_qa.csv")

# === OPTIMIZATION 1: Balanced Hypotheses Set ===
# ~50 hypotheses instead of 144 - keeps coverage while improving speed
core_hypotheses = [
    # Core education concepts (broad coverage)
    "This text is about education in the UAE.",
    "This text is about schools or universities in the United Arab Emirates.",
    "This text is about studying, learning, or teaching in the UAE.",
    "This text is about the education system in the UAE.",
    "This text is about students or academic institutions in the UAE.",
    
    # Fees and admissions (high-frequency topics)
    "This text is about school fees or university fees in the UAE.",
    "This text is about enrolling or applying to schools or universities in the UAE.",
    "This text is about admission requirements in the UAE.",
    "This text is about tuition costs in the UAE.",
    
    # Expat/International (very common in UAE)
    "This text is about expatriate or international students in the UAE.",
    "This text is about foreign students studying in UAE schools.",
    "This text is about visa requirements for students in the UAE.",
    "This text is about international schools in the UAE.",
    "This text is about children of foreign workers in UAE schools.",
    "This text is about student visa procedures for the UAE.",
    
    # Higher education
    "This text is about universities or higher education in the UAE.",
    "This text is about scholarships or financial aid in the UAE.",
    "This text discusses university admissions in the UAE.",
    "This text is about postgraduate studies in the UAE.",
    
    # Career Development (Education-Related)
    "This text is about scholarships in the UAE.",
    "This text discusses student scholarships or work placements in the UAE.",
    "This text is about career opportunities for students in the UAE.",
    "This text mentions graduate employment or job opportunities in the UAE.",
    "This text is about professional training or work experience in the UAE.",
    
    # Student life and services
    "This text discusses student life or school activities in the UAE.",
    "This text is about educational services or support in the UAE.",
    "This text mentions extracurricular activities in UAE schools.",
    "This text is about school transportation in the UAE.",
    
    # Educational authorities and policies
    "This text mentions KHDA, ADEK, or UAE education authorities.",
    "This text refers to the UAE's Ministry of Education or educational policies.",
    "This text discusses school inspections or ratings in the UAE.",
    
    # CITY-SPECIFIC HYPOTHESES - All major UAE cities with comprehensive coverage
    # Dubai (most comprehensive - largest expat population)
    "This text is about schools or universities in Dubai.",
    "This text is about education in Dubai.",
    "This text mentions student life in Dubai.",
    "This text is about school fees in Dubai.",
    "This text is about university fees in Dubai.",
    "This text discusses tuition costs in Dubai.",
    "This text is about scholarships in Dubai.",
    
    # Abu Dhabi (capital - comprehensive coverage)
    "This text is about schools or universities in Abu Dhabi.",
    "This text is about education in Abu Dhabi.",
    "This text mentions student life in Abu Dhabi.",
    "This text is about school fees in Abu Dhabi.",
    "This text is about university fees in Abu Dhabi.",
    "This text discusses tuition costs in Abu Dhabi.",
    "This text is about scholarships in Abu Dhabi.",
    
    # Sharjah (education hub - comprehensive coverage)
    "This text is about schools or universities in Sharjah.",
    "This text is about education in Sharjah.",
    "This text mentions student life in Sharjah.",
    "This text is about school fees in Sharjah.",
    "This text is about university fees in Sharjah.",
    "This text discusses tuition costs in Sharjah.",
    "This text is about scholarships in Sharjah.",
    
    # Other Emirates - with fee coverage
    "This text is about schools or universities in Ajman.",
    "This text is about education in Ajman.",
    "This text is about school fees in Ajman.",
    "This text is about scholarships in Ajman.",
    
    "This text is about schools or universities in Ras Al Khaimah.",
    "This text is about education in Ras Al Khaimah.",
    "This text is about school fees in Ras Al Khaimah.",
    "This text is about scholarships in Ras Al Khaimah.",
    
    "This text is about schools or universities in Fujairah.",
    "This text is about education in Fujairah.",
    "This text is about school fees in Fujairah.",
    "This text is about scholarships in Fujairah.",
    
    "This text is about schools or universities in Umm Al Quwain.",
    "This text is about school fees in Umm Al Quwain.",
    "This text is about scholarships in Umm Al Quwain.",
    
    "This text is about schools or universities in Al Ain.",
    "This text is about school fees in Al Ain.",
    "This text is about scholarships in Al Ain.",
    
    # Specific education types
    "This text is about Arabic language education in UAE schools.",
    "This text is about Islamic studies in UAE schools.",
    "This text discusses vocational training institutes in the UAE.",
    "This text is about technical or skills-based education in the UAE.",
    
    # Broad catch-alls
    "This text contains information relevant to someone seeking education in the UAE.",
    "This text would be useful for parents or students considering UAE education.",
]

# === OPTIMIZATION 2: Batch Processing with Early Stopping ===
class OptimizedEntailmentDataset(Dataset):
    def __init__(self, texts, hypotheses, tokenizer, max_length=256):  # Shorter sequences
        self.texts = [str(text) for text in texts]
        self.hypotheses = hypotheses
        self.tokenizer = tokenizer
        self.max_length = max_length
        
    def __len__(self):
        return len(self.texts)
    
    def __getitem__(self, idx):
        return {
            'text': self.texts[idx],
            'idx': idx
        }

def optimized_batch_process(texts, hypotheses, model, tokenizer, batch_size=32, threshold=0.4, max_length=256):
    """
    Optimized processing with:
    1. Larger batch sizes
    2. Shorter sequence lengths
    3. Early stopping when threshold met
    4. Vectorized operations
    """
    # Convert all texts to strings and handle None values
    texts = [str(text) if text is not None else "" for text in texts]
    
    n_texts = len(texts)
    max_scores = np.zeros(n_texts, dtype=np.float32)
    relevant_indices = set()
    
    print(f"Processing {n_texts} texts with {len(hypotheses)} hypotheses...")
    
    # Process each hypothesis
    for hyp_idx, hypothesis in enumerate(tqdm(hypotheses, desc="Processing hypotheses")):
        # Skip if all texts already meet threshold
        if len(relevant_indices) == n_texts:
            print(f"All texts classified after {hyp_idx + 1} hypotheses!")
            break
            
        # Create batches of texts that haven't met threshold yet
        texts_to_process = []
        indices_to_process = []
        
        for i, text in enumerate(texts):
            if i not in relevant_indices:
                texts_to_process.append(text)
                indices_to_process.append(i)
        
        if not texts_to_process:
            continue
            
        # Process in batches
        for batch_start in range(0, len(texts_to_process), batch_size):
            batch_end = min(batch_start + batch_size, len(texts_to_process))
            batch_texts = texts_to_process[batch_start:batch_end]
            batch_indices = indices_to_process[batch_start:batch_end]
            
            # Ensure all batch texts are strings
            batch_texts = [str(text) if text is not None else "" for text in batch_texts]
            
            # Tokenize batch
            inputs = tokenizer(
                batch_texts,
                [hypothesis] * len(batch_texts),
                return_tensors="pt",
                truncation=True,
                padding=True,
                max_length=max_length  # Shorter sequences = faster processing
            ).to(device)
            
            # Get predictions
            with torch.no_grad():
                logits = model(**inputs).logits
                probs = torch.softmax(logits, dim=1)
                scores = probs[:, entailment_index].cpu().numpy()
            
            # Update scores
            for i, idx in enumerate(batch_indices):
                if scores[i] > max_scores[idx]:
                    max_scores[idx] = scores[i]
                    if max_scores[idx] > threshold:
                        relevant_indices.add(idx)
    
    print(f"Found {len(relevant_indices)} relevant texts out of {n_texts}")
    return max_scores.tolist()

# === OPTIMIZATION 3: Sample and Test First (Optional) ===
def quick_sample_test(df, sample_size=100):
    """Test on a small sample first to validate approach"""
    if len(df) > sample_size:
        print(f"Testing on {sample_size} samples first...")
        sample_df = df.sample(n=sample_size, random_state=42).copy()
        
        sample_df['Title_Entailment'] = optimized_batch_process(
            sample_df['Title'].tolist(), 
            core_hypotheses, 
            model, 
            tokenizer,
            batch_size=32,
            max_length=256
        )
        
        sample_df['Answer_Entailment'] = optimized_batch_process(
            sample_df['Answer'].tolist(), 
            core_hypotheses, 
            model, 
            tokenizer,
            batch_size=32,
            max_length=256
        )
        
        sample_df['Relevant_to_Education_in_UAE'] = (
            (sample_df['Title_Entailment'] > 0.4) | 
            (sample_df['Answer_Entailment'] > 0.4)
        )
        
        relevant_count = sample_df['Relevant_to_Education_in_UAE'].sum()
        print(f"Sample results: {relevant_count}/{sample_size} ({relevant_count/sample_size*100:.1f}%) relevant")
        
        # Save sample results
        sample_df.to_csv("C:/Users/seifs/OneDrive/Desktop/BerTA/sample_results.csv", index=False)
        
        return sample_df
    return None

# === OPTIMIZATION 4: Parallel Processing Alternative ===
def process_with_multiprocessing(texts, hypotheses, model, tokenizer, n_processes=2):
    """Alternative: Use multiple processes (if you have multiple GPUs or want CPU parallelism)"""
    # This is more complex but can be implemented if needed
    pass

# === Main Processing ===
print("=== UAE Education Classification - Optimized Version ===")
print(f"Dataset size: {len(df)} records")
print(f"Using {len(core_hypotheses)} core hypotheses (reduced from 144)")
print(f"Device: {device}")

# Option 1: Test on sample first
sample_results = quick_sample_test(df, sample_size=200)

# Option 2: Process full dataset
print("\nProcessing full dataset...")

print("\nProcessing titles...")
df['Title_Entailment'] = optimized_batch_process(
    df['Title'].tolist(), 
    core_hypotheses, 
    model, 
    tokenizer,
    batch_size=32,  # Larger batches
    threshold=0.4,
    max_length=256  # Shorter sequences
)

print("\nProcessing answers...")
df['Answer_Entailment'] = optimized_batch_process(
    df['Answer'].tolist(), 
    core_hypotheses, 
    model, 
    tokenizer,
    batch_size=32,
    threshold=0.4,
    max_length=256
)

# === Final Results ===
THRESHOLD = 0.4
df['Relevant_to_Education_in_UAE'] = (
    (df['Title_Entailment'] > THRESHOLD) | 
    (df['Answer_Entailment'] > THRESHOLD)
)

# Statistics
total_relevant = df['Relevant_to_Education_in_UAE'].sum()
print(f"\n=== FINAL RESULTS ===")
print(f"Total records: {len(df)}")
print(f"Relevant to UAE education: {total_relevant} ({total_relevant/len(df)*100:.1f}%)")
print(f"Title-based relevance: {(df['Title_Entailment'] > THRESHOLD).sum()}")
print(f"Answer-based relevance: {(df['Answer_Entailment'] > THRESHOLD).sum()}")

# Save results
output_path = "C:/Users/seifs/OneDrive/Desktop/BerTA/optimized_labeled_uae_education_qa.csv"
df.to_csv(output_path, index=False)
print(f"\nResults saved to: {output_path}")

# Save only relevant records
relevant_df = df[df['Relevant_to_Education_in_UAE']].copy()
relevant_output_path = "C:/Users/seifs/OneDrive/Desktop/BerTA/relevant_uae_education_qa.csv"
relevant_df.to_csv(relevant_output_path, index=False)
print(f"Relevant records only saved to: {relevant_output_path}")