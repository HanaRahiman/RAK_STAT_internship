from transformers import AutoTokenizer, AutoModelForSequenceClassification, pipeline

# Define model name
model_name = "MoritzLaurer/mDeBERTa-v3-base-mnli-xnli"

# Download and save model + tokenizer
model = AutoModelForSequenceClassification.from_pretrained(model_name)
tokenizer = AutoTokenizer.from_pretrained(model_name)

model.save_pretrained("./local_models/mdeberta")  # Saves to a local folder
tokenizer.save_pretrained("./local_models/mdeberta")

print("Model downloaded and saved successfully!")