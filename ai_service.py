from transformers import pipeline
# Using a popular NLP service pipeline for text generation
chatbot = pipeline("text-generation", model="gpt2")
response = chatbot("What is the core of CSE?", max_length=50)
print(response)
