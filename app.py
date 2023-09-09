# import libraries
import os
import json
import csv
import sys
import re

# Define the directory path where JSONL files are located
jsonl_directory = '/content/drive/MyDrive/Colab Notebooks/DATA/Morocco Earthquake/Raw Tweets'

# Define the output CSV file path
output_csv_file = '/content/drive/MyDrive/Colab Notebooks/DATA/Morocco Earthquake/morroco_all_tweets.csv'

# Add the path to the directory containing morroco_keywords.py
keywords_path = '/content/drive/MyDrive/Colab Notebooks/DATA/Morocco Earthquake'
sys.path.append(keywords_path)

# Import the keyword lists from morroco_keywords.py
from morroco_keywords import emergency_services_requests, roads, critical_infrastructure, requests_for_help

# Initialize an empty list to store the extracted data from all JSONL files
all_tweets_data = []

# Function to extract data from a JSONL file and append it to the list
def process_jsonl_file(file_path):
    with open(file_path, 'r', encoding='utf-8') as jsonl_file:
        for line in jsonl_file:
            data = json.loads(line)
            all_tweets_data.append(data)

# Loop through all files in the specified directory
for filename in os.listdir(jsonl_directory):
    if filename.endswith('.jsonl'):
        file_path = os.path.join(jsonl_directory, filename)
        process_jsonl_file(file_path)

# Create the output CSV file and write the data
with open(output_csv_file, 'w', encoding='utf-8', newline='') as csv_file:
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow(["created_at", "id", "id_str", "text", "source", "user_id", "user_name", "screen_name", "location", "description"])

    for data in all_tweets_data:
        created_at = data.get("created_at", "")
        id = data.get("id", "")
        id_str = data.get("id_str", "")
        text = data.get("text", "")
        source = data.get("source", "")
        user = data.get("user", {})
        user_id = user.get("id", "")
        user_name = user.get("name", "")
        screen_name = user.get("screen_name", "")
        location = user.get("location", "")
        description = user.get("description", "")

        csv_writer.writerow([created_at, id, id_str, text, source, user_id, user_name, screen_name, location, description])

print(f"CSV file '{output_csv_file}' successfully created.")

# Combine all keywords into a single set for efficient lookup
all_keywords = set(keyword for keyword_list in [emergency_services_requests, roads, critical_infrastructure, requests_for_help] for keyword in keyword_list)

# Define the input and output file paths
input_csv_file = '/content/drive/MyDrive/Colab Notebooks/DATA/Morocco Earthquake/morroco_all_tweets.csv'
output_csv_file = '/content/drive/MyDrive/Colab Notebooks/DATA/Morocco Earthquake/morroco_filtered_tweets.csv'

# Initialize a list to store filtered tweets
filtered_tweets = []

# Function to check if a tweet contains any of the keywords
def contains_keywords(text):
    for keyword_tuple in all_keywords:
        for keyword in keyword_tuple:
            if re.search(rf'\b{re.escape(keyword)}\b', text, re.IGNORECASE):
                return True
    return False
# Read the input CSV file and filter tweets
with open(input_csv_file, 'r', encoding='utf-8') as csv_file:
    csv_reader = csv.DictReader(csv_file)
    for row in csv_reader:
        tweet_text = row.get('text', '')
        if contains_keywords(tweet_text):
            filtered_tweets.append(row)

# Write the filtered tweets to a new CSV file
with open(output_csv_file, 'w', encoding='utf-8', newline='') as output_csv:
    fieldnames = csv_reader.fieldnames
    csv_writer = csv.DictWriter(output_csv, fieldnames=fieldnames)
    csv_writer.writeheader()
    csv_writer.writerows(filtered_tweets)

print(f"Filtered tweets saved to {output_csv_file}.")
