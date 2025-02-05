import os
import pandas as pd
from sklearn.feature_extraction.text import CountVectorizer
import re

# File details
directory = r"D:\Desktop\myproject\HDFC"
file_names = [
    "Decrypted_01Jan2019_TO_27Dec2024_Copy.csv",
    "Decrypted_01Jan2019_TO_27Dec2024_Copy_2.csv",
]

# Helper function to categorize transactions
def categorize(description):
    description = description.lower()
    if "amazon" in description:
        return "Shopping"
    elif "uber" in description:
        return "Transport"
    elif "salary" in description:
        return "Income"
    elif "restaurant" in description or "food" in description:
        return "Dining"
    elif "electricity" in description or "bill" in description:
        return "Utilities"
    else:
        return "Others"

# Process each file
for file_name in file_names:
    file_path = os.path.join(directory, file_name)

    try:
        # Load the CSV file
        df = pd.read_csv(file_path)

        # Assuming column B is the second column
        if len(df.columns) < 2:
            print(f"File {file_name} doesn't have enough columns.")
            continue
        
        # Rename the column for easier access
        description_col = df.columns[1]  # Second column (column B)
        df.rename(columns={description_col: "Description"}, inplace=True)

        # Clean descriptions
        df["Description"] = df["Description"].astype(str).apply(lambda x: re.sub(r"[^a-zA-Z0-9\s]", "", x))

        # Categorize transactions
        df["Category"] = df["Description"].apply(categorize)

        # Extract top keywords for exploration
        vectorizer = CountVectorizer(stop_words="english", max_features=20)
        X = vectorizer.fit_transform(df["Description"])
        top_keywords = vectorizer.get_feature_names_out()

        # Save the categorized file
        output_file = os.path.join(directory, f"categorized_{file_name}")
        df.to_csv(output_file, index=False)
        
        print(f"Processed file: {file_name}")
        print(f"Top Keywords in {file_name}: {', '.join(top_keywords)}")
        print(f"Categorized file saved as: {output_file}\n")

    except Exception as e:
        print(f"Error processing file {file_name}: {e}")
