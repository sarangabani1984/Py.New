import fitz  # PyMuPDF for PDF parsing
import pandas as pd
import re

# File paths
pdf_file_path = r"D:\Desktop\myproject\Dataset\PhoePe\PhonePe_Statement_Sep2024_Jan2025_unlocked.pdf"
csv_file_path = r"D:\Desktop\myproject\Dataset\PhoePe\PhonePe_Statement.csv"

# Initialize an empty list to store rows
transactions = []

# Function to extract data from lines
def extract_transaction_data(lines):
    row = {}
    for i, line in enumerate(lines):
        if re.search(r"\d{1,2}-[A-Za-z]{3}-\d{2,4}", line):  # Match date
            row['Date'] = line.strip()
        elif "Transaction ID" in line:
            row['Transaction ID'] = line.strip()
        elif "UTR No." in line:
            row['UTR'] = line.strip()
        elif "Paid by" in line or "to" in line:
            row['Bank'] = line.strip()
        elif "DEBIT" in line or "CREDIT" in line:
            parts = line.split()
            row['Type'] = parts[0].strip()
            row['Amount'] = parts[-1].strip()
        elif "Transfer to" in line or "Received from" in line:
            row['Transaction Details'] = line.strip()
    
    if row:  # Add row only if it contains data
        transactions.append(row)

# Open the PDF file
with fitz.open(pdf_file_path) as pdf:
    for page in pdf:
        text = page.get_text()
        lines = text.split('\n')  # Split page content into lines
        extract_transaction_data(lines)

# Convert list of transactions to DataFrame
df = pd.DataFrame(transactions)

# Reformat date and time if available
def format_date_time(row):
    if 'Date' in row:
        date = row['Date']
        time = row.get('Time', '')
        return f"{date} {time}".strip()
    return None

df['Date'] = df.apply(format_date_time, axis=1)

# Save to CSV
df.to_csv(csv_file_path, index=False)

print(f"CSV file saved successfully at {csv_file_path}")
