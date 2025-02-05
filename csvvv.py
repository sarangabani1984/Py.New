import pdfplumber
import pandas as pd

def extract_table_from_pdf(pdf_path, output_csv_path):
    # List to store extracted data from all pages
    extracted_data = []

    # Open the PDF file
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            print(f"Processing Page {page.page_number}...")
            
            # Extract table data from the current page
            tables = page.extract_tables()
            for table in tables:
                # Add table rows to the extracted data
                extracted_data.extend(table)
    
    # Convert the extracted data to a DataFrame
    df = pd.DataFrame(extracted_data)

    # Assign column names if known (customize these to match your bank statement)
    df.columns = ["Date", "Transaction Details", "Type", "Amount"]

    # Save the DataFrame to a CSV file
    df.to_csv(output_csv_path, index=False)
    print(f"Data has been successfully saved to {output_csv_path}")

# File paths
pdf_path = r"D:\New folder\Decrypted_01Jan2019_TO_27Dec2024.pdf"
output_csv_path = r"D:\New folder\Bank_Statement.csv"

# Call the function
extract_table_from_pdf(pdf_path, output_csv_path)
