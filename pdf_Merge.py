import os
from PyPDF2 import PdfMerger, PdfReader

# Folder containing the PDF files
folder_path = r"C:\Users\sarangs\Downloads\drive-download-20241227T160814Z-001"

# Output file path for the merged PDF
output_pdf = r"C:\Users\sarangs\Downloads\Merged_File.pdf"

# PDF password
pdf_password = "7411346811"

# Initialize the PDF Merger
merger = PdfMerger()

# Loop through all PDF files in the folder, sorted alphabetically
for file_name in sorted(os.listdir(folder_path)):
    if file_name.endswith(".pdf"):  # Check if the file is a PDF
        file_path = os.path.join(folder_path, file_name)
        
        # Open and unlock the PDF
        try:
            reader = PdfReader(file_path)
            if reader.is_encrypted:
                reader.decrypt(pdf_password)
            merger.append(reader)
            print(f"Added: {file_name}")
        except Exception as e:
            print(f"Failed to process {file_name}: {e}")

# Write the merged PDF
merger.write(output_pdf)
merger.close()

print(f"Merged PDF saved at: {output_pdf}")
