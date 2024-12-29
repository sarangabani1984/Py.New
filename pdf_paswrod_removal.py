from pypdf import PdfReader, PdfWriter

def remove_pdf_password(input_pdf, output_pdf, password):
    try:
        # Load the encrypted PDF
        reader = PdfReader(input_pdf)
        if reader.is_encrypted:
            # Decrypt the PDF
            reader.decrypt(password)

        # Create a PDF writer
        writer = PdfWriter()
        for page in reader.pages:
            writer.add_page(page)

        # Write the decrypted content to a new PDF
        with open(output_pdf, "wb") as output_file:
            writer.write(output_file)
        print(f"Password removed successfully. Decrypted PDF saved at: {output_pdf}")
    except Exception as e:
        print(f"An error occurred: {e}")

# File paths and password
input_pdf = r"C:\Users\sarangs\Downloads\01Jan2019_TO_27Dec2024.pdf"
output_pdf = r"C:\Users\sarangs\Downloads\Decrypted_01Jan2019_TO_27Dec2024.pdf"
password = "173407074"

# Remove password
remove_pdf_password(input_pdf, output_pdf, password)
