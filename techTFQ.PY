import pandas as pd
from sqlalchemy import create_engine

conn_string = (
    "mssql+pyodbc://@SARANGS-X1-G10\\SQLEXPRESS/tecTFQ"
    "?driver=ODBC+Driver+17+for+SQL+Server"
    "&trusted_connection=yes"
)

db = create_engine(conn_string)
conn = db.connect()

files = ['artist', 'canvas_size', 'image_link', 'museum_hours', 'museum', 'product_size', 'subject', 'work']

for file in files:
    # Use raw string to handle Windows path properly
    df = pd.read_csv(rf'D:\SQL\techTFQ\{file}.csv')  
    df.to_sql(file, con=conn, if_exists='replace', index=False)
