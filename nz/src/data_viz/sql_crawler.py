
import sqlite3
import pandas as pd

conn = sqlite3.connect("./nz/data/processed/db.db")

size = conn.execute("SELECT page_count * page_size FROM pragma_page_count(), pragma_page_size()").fetchone()[0]
print(f"Taille DB : {size / 1e9:.2f} Go")

print(conn.execute("SELECT COUNT(*) FROM data").fetchone())

df = pd.read_sql("SELECT * FROM data LIMIT 10000", conn)
print("\nTaille estimée par colonne (10k lignes) :")
for col in df.columns:
    mb = df[col].memory_usage(deep=True) / 1e6
    print(f"  {col:<25} {mb:.2f} MB")

conn.close()