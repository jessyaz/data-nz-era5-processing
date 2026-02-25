import sqlite3
import pandas as pd
import random
import time

db_raw_path = "./nz/data/processed/db.db"

with sqlite3.connect(db_raw_path) as conn:
    print("Wait for SITEREF list with min/max dates")

    query = """
            SELECT SITEREF,
                   REGION,
                   MIN(DATETIME) AS MIN_DATE,
                   MAX(DATETIME) AS MAX_DATE
            FROM data
            GROUP BY SITEREF, REGION \
            """
    unique_sites = pd.read_sql(query, conn)

unique_sites.to_csv('unique_sites.csv', index=False)

print(f"{len(unique_sites)} SITEREF avec min/max date enregistrés dans 'unique_sites.csv'")
print(unique_sites.head())

print("Starting timer")
start_time = time.time()

# Tirer 5 SITEREF aléatoires
sample_sites = random.sample(unique_sites, 1)

with sqlite3.connect(db_raw_path) as conn:
    # Récupérer toutes les données associées aux SITEREF tirés
    placeholders = ",".join(["?"] * len(sample_sites))  # pour SQL paramétré
    query = f"SELECT * FROM data WHERE SITEREF IN ({placeholders})"
    df_sample = pd.read_sql(query, conn, params=sample_sites)

end_time = time.time()
print("len : ", len(df_sample))
print(f"Données associées aux 5 SITEREF récupérées en {end_time - start_time:.3f} s")
print(df_sample.head())