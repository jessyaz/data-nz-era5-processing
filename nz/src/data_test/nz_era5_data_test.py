import sqlite3
import pandas as pd
import json
from scipy.spatial import cKDTree
from tqdm import tqdm

def validate_pipeline(db_raw_path, db_era5_path, db_processed_path, grid_json_path, k=5):
    with sqlite3.connect(db_raw_path) as conn_raw:
        stations_df = pd.read_sql("SELECT DISTINCT SITEREF FROM flow ORDER BY RANDOM() LIMIT ?", conn_raw, params=[k])
        test_stations = stations_df['SITEREF'].tolist()

    with open(grid_json_path, "r", encoding="utf-8") as f:
        grid_data = json.load(f)
        weather_grid = pd.DataFrame(grid_data)
        grid_coords = weather_grid[['longitude', 'latitude']].values
        tree = cKDTree(grid_coords)

    conn_raw = sqlite3.connect(db_raw_path)
    conn_era5 = sqlite3.connect(db_era5_path)
    conn_proc = sqlite3.connect(db_processed_path)

    for station in tqdm( test_stations ):
        raw_flow = pd.read_sql("SELECT * FROM flow WHERE SITEREF = ? LIMIT 100", conn_raw, params=[station])
        raw_flow_meta = pd.read_sql("SELECT * FROM flow_meta WHERE SITEREF = ? LIMIT 100", conn_raw, params=[station])

       # print(len(raw_flow))
        raw_flow = raw_flow.merge(raw_flow_meta, how='outer', on=['SITEREF'])
       # print(len(raw_flow))

       # print('[DEBUG]', raw_flow.columns)
       # print('[DEBUG]', raw_flow_meta.columns)
        if raw_flow.empty:
            continue

        raw_flow['DATETIME'] = pd.to_datetime(raw_flow['DATETIME']).dt.floor('h')
        raw_flow['DATETIME_NZ'] = raw_flow['DATETIME'].dt.tz_localize('Pacific/Auckland', ambiguous=False, nonexistent='shift_forward')
        raw_flow['time_in_utc'] = raw_flow['DATETIME_NZ'].dt.tz_convert('UTC').dt.tz_localize(None).astype('datetime64[s]')

        pts = raw_flow[['LON', 'LAT']].values
        _, idxs = tree.query(pts)

        raw_flow['era5_lon'] = grid_coords[idxs, 0].round(2)
        raw_flow['era5_lat'] = grid_coords[idxs, 1].round(2)

        proc_flow = pd.read_sql(f"SELECT * FROM data WHERE SITEREF = '{station}'", conn_proc)

        if proc_flow.empty:
            print(f"ECHEC: {station} introuvable dans la base traitee.")
            continue

        proc_flow['DATETIME'] = pd.to_datetime(proc_flow['DATETIME'])

        for _, row in raw_flow.iterrows():
            dt_nz = row['DATETIME']
            dt_utc = row['time_in_utc'].strftime('%Y-%m-%d %H:%M:%S')
            lon_e = row['era5_lon']
            lat_e = row['era5_lat']

            w_query = "SELECT t2m, u10 FROM weather_data WHERE time = ? AND longitude = ? AND latitude = ?"
            w_data = pd.read_sql(w_query, conn_era5, params=[dt_utc, lon_e, lat_e])

            p_data = proc_flow[proc_flow['DATETIME'] == dt_nz]

            if not w_data.empty and not p_data.empty:
                t2m_era5 = round(float(w_data.iloc[0]['t2m']), 4)
                t2m_proc = round(float(p_data.iloc[0]['t2m']), 4)

                if abs(t2m_era5 - t2m_proc) > 0.05:
                    print(f"ERREUR - Station: {station} | Date: {dt_nz} | ERA5: {t2m_era5} | Processed: {t2m_proc}")
            elif w_data.empty and not p_data.empty:
                if pd.notna(p_data.iloc[0].get('t2m')):
                    print(f"INCOHERENCE - Station: {station} | Date: {dt_nz} | ERA5 manquant mais Processed a une valeur.")

        print(f"data for {station} pass the test")

    conn_raw.close()
    conn_era5.close()
    conn_proc.close()
    print("Test termine.")

if __name__ == "__main__":
    db_raw = "./nz/data/raw/nz_downloads/NZDB.db"
    db_era5 = "./nz/data/raw/era5_downloads/era5_data.db"
    db_processed = "./nz/data/processed/db.db"
    grid_json = "./nz/data/raw/era5_downloads/weather_grid.json"

    validate_pipeline(db_raw, db_era5, db_processed, grid_json, k=30)