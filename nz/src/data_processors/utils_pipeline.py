import sqlite3
import pandas as pd
import time
from queue import Empty
from tqdm import tqdm
import json

from multiprocessing import Process, Queue, Event, Manager

from nz.src.data_processors.region_map import REGION_MAP


from scipy.spatial import cKDTree

import numpy as np


import pytz
from pathlib import Path


def chk_conn(conn):
    try:
        conn.cursor()
        return True
    except Exception as ex:
        return False

def clean_region_name(name):
    print("test")
    if pd.isna(name):
        return ''
    return (
        str(name)
        .strip()
        .rstrip(';')
        .strip()
        .replace('\u2019', '')
        .replace('\u2018', '')
        .replace("'", '')
    )

def apply_region_map(df, col='REGION'):
    df[col] = df[col].astype(str).str.strip().str.rstrip(';').str.replace(r"['\u2019\u2018]", "", regex=True)
    df['script_region'] = df[col].map(REGION_MAP)
    return df

def get_holiday_data(chunk, df_holiday, region):

    df_h = df_holiday[df_holiday['script_region'].isin([region, 'all'])].copy()

    chunk['join_date'] = chunk['DATETIME_NZ'].dt.date
    df_h['join_date'] = df_h['START_DATE'].dt.date

    df_h = df_h.drop_duplicates(subset=['join_date'])
    chunk = chunk.merge(df_h, on='join_date', how='left')

    chunk['is_holiday'] = chunk['START_DATE'].notna().astype(int)
    return chunk.drop(columns=['join_date'])

def get_xtw_data(chunk, df_xtw, region):
    df_x = df_xtw[df_xtw['script_region'].isin([region, 'all'])].copy()
    if df_x.empty:
        return chunk

    chunk['time_in_utc'] = pd.to_datetime(chunk['time_in_utc']).astype('datetime64[s]')
    df_x['START_DATE'] = pd.to_datetime(df_x['START_DATE']).astype('datetime64[s]')

    chunk = chunk.sort_values('time_in_utc')
    df_x = df_x.sort_values('START_DATE')

    chunk = pd.merge_asof(
        chunk, df_x,
        left_on='time_in_utc',
        right_on='START_DATE',
        direction='backward',
        suffixes=('', '_xtw')
    )

    delta = (chunk['time_in_utc'] - chunk['START_DATE_xtw']).dt.total_seconds() / 3600
    chunk['hours_since_xtw'] = delta.fillna(336).clip(upper=336)
    chunk['days_since_xtw'] = (chunk['hours_since_xtw'] / 24).round(2)

    return chunk


def mount_worker(worker_id, tasks, db_path, db_path_era5, chunksize, output_queue, stop_event, progress_dict):
    if isinstance(chunksize, str):
        chunksize = int(chunksize.replace("_", ""))

    with sqlite3.connect(db_path) as conn, sqlite3.connect(db_path_era5) as conn_era5:
        with open('./nz/data/raw/era5_downloads/weather_grid.json', "r", encoding="utf-8") as f:
            weather_grid = pd.DataFrame(json.load(f))
            grid_coords = weather_grid[['longitude', 'latitude']].values
            tree = cKDTree(grid_coords)

        df_holiday = pd.read_sql("SELECT * FROM holiday", conn)
        df_holiday['START_DATE'] = pd.to_datetime(df_holiday['START_DATE']).dt.normalize()
        df_holiday['STOP_DATE'] = pd.to_datetime(df_holiday['STOP_DATE']).dt.normalize()
        df_holiday = apply_region_map(df_holiday)

        df_xtw = pd.read_sql("SELECT * FROM extreme_weather", conn)
        df_xtw['START_DATE'] = pd.to_datetime(df_xtw['START_DATE']).dt.normalize()
        df_xtw = apply_region_map(df_xtw)

        try:
            for region, station, lon, lat in tasks:
                if stop_event.is_set():
                    break

                #print("aaa", region, station, lon, lat)
                try:
                    region = region.split(" - ")[-1]

                    #for siteref in u_siteref_list:

                   # print("station['SITEREF'].values" , station['SITEREF'].values)

                    if True:

                        bounds_query = "SELECT MIN(DATETIME) as min_dt, MAX(DATETIME) as max_dt FROM flow WHERE SITEREF = ? AND DATETIME >= '2013-01-02'"
                        bounds = pd.read_sql(bounds_query, conn, params=[station])

                        if bounds.empty or pd.isna(bounds['min_dt'].iloc[0]):
                            continue

                        current_start = pd.to_datetime(bounds['min_dt'].iloc[0])
                        final_end = pd.to_datetime(bounds['max_dt'].iloc[0])
                        chunk_idx = 0

                        if True:
                       # while True: #current_start <= final_end:
                            if stop_event.is_set():
                                break

                           # current_end = current_start + pd.DateOffset(months=48)

                            query = """
                                    SELECT * FROM flow
                                    WHERE SITEREF = ?
                                    ORDER BY DATETIME 
                                    """
                #       AND DATETIME >= ?
                #        AND DATETIME < ?

                            chunk = pd.read_sql(
                                query,
                                conn,
                                params=[
                                    station,
                                   # current_start.strftime('%Y-%m-%d %H:%M:%S'),
                                   # current_end.strftime('%Y-%m-%d %H:%M:%S')
                                ]
                            )

                        #    if chunk.empty:
                        #        current_start = current_end
                        #        continue

                            chunk['SITEREF'] = station
                            chunk['LON'] = lon
                            chunk['LAT'] = lat
                           # chunk['DATETIME'] = pd.to_datetime(chunk['DATETIME'])
                            chunk['DATETIME'] = pd.to_datetime(chunk['DATETIME']).dt.floor('h')

                          #  print(chunk)

                            try:
                                chunk['DATETIME_NZ'] = (
                                    chunk['DATETIME']
                                    .dt.tz_localize('Pacific/Auckland', ambiguous=False, nonexistent='shift_forward')
                                )
                            except Exception as e:
                                print(e)
                                print(chunk['DATETIME'])

                            chunk['time_in_utc'] = chunk['DATETIME_NZ'].dt.tz_convert('UTC').dt.tz_localize(None).astype('datetime64[s]')

                            points_trafic = chunk[['LON', 'LAT']].values
                            _, indices = tree.query(points_trafic)

                            chunk['era5_lon'] = grid_coords[indices, 0].round(2)
                            chunk['era5_lat'] = grid_coords[indices, 1].round(2)

                            u_lons = chunk['era5_lon'].unique().tolist()
                            u_lats = chunk['era5_lat'].unique().tolist()
                            u_times = chunk['time_in_utc'].dt.strftime('%Y-%m-%d %H:%M:%S').unique().tolist()

                            query_weather = f"""
                                SELECT * FROM weather_data
                                WHERE time IN ({','.join(['?']*len(u_times))})
                                AND longitude IN ({','.join(['?']*len(u_lons))})
                                AND latitude IN ({','.join(['?']*len(u_lats))})
                            """

                            weather_params = u_times + u_lons + u_lats
                            weather_df = pd.read_sql(query_weather, conn_era5, params=weather_params)
                            print("res weather : " , len(weather_df) -  len( weather_params ), len(weather_df))
                            if not weather_df.empty:
                                weather_df['time'] = pd.to_datetime(weather_df['time']).astype('datetime64[s]')
                                weather_df['longitude'] = weather_df['longitude'].astype(float).round(2)
                                weather_df['latitude'] = weather_df['latitude'].astype(float).round(2)

                                chunk = chunk.merge(
                                    weather_df,
                                    left_on=['era5_lon', 'era5_lat', 'time_in_utc'],
                                    right_on=['longitude', 'latitude', 'time'],
                                    how='left'
                                )


                                drop_cols = [c for c in ['longitude', 'latitude', 'time', 'time_nz_flow'] if c in chunk.columns]
                                chunk = chunk.drop(columns=drop_cols)
                            else:
                                print(f"[Worker {worker_id}] No weather data for chunk {chunk_idx} in {region}")

                            chunk = get_holiday_data(chunk, df_holiday, region)
                            chunk = get_xtw_data(chunk, df_xtw, region)

                            output_queue.put({
                                'worker_id': worker_id,
                                'region': region,
                                'chunk_idx': chunk_idx,
                                'data': chunk,
                            })

                            progress_dict['total_chunks_produced'] += 1
                            progress_dict[f'producer_{worker_id}_chunks'] = progress_dict.get(f'producer_{worker_id}_chunks', 0) + 1

                            chunk_idx += 1
                          #  current_start = current_end

                except Exception as e:
                    print(f"[Producer {worker_id}] Error in region {region}: {e}")

                progress_dict['total_tasks_done'] = progress_dict.get('total_tasks_done', 0) + 1

        except Exception as e:
            print(f"[Producer {worker_id}] Critical Error: {e}")

CATALOG = {
    'SITEREF': {}, 'REGION': {}, 'DIRECTION': {},
    'TYPE': {}, 'HAZARD': {}, 'WEIGHT': {}
}

def ensure_catalog_table(conn):
    conn.execute("""
                 CREATE TABLE IF NOT EXISTS catalog (
                                                        col TEXT NOT NULL,
                                                        code INTEGER NOT NULL,
                                                        label TEXT NOT NULL,
                                                        PRIMARY KEY (col, code)
                     )
                 """)
    conn.commit()

def load_catalog_from_db(conn):
    """Charge le catalog existant depuis SQLite au démarrage."""
    try:
        rows = conn.execute("SELECT col, code, label FROM catalog").fetchall()
        for col, code, label in rows:
            if col in CATALOG:
                CATALOG[col][label] = code
        print(f"[Catalog] Chargé : { {k: len(v) for k, v in CATALOG.items()} }")
    except Exception as e:
        print(f"[Catalog] Rien à charger : {e}")

def encode_with_catalog(conn, chunk, col):
    existing = CATALOG[col]
    unique_vals = chunk[col].fillna('').astype(str).unique()

    new_entries = []
    for val in unique_vals:
        if val not in existing:
            new_code = len(existing)
            existing[val] = new_code
            new_entries.append((col, new_code, val))

    if new_entries:
        conn.executemany(
            "INSERT OR IGNORE INTO catalog (col, code, label) VALUES (?, ?, ?)",
            new_entries
        )
        conn.commit()  # persisté immédiatement

    chunk[col] = chunk[col].fillna('').astype(str).map(existing).astype('int32')
    return chunk

MAP_PATH = Path("./nz/data/processed/text_map.json")

# Structure : { "IMPACT": {"texte...": 0, ...}, "ABSTRACT": {...}, ... }
TEXT_COLS = ['IMPACT', 'ABSTRACT', 'IDENTIFIER', 'HOLIDAY']

def load_text_map():
    if MAP_PATH.exists():
        return json.loads(MAP_PATH.read_text())
    return {col: {} for col in TEXT_COLS}

def save_text_map(text_map):
    MAP_PATH.write_text(json.dumps(text_map, ensure_ascii=False, indent=2))

def encode_text_cols(chunk, text_map):
    changed = False
    for col in TEXT_COLS:
        mapping = text_map[col]
        for val in chunk[col].fillna('').astype(str).unique():
            if val not in mapping:
                mapping[val] = len(mapping)
                changed = True
        chunk[col] = chunk[col].fillna('').astype(str).map(mapping).astype('int32')
    return chunk, changed


def mount_consumer(consumer_id, input_queue, stop_event, process_func, progress_dict):
    text_map = load_text_map()

    with sqlite3.connect("./nz/data/processed/db.db") as conn:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=OFF;")

        while not stop_event.is_set():
            try:
                item = input_queue.get(timeout=5)
                if item is None:
                    break

                chunk = item['data']

                for col in ['SITEREF', 'REGION', 'DIRECTION', 'TYPE', 'HAZARD', 'WEIGHT']:
                    chunk[col] = chunk[col].astype('category')

                for col in ['DATETIME', 'START_DATE', 'STOP_DATE']:
                    chunk[col] = pd.to_datetime(chunk[col]).dt.strftime('%Y-%m-%d %H:%M:%S')

                chunk['is_holiday'] = chunk['is_holiday'].fillna(0).astype('int8')
                chunk['FLOW'] = pd.to_numeric(chunk['FLOW'], downcast='integer')

                for col in ['LON', 'LAT', 'u10', 'v10', 't2m', 'd2m', 'msl', 'ssrd', 'hours_since_xtw']:
                    chunk[col] = pd.to_numeric(chunk[col], downcast='float')

                chunk['tcc'] = (pd.to_numeric(chunk['tcc'], errors='coerce').fillna(0) * 100).round(0).astype('int8')
                for col in ['tp', 'cp']:
                    chunk[col] = (pd.to_numeric(chunk[col], errors='coerce').fillna(0) * 10_000).round(0).astype('int16')

                # Textes longs → int32 + map JSON
                chunk, changed = encode_text_cols(chunk, text_map)
                if changed:
                    save_text_map(text_map)

                chunk = chunk[[
                    'ID', 'SITEREF', 'DATETIME', 'FLOW', 'WEIGHT', 'DIRECTION', 'LON', 'LAT',
                    'msl', 'tcc', 'u10', 'v10', 't2m', 'd2m', 'tp', 'cp', 'ssrd',
                    'HOLIDAY', 'TYPE', 'START_DATE', 'STOP_DATE', 'REGION',
                    'is_holiday', 'HAZARD', 'IDENTIFIER', 'ABSTRACT', 'IMPACT', 'hours_since_xtw'
                ]]

                chunk.to_sql('data', conn, if_exists='append', index=False, method='multi', chunksize=500)

                progress_dict['total_chunks_processed'] += 1

            except Empty:
                continue



                #reverse = {v: k for k, v in text_map['IMPACT'].items()}


def progress_monitor(progress_dict, stop_event, n_producers, n_consumers, total_tasks):

    pbar = tqdm(total=total_tasks, desc="GLOBAL PROGRESS", dynamic_ncols=True)

    while not stop_event.is_set():
        tasks_done = progress_dict.get('total_tasks_done', 0)
        produced = progress_dict.get('total_chunks_produced', 0)
        processed = progress_dict.get('total_chunks_processed', 0)

        pbar.n = tasks_done

        pbar.set_description(f"Tasks: {tasks_done}/{total_tasks} | Stock: {produced-processed} | Total Chunks: {processed}")

        pbar.refresh()
        time.sleep(0.5)

    pbar.close()