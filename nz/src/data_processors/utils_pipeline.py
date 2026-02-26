import sqlite3
from logging.config import IDENTIFIER

import pandas as pd
import time
from queue import Empty
from tqdm import tqdm
import json
from multiprocessing import Process, Queue, Event, Manager
from scipy.spatial import cKDTree
import numpy as np
import pytz
from pathlib import Path
from nz.src.data_processors.region_map import REGION_MAP

def chk_con(c):
    try:
        return bool(c.cursor())
    except Exception:
        return False

def map_reg(df, col='REGION'):
    df[col] = df[col].astype(str).str.split(" - ").str[-1].str.strip().str.rstrip(';').str.replace(r"['\u2019\u2018]", "", regex=True)
    df['r_code'] = df[col].map(REGION_MAP)
    return df

#def get_hol(df, df_h, reg):
#    df_s = df_h[df_h['r_code'].isin([reg, 'all'])].drop_duplicates(subset=['START_DATE'])
#    df = df.assign(j_dt=df['DATETIME_NZ'].dt.date).merge(
#        df_s.assign(j_dt=df_s['START_DATE'].dt.date), on='j_dt', how='left'
#    )
#    df['is_holiday'] = df['START_DATE'].notna().astype(int)
#    return df.drop(columns=['j_dt'])

def get_hol(df, df_h, reg):
    df['IS_HOLIDAY'] = False
    holidays = df_h[df_h['r_code'].isin([reg, 'all'])]
    for _, row in holidays.iterrows():
        mask = df['DATETIME'].between(row['START_DATE'], row['STOP_DATE'])
        df.loc[mask, 'IS_HOLIDAY'] = True
    return df

def get_xtw(df, df_x, reg):
    max_hours = 336  #14 jours * 24 heures

    df_s = df_x[df_x['r_code'].isin([reg, 'all'])].copy()
    df['DATETIME'] = pd.to_datetime(df['DATETIME']).astype('datetime64[s]')
    df_s['START_DATE'] = pd.to_datetime(df_s['START_DATE']).astype('datetime64[s]')

    df = pd.merge_asof(
        df.sort_values('DATETIME'),
        df_s[['START_DATE','HAZARD','IDENTIFIER','ABSTRACT','IMPACT']].drop_duplicates().sort_values('START_DATE'),
        left_on='DATETIME',
        right_on='START_DATE',
        direction='backward'
    )

    df['H_SINCE_XTW'] = (
            (df['DATETIME'] - df['START_DATE'])
            .dt.total_seconds() / 3600
    ).clip(lower=0, upper=max_hours).fillna(max_hours).astype(int)


    return df

def init_db(c):
    c.execute("CREATE TABLE IF NOT EXISTS catalog (col TEXT NOT NULL, code INTEGER NOT NULL, label TEXT NOT NULL, PRIMARY KEY (col, label))")
    c.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_cat ON catalog(col, code)")
    c.execute("CREATE TABLE IF NOT EXISTS site_info (region TEXT, siteref TEXT, min_date TEXT, max_date TEXT, PRIMARY KEY (region, siteref))")
    c.commit()

def upd_site(c, df):

    for _, row in df.iterrows():
        c.execute("""
                  INSERT INTO site_info (region, siteref, min_date, max_date)
                  VALUES (?, ?, ?, ?)
                      ON CONFLICT(region, siteref) DO UPDATE SET
                      min_date = MIN(site_info.min_date, excluded.min_date),
                                                          max_date = MAX(site_info.max_date, excluded.max_date)
                  """, (str(row['REGION']), str(row['SITEREF']), str(row['DATETIME_MIN']), str(row['DATETIME_MAX'])))
    c.commit()

def enc_db(c, df, cols):
    for col in cols:

        u_val = df[col].astype(str).unique()
        rows = c.execute("SELECT label, code FROM catalog WHERE col = ?", (col,)).fetchall()
        d_map = {r[0]: r[1] for r in rows}

        n_ent = []
        c_max = max(d_map.values()) if d_map else -1

        for v in u_val:
            if v not in d_map:
                c_max += 1
                d_map[v] = c_max
                n_ent.append((col, c_max, v))

        if n_ent:
            c.executemany("INSERT OR IGNORE INTO catalog (col, code, label) VALUES (?, ?, ?)", n_ent)
            c.commit()

        df[col] = df[col].astype(str).map(d_map).astype('int32')

    return df

def mount_worker(w_id, tasks, db_in, db_era, b_size, q_out, stop, prog):
    b_size = int(str(b_size).replace("_", ""))

    with sqlite3.connect(db_in) as c_in, sqlite3.connect(db_era) as c_era:
        with open('./nz/data/raw/era5_downloads/weather_grid.json', "r", encoding="utf-8") as f:
            grid = pd.DataFrame(json.load(f))[['longitude', 'latitude']].values
            tree = cKDTree(grid)

        df_h = map_reg(pd.read_sql("SELECT * FROM holiday", c_in).assign(
            START_DATE=lambda x: pd.to_datetime(x['START_DATE']).dt.normalize(),
            STOP_DATE=lambda x: pd.to_datetime(x['STOP_DATE']).dt.normalize()
        ))

        df_x = map_reg(pd.read_sql("SELECT * FROM extreme_weather", c_in).assign(
            START_DATE=lambda x: pd.to_datetime(x['START_DATE']).dt.normalize()
        ))

        try:
            for reg, site, lon, lat in tasks:
                if stop.is_set():
                    break

                try:
                    reg = map_reg(pd.DataFrame({'REGION': [reg]}) )['REGION'].iloc[0]   # MAP REG DIRECT

                    bnd = pd.read_sql("SELECT MIN(DATETIME) as m_dt, MAX(DATETIME) as mx_dt FROM flow WHERE SITEREF = ? AND DATETIME >= '2013-01-02'", c_in, params=[site])
                    if bnd.empty or pd.isna(bnd['m_dt'].iloc[0]):
                        continue

                    df = pd.read_sql("SELECT * FROM flow WHERE SITEREF = ? AND DATETIME >= '2013-01-02' ORDER BY DATETIME", c_in, params=[site])
                    df_flowmeta = pd.read_sql("SELECT SH, RS, RP, LANE, TYPE, REGION, SITETYPE, LAT, LON FROM flow_meta WHERE SITEREF = ?", c_in, params=[site])
                    df_flowmeta[['DATETIME_MIN','DATETIME_MAX']] = pd.DataFrame({
                        'DATETIME_MIN': [df['DATETIME'].min()],
                        'DATETIME_MAX': [df['DATETIME'].max()]
                    })
                    df_flowmeta = df_flowmeta.assign(SITEREF=site)

                    if not df_flowmeta.shape[0] == 1:
                        print( "ERROR SHAPE" )
                        break
                    df = df.assign(SITEREF=site, REGION=reg, LON=lon, LAT=lat, DATETIME=pd.to_datetime(df['DATETIME']).dt.floor('h'))
                    df = df.groupby(['SITEREF', 'FLOW' , 'WEIGHT','DIRECTION', 'REGION', 'DATETIME', 'LON', 'LAT'], as_index=False).agg(FLOW=('FLOW', 'sum'))
                    try:
                        df['DATETIME_NZ'] = df['DATETIME'].dt.tz_localize('Pacific/Auckland', ambiguous=False, nonexistent='shift_forward')
                    except Exception as e:
                        print(e, "Erreur detection changement d'horaire")

                    df['t_utc'] = df['DATETIME_NZ'].dt.tz_convert('UTC').dt.tz_localize(None).astype('datetime64[s]')
                    _, idx = tree.query(df[['LON', 'LAT']].values)

                    df['ELON'] = grid[idx, 0]
                    df['ELAT'] = grid[idx, 1]

                    u_t = df['t_utc'].dt.strftime('%Y-%m-%d %H:%M:%S').unique().tolist()
                    u_lon = df['ELON'].unique().tolist()
                    u_lat = df['ELAT'].unique().tolist()
                    w_prm = u_t + u_lon + u_lat

                    q_w = f"SELECT * FROM weather_data WHERE time IN ({','.join(['?']*len(u_t))}) AND longitude IN ({','.join(['?']*len(u_lon))}) AND latitude IN ({','.join(['?']*len(u_lat))})"
                    df_w = pd.read_sql(q_w, c_era, params=w_prm)

                    attendu = len(u_t) * len(u_lon) * len(u_lat)
                    if len(df_w) != attendu:
                        print(f"Err df_w: {len(df_w)}/{attendu} (Reg: {reg})")

                    df_w['time'] = pd.to_datetime(df_w['time']).astype('datetime64[s]')
                    df_w[['longitude', 'latitude']] = df_w[['longitude', 'latitude']].astype(float)

                    df = df.merge(df_w, left_on=['ELON', 'ELAT', 't_utc'], right_on=['longitude', 'latitude', 'time'], how='left')
                    df = df.drop(columns=[c for c in ['longitude', 'latitude', 'time', 'time_nz_flow'] if c in df.columns])

                    df = get_hol(df, df_h, reg)
                    df = get_xtw(df, df_x, reg)

                    q_out.put({
                        'w_id': w_id,
                        'reg': reg,
                        'df': df,
                        'df_meta' : df_flowmeta,
                    })

                    prog['t_prod'] += 1
                    prog[f'p_{w_id}_c'] = prog.get(f'p_{w_id}_c', 0) + 1

                except Exception as e:
                    print(f"[{w_id}] Err {reg}: {e} ERR2")

                prog['t_done'] = prog.get('t_done', 0) + 1

        except Exception as e:
            print(f"[{w_id}] Crit: {e}")


def mount_consumer(c_id, q_in, stop, func, prog):
    e_cols = ['WEIGHT','HAZARD','IDENTIFIER','ABSTRACT','IMPACT']
        #['SITEREF', 'REGION', 'HAZARD', 'IMPACT', 'ABSTRACT', 'IDENTIFIER', 'HOLIDAY', 'TYPE']

    with sqlite3.connect("./nz/data/processed/db.db") as c:
        c.execute("PRAGMA journal_mode=WAL;")
        c.execute("PRAGMA synchronous=OFF;")
        init_db(c)

        while not stop.is_set():
            try:
                item = q_in.get(timeout=5)
                if item is None:
                    print("None item")
                    break

                df = item['df']

               # for col in ['DATETIME', 'START_DATE', 'STOP_DATE']:
               #     if col in df.columns:
               #         df[col] = pd.to_datetime(df[col]).dt.strftime('%Y-%m-%d %H:%M:%S')


                df_flowmeta = item['df_meta']
                upd_site(c, df_flowmeta)

                df['IS_HOLIDAY'] = df['IS_HOLIDAY'].astype('int8')
                df['H_SINCE_XTW'] = df['H_SINCE_XTW'].astype('int16')
                df['FLOW'] = pd.to_numeric(df['FLOW'], downcast='integer')

                #Map
                df = enc_db(c, df, e_cols)

                for col in e_cols:
                    df[col] = df[col].astype('int32')

                meteo_cols = ['msl', 'tcc', 'u10', 'v10', 't2m', 'd2m', 'tp', 'cp', 'ssrd']
                df[meteo_cols] = df[meteo_cols].astype('float32')

                df[['LON', 'LAT', 'ELON', 'ELAT']] = df[['LON', 'LAT', 'ELON', 'ELAT']].astype('float32')

                keep = [
                    'DATETIME', 'REGION', 'SITEREF', 'LON', 'LAT', 'ELON', 'ELAT',
                    'FLOW', 'WEIGHT', 'DIRECTION',
                    'msl', 'tcc', 'u10', 'v10', 't2m', 'd2m', 'tp', 'cp', 'ssrd',
                    'IS_HOLIDAY',
                    'HAZARD', 'IDENTIFIER', 'ABSTRACT', 'IMPACT', 'H_SINCE_XTW'
                ]

                df = df[keep]

                df[keep].to_sql('data', c, if_exists='append', index=False, method='multi', chunksize=5000)

                prog['t_proc'] += 1

            except Empty:
                print("None item*")
                continue

def prog_mon(prog, stop, n_p, n_c, tot):
    pb = tqdm(total=tot, desc="PROG", dynamic_ncols=True)

    while not stop.is_set():
        done = prog.get('t_done', 0)
        prod = prog.get('t_prod', 0)
        proc = prog.get('t_proc', 0)

        pb.n = done
        pb.set_description(f"T: {done}/{tot} | S: {prod-proc} | C: {proc}")
        pb.refresh()
        time.sleep(0.5)

    pb.close()