import sqlite3
import pandas as pd
from tqdm import tqdm
from multiprocessing import Process, Queue, cpu_count
import os

def worker_fn(worker_id, sites, db_raw, db_proc, n_pts, result_q, progress_q):
    pid = os.getpid()
    for s_ref in sites:
        results = []
        try:
            with sqlite3.connect(db_raw) as c_r, sqlite3.connect(db_proc) as c_p:
                s_ref = str(s_ref)

                df_raw = pd.read_sql("""
                                     SELECT DATETIME, SUM(FLOW) as FLOW FROM flow
                                     WHERE SITEREF = ? AND DATETIME >= '2013-01-02'
                                     GROUP BY strftime('%Y-%m-%d %H', DATETIME)
                                     """, c_r, params=[s_ref])
                df_raw['DATETIME'] = pd.to_datetime(df_raw['DATETIME']).dt.floor('h').dt.strftime('%Y-%m-%d %H:%M:%S')
                df_raw = df_raw.groupby('DATETIME', as_index=False).agg(FLOW=('FLOW', 'sum'))
                df_raw = df_raw[df_raw['DATETIME'] >= '2013-01-02']

                if df_raw.empty:
                    progress_q.put((worker_id, s_ref, pid, []))
                    continue

                sum_raw = df_raw['FLOW'].sum()
                df_pts  = df_raw.sample(min(n_pts, len(df_raw)))
                dts     = df_pts['DATETIME'].tolist()

                placeholders = ','.join(['?'] * len(dts))
                df_proc_pts = pd.read_sql(f"""
                    SELECT DATETIME, SUM(FLOW) as FLOW 
                    FROM data WHERE SITEREF = ? AND DATETIME IN ({placeholders})
                    GROUP BY DATETIME
                """, c_p, params=[s_ref] + dts)

                sum_proc = pd.read_sql("""
                                       SELECT SUM(FLOW) as total FROM data
                                       WHERE SITEREF = ? AND DATETIME >= '2013-01-02'
                                       """, c_p, params=[s_ref])['total'].iloc[0] or 0

                if abs(sum_raw - sum_proc) > 0.01:
                    results.append({'siteref': s_ref, 'type': 'SUM_TOTAL',
                                    'datetime': f"{df_raw['DATETIME'].min()} → {df_raw['DATETIME'].max()}",
                                    'raw': round(sum_raw, 2), 'proc': round(sum_proc, 2),
                                    'diff': round(sum_raw - sum_proc, 2)})

                missing = df_pts[~df_pts['DATETIME'].isin(df_proc_pts['DATETIME'])]
                for _, row in missing.iterrows():
                    results.append({'siteref': s_ref, 'type': 'MISSING_TS',
                                    'datetime': row['DATETIME'], 'raw': row['FLOW'], 'proc': 0, 'diff': row['FLOW']})

                merged = df_pts.merge(df_proc_pts, on='DATETIME', suffixes=('_raw', '_proc'))
                bad = merged[abs(merged['FLOW_raw'] - merged['FLOW_proc']) > 0.01]
                for _, row in bad.iterrows():
                    results.append({'siteref': s_ref, 'type': 'FLOW_MISMATCH',
                                    'datetime': row['DATETIME'],
                                    'raw': round(row['FLOW_raw'], 2), 'proc': round(row['FLOW_proc'], 2),
                                    'diff': round(row['FLOW_raw'] - row['FLOW_proc'], 2)})

        except Exception as e:
            results.append({'siteref': s_ref, 'type': 'ERROR', 'datetime': '-',
                            'raw': 0, 'proc': 0, 'diff': 0, 'msg': str(e)})

        progress_q.put((worker_id, s_ref, pid, results))

    result_q.put(worker_id)  # signal fin


def run_test(db_raw, db_proc, n_sites=50, n_pts=30, k=None):
    k = k or max(1, cpu_count() - 1)
    print(f"Lancement sur {k} workers\n")

    with sqlite3.connect(db_raw) as c_r:
        sites = pd.read_sql(f"""
            SELECT DISTINCT SITEREF FROM flow 
            WHERE DATETIME >= '2013-01-02'
            ORDER BY RANDOM() LIMIT {n_sites}
        """, c_r)['SITEREF'].tolist()

    # Découpe les sites entre workers
    chunks = [sites[i::k] for i in range(k)]

    result_q   = Queue()
    progress_q = Queue()

    # Lance les workers
    procs = []
    for i, chunk in enumerate(chunks):
        p = Process(target=worker_fn, args=(i, chunk, db_raw, db_proc, n_pts, result_q, progress_q))
        p.start()
        procs.append(p)

    # Progress bars par worker
    pbars = [
        tqdm(total=len(chunks[i]), position=i, desc=f"W{i} | PID=?", leave=True)
        for i in range(k)
    ]
    # Barre globale
    pbar_total = tqdm(total=n_sites, position=k, desc="TOTAL", leave=True)

    all_errors = []
    done_workers = 0

    while done_workers < k:
        item = progress_q.get()
        w_id, s_ref, pid, results = item
        all_errors.extend(results)

        err_count = len(results)
        pbars[w_id].set_description(f"W{w_id} PID={pid} | Err: {err_count}")
        pbars[w_id].set_postfix(site=s_ref)
        pbars[w_id].update(1)
        pbar_total.update(1)

        # Vérifie si un worker a fini
        while not result_q.empty():
            result_q.get()
            done_workers += 1

    for p in procs:
        p.join()

    for pb in pbars:
        pb.close()
    pbar_total.close()

    # Rapport
    err = len(all_errors)
    print(f"\n{'='*60}")
    print(f"Sites: {len(sites)} | Workers: {k} | Erreurs: {err}")

    if all_errors:
        df_err = pd.DataFrame(all_errors)
        print("\nPar type:")
        print(df_err.groupby('type').size().to_string())
        print("\nDétail:")
        print(df_err[['type', 'siteref', 'datetime', 'raw', 'proc', 'diff']].to_string(index=False))
        print("\nDates problématiques:")
        df_dates = df_err[df_err['type'] != 'SUM_TOTAL'][['siteref', 'type', 'datetime', 'diff']]
        if not df_dates.empty:
            print(df_dates.sort_values('datetime').to_string(index=False))

    print(f"{'='*60}")
    return err == 0


if __name__ == "__main__":
    res = run_test(
        "./nz/data/raw/nz_downloads/NZDB.db",
        "./nz/data/processed/db.db",
        n_sites=1000, n_pts=1000, k=24
    )

    print(res)
