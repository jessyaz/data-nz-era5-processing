import sqlite3
import pandas as pd
from tqdm import tqdm

def load_cat(c):
    rows = c.execute("SELECT col, code, label FROM catalog").fetchall()
    cat = {}
    for col, code, label in rows:
        if col not in cat:
            cat[col] = {}
        cat[col][label] = code
    return cat

def run_test(db_raw, db_proc, n=1000):
    with sqlite3.connect(db_raw) as c_r, sqlite3.connect(db_proc) as c_p:
        cat = load_cat(c_p)

        print("Fetch db...")

        df_r = pd.read_sql(f"SELECT SITEREF, DATETIME, FLOW FROM flow ORDER BY RANDOM() LIMIT {n}", c_r)

        print("Fecthing ok.")

        if df_r.empty:
            return False

        df_r['DATETIME'] = pd.to_datetime(df_r['DATETIME']).dt.floor('h').dt.strftime('%Y-%m-%d %H:%M:%S')
        df_r = df_r.groupby(['SITEREF', 'DATETIME'], as_index=False).agg(FLOW=('FLOW', 'sum'))

        err = 0

        s_code_err_list = []
        res_err_list = []
        print("Try")

        #for _, r in tqdm( df_r.iterrows(),desc=f'{ok}', total=df_r.shape[0] ):
        pbar = tqdm(df_r.iterrows(), desc='Bar desc', total=df_r.shape[0])
        for idx, r in pbar:
            pbar.set_postfix(message="En cours", index_ligne=idx)

            s_ref = str(r['SITEREF'])
            dt = r['DATETIME']
            flw = r['FLOW']

            s_code = cat.get('SITEREF', {}).get(s_ref)

            if s_code is None:

                err += 1
                s_code_err_list.append(s_ref)

                continue

            q = "SELECT FLOW FROM data WHERE SITEREF = ? AND DATETIME = ?"
            res = pd.read_sql(q, c_p, params=(s_code, dt))

            if res.empty or res['FLOW'].iloc[0] != flw:
                err += 1
                res_err_list .append ( (s_code, dt ) )

            pbar.set_description(f"Erreur cumulée: {err}")

        print("res_err_list : ", res_err_list)
        print("s_code_err_list : ",s_code_err_list)

        print(f"T: {len(df_r)} | E: {err}")
        return err == 0

if __name__ == "__main__":
    run_test("./nz/data/raw/nz_downloads/NZDB.db", "./nz/data/processed/db.db", 50)