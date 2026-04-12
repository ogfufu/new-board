import os
import io
import json
import sqlite3
import threading
from datetime import datetime, date, timedelta

import pandas as pd
import requests
from flask import Flask, jsonify, render_template, request

app = Flask(__name__)

HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/91.0.4472.124 Safari/537.36'
    )
}

DB_PATH      = os.path.join(os.path.dirname(__file__), 'history.db')
BACKUP_XLSX  = os.path.join(os.path.dirname(__file__), 'backup.xlsx')
KEEP_DAYS = 5

# ---------- SQLite history ----------

def init_db():
    con = sqlite3.connect(DB_PATH)
    con.execute('''
        CREATE TABLE IF NOT EXISTS snapshots (
            date       TEXT NOT NULL,
            rank       INTEGER,
            code       TEXT,
            name       TEXT,
            price      REAL,
            change_pct REAL,
            trust      INTEGER,
            yoy        REAL,
            yoy1       REAL,
            open       REAL,
            high       REAL,
            low        REAL,
            capital    REAL,
            industry   TEXT,
            PRIMARY KEY (date, code)
        )
    ''')
    # Crown reference: independent of date, stores the "previous day" baseline
    con.execute('''
        CREATE TABLE IF NOT EXISTS crown_ref (
            code  TEXT PRIMARY KEY,
            name  TEXT,
            rank  INTEGER
        )
    ''')
    # Backup metadata: records which date was last manually backed up
    con.execute('''
        CREATE TABLE IF NOT EXISTS backup_meta (
            id      INTEGER PRIMARY KEY CHECK (id = 1),
            date    TEXT,
            created TEXT
        )
    ''')
    # Sector card configurations (JSON blob, singleton row)
    con.execute('''
        CREATE TABLE IF NOT EXISTS sector_configs (
            id   INTEGER PRIMARY KEY CHECK (id = 1),
            data TEXT NOT NULL
        )
    ''')
    con.commit()
    con.close()


def save_crown_ref(df):
    """Replace the crown reference with current data."""
    con = sqlite3.connect(DB_PATH)
    con.execute('DELETE FROM crown_ref')
    rows = [(str(r['代號']), r['名稱'], int(r['排序'])) for _, r in df.iterrows()]
    con.executemany('INSERT INTO crown_ref VALUES (?,?,?)', rows)
    con.commit()
    con.close()


def get_crown_ref():
    """Return set of codes saved as crown reference."""
    con = sqlite3.connect(DB_PATH)
    rows = con.execute('SELECT code FROM crown_ref').fetchall()
    con.close()
    return [r[0] for r in rows]


def save_backup_meta(date_str):
    con = sqlite3.connect(DB_PATH)
    con.execute('INSERT OR REPLACE INTO backup_meta VALUES (1, ?, ?)',
                (date_str, datetime.now().isoformat()))
    con.commit()
    con.close()


def get_backup_meta():
    con = sqlite3.connect(DB_PATH)
    row = con.execute('SELECT date FROM backup_meta WHERE id=1').fetchone()
    con.close()
    return row[0] if row else None


def save_sector_configs(data_json_str):
    con = sqlite3.connect(DB_PATH)
    con.execute('INSERT OR REPLACE INTO sector_configs VALUES (1, ?)', (data_json_str,))
    con.commit()
    con.close()


def get_sector_configs():
    con = sqlite3.connect(DB_PATH)
    row = con.execute('SELECT data FROM sector_configs WHERE id=1').fetchone()
    con.close()
    return row[0] if row else None


def last_trading_day():
    """Most recent weekday on or before today (today itself if weekday)."""
    d = date.today()
    while d.weekday() >= 5:   # Sat=5, Sun=6
        d -= timedelta(days=1)
    return d.strftime('%Y-%m-%d')


def prev_trading_day():
    """The trading day immediately before last_trading_day()."""
    d = date.today()
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    d -= timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d.strftime('%Y-%m-%d')


def save_snapshot(df, date_str, overwrite=False):
    """Save snapshot for date_str.  If overwrite=False, skip when date exists."""
    con = sqlite3.connect(DB_PATH)
    exists = con.execute(
        'SELECT 1 FROM snapshots WHERE date=? LIMIT 1', (date_str,)
    ).fetchone()
    if exists and not overwrite:
        con.close()
        return
    if exists and overwrite:
        con.execute('DELETE FROM snapshots WHERE date=?', (date_str,))

    rows = [
        (
            date_str,
            int(r['排序']),
            str(r['代號']),
            r['名稱'],
            r['股價'],
            r['漲跌幅'],
            int(r['投信']) if r['投信'] is not None else None,
            r['月(YOY)'],
            r['月-1(YOY)'],
            r['開盤'],
            r['最高'],
            r['最低'],
            r['資金(億)'],
            r['產業類型'],
        )
        for _, r in df.iterrows()
    ]
    con.executemany(
        'INSERT OR REPLACE INTO snapshots VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
        rows
    )

    # Keep only last KEEP_DAYS dates
    dates = [
        d[0] for d in con.execute(
            'SELECT DISTINCT date FROM snapshots ORDER BY date DESC'
        ).fetchall()
    ]
    if len(dates) > KEEP_DAYS:
        for old in dates[KEEP_DAYS:]:
            con.execute('DELETE FROM snapshots WHERE date=?', (old,))

    con.commit()
    con.close()


def get_history_dates():
    con = sqlite3.connect(DB_PATH)
    dates = [
        d[0] for d in con.execute(
            'SELECT DISTINCT date FROM snapshots ORDER BY date DESC'
        ).fetchall()
    ]
    con.close()
    return dates


def get_snapshot(date_str):
    con = sqlite3.connect(DB_PATH)
    rows = con.execute(
        '''SELECT rank,code,name,price,change_pct,trust,yoy,yoy1,
                  open,high,low,capital,industry
           FROM snapshots WHERE date=? ORDER BY rank''',
        (date_str,)
    ).fetchall()
    con.close()
    cols = ['排序','代號','名稱','股價','漲跌幅','投信','月(YOY)','月-1(YOY)','開盤','最高','最低','資金(億)','產業類型']
    return [dict(zip(cols, r)) for r in rows]


# ---------- In-memory cache ----------
_last_df = None          # last successfully fetched DataFrame (for backup)
_wespai_cache = {'data': None, 'date': None}
_wespai_lock = threading.Lock()


def get_wespai_data():
    with _wespai_lock:
        today = datetime.now().strftime('%Y-%m-%d')
        if _wespai_cache['data'] is not None and _wespai_cache['date'] == today:
            return _wespai_cache['data']

        url = 'https://stock.wespai.com/p/71294'
        r = requests.get(url, headers=HEADERS, timeout=20)
        r.raise_for_status()
        df_all = pd.read_html(io.StringIO(r.text))[0]
        cols = ['代號', '公司', '投信買賣超', '(月)營收年增率(%)', '(月-1)營收年增率(%)', '產業類型']
        df = df_all[cols].copy()
        df['代號'] = df['代號'].astype(str)

        _wespai_cache['data'] = df
        _wespai_cache['date'] = today
        return df


def get_histock_data():
    url = 'https://histock.tw/stock/rank.aspx?m=13&p=all'
    r = requests.get(url, headers=HEADERS, timeout=20)
    r.raise_for_status()
    df_all = pd.read_html(io.StringIO(r.text))[0]
    df_all.columns = df_all.columns.str.replace('▼', '', regex=False)
    cols = ['代號', '名稱', '價格', '漲跌', '漲跌幅', '開盤', '最高', '最低', '昨收', '成交值(億)']
    df = df_all[cols].copy()
    df['代號'] = df['代號'].astype(str)
    return df


def run_stock_update():
    df_hi = get_histock_data()
    df_wes = get_wespai_data()

    merged = pd.merge(df_hi, df_wes, on='代號', how='inner')
    processed = merged.rename(columns={
        '價格': '股價',
        '投信買賣超': '投信',
        '(月)營收年增率(%)': '月(YOY)',
        '(月-1)營收年增率(%)': '月-1(YOY)',
        '成交值(億)': '資金(億)',
    })

    processed['漲跌幅'] = pd.to_numeric(
        processed['漲跌幅'].astype(str)
            .str.replace('+', '', regex=False)
            .str.replace('%', '', regex=False)
            .str.strip(),
        errors='coerce'
    ).fillna(0)

    processed['投信'] = pd.to_numeric(
        processed['投信'], errors='coerce'
    ).fillna(0).round().astype(int)

    numeric_cols = ['股價', '開盤', '最高', '最低', '月(YOY)', '月-1(YOY)', '資金(億)']
    for col in numeric_cols:
        processed[col] = pd.to_numeric(processed[col], errors='coerce')

    processed = (
        processed
        .sort_values('資金(億)', ascending=False)
        .head(100)
        .reset_index(drop=True)
    )
    processed.insert(0, '排序', range(1, len(processed) + 1))

    final_cols = ['排序', '代號', '名稱', '股價', '漲跌幅', '投信', '月(YOY)', '月-1(YOY)', '開盤', '最高', '最低', '資金(億)', '產業類型']
    return processed[final_cols]


# ---------- Routes ----------

@app.route('/')
def index():
    return render_template('index.html')


@app.route('/api/backup', methods=['POST'])
def api_backup():
    """Save current data to SQLite + Excel, update crown ref and backup metadata."""
    global _last_df
    if _last_df is None:
        try:
            _last_df = run_stock_update()
        except Exception as e:
            return jsonify({'success': False, 'error': f'資料抓取失敗：{str(e)}'}), 500
    try:
        target_date = last_trading_day()
        save_crown_ref(_last_df)
        save_snapshot(_last_df, target_date, overwrite=True)   # persist to SQLite
        save_backup_meta(target_date)                          # record manual backup date
        try:                                                   # also write Excel (best-effort)
            _last_df.to_excel(BACKUP_XLSX, index=False, engine='openpyxl')
        except Exception:
            pass
        return jsonify({'success': True, 'date': target_date})
    except Exception as e:
        return jsonify({'success': False, 'error': f'儲存失敗：{str(e)}'}), 500


@app.route('/api/backup-status')
def api_backup_status():
    """Return whether a manual backup exists (reads from SQLite)."""
    date_str = get_backup_meta()
    return jsonify({'exists': date_str is not None, 'date': date_str})


@app.route('/api/history-backup')
def api_history_backup():
    """Return the manually backed-up ranking (SQLite primary, Excel fallback)."""
    date_str = get_backup_meta()
    if not date_str:
        return jsonify({'success': False, 'error': '尚無備份資料'}), 404
    records = get_snapshot(date_str)
    if records:
        return jsonify({'success': True, 'data': records, 'date': date_str})
    # Fallback: read from Excel if SQLite row was pruned
    if os.path.exists(BACKUP_XLSX):
        try:
            df = pd.read_excel(BACKUP_XLSX, engine='openpyxl')
            df = df.where(pd.notnull(df), None)
            if '代號' in df.columns:
                df['代號'] = df['代號'].astype(str)
            if '投信' in df.columns:
                df['投信'] = pd.to_numeric(df['投信'], errors='coerce').fillna(0).round().astype(int)
            return jsonify({'success': True, 'data': df.to_dict(orient='records'), 'date': date_str})
        except Exception as e:
            return jsonify({'success': False, 'error': str(e)}), 500
    return jsonify({'success': False, 'error': '查無備份資料'}), 404


@app.route('/api/sectors', methods=['GET'])
def api_get_sectors():
    """Return saved sector card configurations."""
    data = get_sector_configs()
    if data:
        return jsonify({'success': True, 'sectors': json.loads(data)})
    return jsonify({'success': True, 'sectors': []})


@app.route('/api/sectors', methods=['POST'])
def api_save_sectors():
    """Save sector card configurations."""
    body = request.get_json()
    if not body or 'sectors' not in body:
        return jsonify({'success': False, 'error': 'invalid body'}), 400
    save_sector_configs(json.dumps(body['sectors'], ensure_ascii=False))
    return jsonify({'success': True})


@app.route('/api/crown-ref')
def api_crown_ref():
    """Return the set of codes stored as crown reference."""
    codes = get_crown_ref()
    return jsonify({'success': True, 'codes': codes})


@app.route('/api/stocks')
def api_stocks():
    global _last_df
    try:
        df = run_stock_update()
        _last_df = df
        trading_date = last_trading_day()
        save_snapshot(df, trading_date)

        records = df.where(pd.notnull(df), None).to_dict(orient='records')
        return jsonify({
            'success': True,
            'data': records,
            'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'count': len(records),
            'date': trading_date,
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/history')
def api_history():
    """Return available dates, or snapshot for a specific date."""
    date = request.args.get('date')
    if date:
        records = get_snapshot(date)
        if not records:
            return jsonify({'success': False, 'error': '查無此日期資料'}), 404
        return jsonify({'success': True, 'data': records, 'date': date, 'count': len(records)})
    else:
        dates = get_history_dates()
        return jsonify({'success': True, 'dates': dates})


init_db()

# Remove snapshots saved on weekends (cleanup for old bad data)
def purge_weekend_snapshots():
    con = sqlite3.connect(DB_PATH)
    dates = [d[0] for d in con.execute('SELECT DISTINCT date FROM snapshots').fetchall()]
    for d in dates:
        try:
            if datetime.strptime(d, '%Y-%m-%d').weekday() >= 5:
                con.execute('DELETE FROM snapshots WHERE date=?', (d,))
        except Exception:
            pass
    con.commit()
    con.close()

purge_weekend_snapshots()

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
