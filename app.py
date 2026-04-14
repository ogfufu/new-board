import os
import io
import json
import sqlite3
import threading
from datetime import datetime, date, timedelta

import pandas as pd
import requests
import twstock
from flask import Flask, jsonify, render_template, request

app = Flask(__name__)

HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/91.0.4472.124 Safari/537.36'
    )
}

DB_PATH   = os.path.join(os.path.dirname(__file__), 'history.db')
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
            market     TEXT,
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
    # Migration: add market column to older snapshots tables
    try:
        con.execute('ALTER TABLE snapshots ADD COLUMN market TEXT')
    except sqlite3.OperationalError:
        pass  # column already exists

    # Crown reference: independent of date, stores the "previous" baseline
    con.execute('''
        CREATE TABLE IF NOT EXISTS crown_ref (
            code  TEXT PRIMARY KEY,
            name  TEXT,
            rank  INTEGER
        )
    ''')
    # Compare snapshot: user-triggered "複製到對照排行榜"
    con.execute('''
        CREATE TABLE IF NOT EXISTS compare_snapshot (
            rank       INTEGER,
            code       TEXT PRIMARY KEY,
            name       TEXT,
            market     TEXT,
            price      REAL,
            change_pct REAL,
            trust      INTEGER,
            yoy        REAL,
            yoy1       REAL,
            open       REAL,
            high       REAL,
            low        REAL,
            capital    REAL,
            industry   TEXT
        )
    ''')
    con.execute('''
        CREATE TABLE IF NOT EXISTS compare_meta (
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
    """Return list of codes saved as crown reference."""
    con = sqlite3.connect(DB_PATH)
    rows = con.execute('SELECT code FROM crown_ref').fetchall()
    con.close()
    return [r[0] for r in rows]


def save_compare_snapshot(df, date_str):
    """Replace compare snapshot with current df data."""
    con = sqlite3.connect(DB_PATH)
    con.execute('DELETE FROM compare_snapshot')
    rows = [
        (
            int(r['排序']),
            str(r['代號']),
            r['名稱'],
            r.get('市場'),
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
    con.executemany('INSERT OR REPLACE INTO compare_snapshot VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)', rows)
    con.execute('INSERT OR REPLACE INTO compare_meta VALUES (1, ?, ?)',
                (date_str, datetime.now().isoformat()))
    con.commit()
    con.close()


def get_compare_snapshot():
    """Return compare snapshot records."""
    con = sqlite3.connect(DB_PATH)
    rows = con.execute(
        '''SELECT rank,code,name,market,price,change_pct,trust,yoy,yoy1,
                  open,high,low,capital,industry
           FROM compare_snapshot ORDER BY rank'''
    ).fetchall()
    con.close()
    cols = ['排序','代號','名稱','市場','股價','漲跌幅','投信','月(YOY)','月-1(YOY)','開盤','最高','最低','資金(億)','產業類型']
    return [dict(zip(cols, r)) for r in rows]


def get_compare_meta():
    """Return saved compare date string, or None."""
    con = sqlite3.connect(DB_PATH)
    row = con.execute('SELECT date FROM compare_meta WHERE id=1').fetchone()
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
    """Most recent weekday on or before today."""
    d = date.today()
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d.strftime('%Y-%m-%d')


def save_snapshot(df, date_str, overwrite=False):
    """Save snapshot for date_str. If overwrite=False, skip when date exists."""
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
            r.get('市場'),
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
        'INSERT OR REPLACE INTO snapshots VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
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
        '''SELECT rank,code,name,market,price,change_pct,trust,yoy,yoy1,
                  open,high,low,capital,industry
           FROM snapshots WHERE date=? ORDER BY rank''',
        (date_str,)
    ).fetchall()
    con.close()
    cols = ['排序','代號','名稱','市場','股價','漲跌幅','投信','月(YOY)','月-1(YOY)','開盤','最高','最低','資金(億)','產業類型']
    return [dict(zip(cols, r)) for r in rows]


# ---------- In-memory cache ----------
_last_df = None
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


def get_histock_codes():
    """Get top-100 stock codes + volume (億) from HiStock."""
    url = 'https://histock.tw/stock/rank.aspx?m=13&p=all'
    r = requests.get(url, headers=HEADERS, timeout=20)
    r.raise_for_status()
    df_all = pd.read_html(io.StringIO(r.text))[0]
    df_all.columns = df_all.columns.str.replace('▼', '', regex=False)
    df = df_all[['代號', '成交值(億)']].copy()
    df['代號'] = df['代號'].astype(str)
    df['成交值(億)'] = pd.to_numeric(df['成交值(億)'], errors='coerce')
    return df.head(100)


def get_stock_market(code):
    """Return '上市' or '上櫃' using twstock.codes metadata."""
    try:
        info = twstock.codes.get(code)
        if info is None:
            return '上市'
        market = getattr(info, 'market', '') or ''
        m = market.upper()
        if 'OTC' in m or 'TPEX' in m or '上櫃' in market:
            return '上櫃'
        return '上市'
    except Exception:
        return '上市'


def _parse_num(s):
    """Parse numeric string from TWSE API; return float or None."""
    if s in ('-', '--', '', None):
        return None
    try:
        return float(str(s).replace(',', ''))
    except (ValueError, TypeError):
        return None


def get_twse_realtime(codes_markets):
    """
    Batch-fetch real-time price data from TWSE unified API.
    codes_markets: list of (code, market_str)
    Returns dict: code -> {name, price, change_pct, open, high, low}
    """
    TWSE_HDR = {**HEADERS, 'Referer': 'https://mis.twse.com.tw/stock/fibest.html'}
    result = {}
    batch_size = 50

    for i in range(0, len(codes_markets), batch_size):
        batch = codes_markets[i:i + batch_size]
        parts = []
        for code, mkt in batch:
            prefix = 'otc' if mkt == '上櫃' else 'tse'
            parts.append(f'{prefix}_{code}.tw')
        ex_ch = '|'.join(parts)
        url = (
            f'https://mis.twse.com.tw/stock/api/getStockInfo.jsp'
            f'?ex_ch={ex_ch}&json=1&delay=0'
        )
        try:
            resp = requests.get(url, headers=TWSE_HDR, timeout=20)
            resp.raise_for_status()
            data = resp.json()
            for item in data.get('msgArray', []):
                code = item.get('c', '')
                if not code:
                    continue
                y = _parse_num(item.get('y'))   # yesterday close
                z = _parse_num(item.get('z'))   # current / last price
                price = z if z is not None else y
                chg = 0.0
                if price is not None and y and y != 0:
                    chg = round((price - y) / y * 100, 2)
                result[code] = {
                    'name':       item.get('n', code),
                    'price':      price,
                    'change_pct': chg,
                    'open':       _parse_num(item.get('o')),
                    'high':       _parse_num(item.get('h')),
                    'low':        _parse_num(item.get('l')),
                }
        except Exception:
            pass

    return result


def run_stock_update():
    # 1. Top-100 codes + volume from HiStock
    df_codes = get_histock_codes()
    codes = df_codes['代號'].tolist()

    # 2. Market type for each code (上市 / 上櫃)
    codes_markets = [(c, get_stock_market(c)) for c in codes]
    market_map = dict(codes_markets)

    # 3. Real-time prices from TWSE
    price_data = get_twse_realtime(codes_markets)

    # 4. Wespai: 投信 + YOY
    df_wes = get_wespai_data()
    wes_idx = df_wes.set_index('代號')

    # 5. Merge
    rows = []
    for _, row in df_codes.iterrows():
        code = row['代號']
        cap  = row['成交值(億)']
        if isinstance(cap, float) and (cap != cap):  # NaN check
            cap = None
        pi   = price_data.get(code)
        if pi is None:
            continue

        name     = pi['name']
        trust    = 0
        yoy      = None
        yoy1     = None
        industry = ''

        if code in wes_idx.index:
            w = wes_idx.loc[code]
            company = str(w.get('公司', '') or '')
            if company:
                name = company
            t = pd.to_numeric(w.get('投信買賣超'), errors='coerce')
            trust = int(round(t)) if pd.notna(t) else 0
            yv = pd.to_numeric(w.get('(月)營收年增率(%)'), errors='coerce')
            yoy = float(yv) if pd.notna(yv) else None
            yv1 = pd.to_numeric(w.get('(月-1)營收年增率(%)'), errors='coerce')
            yoy1 = float(yv1) if pd.notna(yv1) else None
            industry = str(w.get('產業類型', '') or '')

        rows.append({
            '代號':      code,
            '名稱':      name,
            '市場':      market_map.get(code, '上市'),
            '股價':      pi['price'],
            '漲跌幅':    pi['change_pct'],
            '開盤':      pi['open'],
            '最高':      pi['high'],
            '最低':      pi['low'],
            '投信':      trust,
            '月(YOY)':   yoy,
            '月-1(YOY)': yoy1,
            '資金(億)':  cap,
            '產業類型':  industry,
        })

    if not rows:
        raise ValueError('無法取得任何股票資料')

    df = pd.DataFrame(rows)
    df = df.sort_values('資金(億)', ascending=False).head(100).reset_index(drop=True)
    df.insert(0, '排序', range(1, len(df) + 1))

    final_cols = ['排序','代號','名稱','市場','股價','漲跌幅','投信',
                  '月(YOY)','月-1(YOY)','開盤','最高','最低','資金(億)','產業類型']
    return df[final_cols]


# ---------- Routes ----------

@app.route('/')
def index():
    return render_template('index.html')


@app.route('/api/compare', methods=['POST'])
def api_save_compare():
    """Copy current ranking to 對照排行榜."""
    global _last_df
    if _last_df is None:
        try:
            _last_df = run_stock_update()
        except Exception as e:
            return jsonify({'success': False, 'error': f'資料抓取失敗：{str(e)}'}), 500
    try:
        target_date = last_trading_day()
        save_compare_snapshot(_last_df, target_date)
        save_crown_ref(_last_df)
        return jsonify({'success': True, 'date': target_date})
    except Exception as e:
        return jsonify({'success': False, 'error': f'儲存失敗：{str(e)}'}), 500


@app.route('/api/compare', methods=['GET'])
def api_get_compare():
    """Return 對照排行榜 snapshot."""
    date_str = get_compare_meta()
    if not date_str:
        return jsonify({'success': False, 'error': '尚無對照資料'}), 404
    records = get_compare_snapshot()
    if not records:
        return jsonify({'success': False, 'error': '查無對照資料'}), 404
    return jsonify({'success': True, 'data': records, 'date': date_str})


@app.route('/api/compare-status')
def api_compare_status():
    """Return whether a compare snapshot exists."""
    date_str = get_compare_meta()
    return jsonify({'exists': date_str is not None, 'date': date_str})


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

        # Use pandas to_json → json.loads to guarantee NaN → null (prevents invalid JSON)
        records = json.loads(df.to_json(orient='records', force_ascii=False))
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
