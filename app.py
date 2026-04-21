import os
import io
import re
import json
import sqlite3
import threading
from datetime import datetime, date, timedelta

import urllib3
import pandas as pd
import requests
import twstock
import gspread
from bs4 import BeautifulSoup
from google.oauth2.service_account import Credentials

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from flask import Flask, jsonify, render_template, request

# ---------- Google Sheets ----------
GSHEET_ID     = '1lRu7XAzla5K4JnM6ZGR3dXOAA-XC8EBNWLXzt3COGvY'
GSHEET_SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# 優先用環境變數（Railway 部署用），本機開發則 fallback 到 JSON 檔案
_CREDS_FILE = os.path.join(os.path.dirname(__file__), 'vital-form-493406-t6-809b6e596f6a.json')

def _get_gsheet():
    creds_json = os.environ.get('GOOGLE_CREDENTIALS_JSON', '')
    if creds_json:
        # Railway：從環境變數讀取 JSON 字串
        info = json.loads(creds_json)
        creds = Credentials.from_service_account_info(info, scopes=GSHEET_SCOPES)
    elif os.path.exists(_CREDS_FILE):
        # 本機開發：從 JSON 檔案讀取
        creds = Credentials.from_service_account_file(_CREDS_FILE, scopes=GSHEET_SCOPES)
    else:
        raise RuntimeError('找不到 Google 憑證：請設定環境變數 GOOGLE_CREDENTIALS_JSON')
    gc = gspread.authorize(creds)
    return gc.open_by_key(GSHEET_ID).sheet1


def _get_gsheet_sectors():
    """Return Sheet2 (sectors watchlist). Creates it if it doesn't exist."""
    creds_json = os.environ.get('GOOGLE_CREDENTIALS_JSON', '')
    if creds_json:
        info = json.loads(creds_json)
        creds = Credentials.from_service_account_info(info, scopes=GSHEET_SCOPES)
    elif os.path.exists(_CREDS_FILE):
        creds = Credentials.from_service_account_file(_CREDS_FILE, scopes=GSHEET_SCOPES)
    else:
        raise RuntimeError('找不到 Google 憑證')
    gc = gspread.authorize(creds)
    wb = gc.open_by_key(GSHEET_ID)
    try:
        return wb.get_worksheet(1)
    except Exception:
        return wb.add_worksheet(title='sectors', rows=10, cols=2)


def save_sectors_to_gsheet(sectors):
    """Save sectors JSON to Sheet2 cell A1."""
    ws = _get_gsheet_sectors()
    ws.clear()
    ws.update('A1', [[json.dumps(sectors, ensure_ascii=False)]])


def get_sectors_from_gsheet():
    """Read sectors JSON from Sheet2 cell A1. Returns list or None."""
    try:
        ws = _get_gsheet_sectors()
        val = ws.cell(1, 1).value
        if val:
            data = json.loads(val)
            if isinstance(data, list):
                return data
    except Exception:
        pass
    return None


# ---------- Wiki index / cache (Sheet3 / Sheet4) ----------

def _get_google_creds():
    creds_json = os.environ.get('GOOGLE_CREDENTIALS_JSON', '')
    if creds_json:
        info = json.loads(creds_json)
        return Credentials.from_service_account_info(info, scopes=GSHEET_SCOPES)
    if os.path.exists(_CREDS_FILE):
        return Credentials.from_service_account_file(_CREDS_FILE, scopes=GSHEET_SCOPES)
    raise RuntimeError('找不到 Google 憑證')


def _get_or_create_ws(index, title, rows=600, cols=6):
    gc = gspread.authorize(_get_google_creds())
    wb = gc.open_by_key(GSHEET_ID)
    sheets = wb.worksheets()
    if len(sheets) > index:
        return sheets[index]
    return wb.add_worksheet(title=title, rows=rows, cols=cols)


def _wiki_index_ws():
    return _get_or_create_ws(2, 'wiki_index', rows=600, cols=2)

def _wiki_cache_ws():
    return _get_or_create_ws(3, 'wiki_cache', rows=600, cols=6)


# ── 建立索引：抓 MoneyDJ wiki 列表所有頁 ──
def build_wiki_index_from_moneydj():
    """Scrape all pages of MoneyDJ wiki list using a persistent session (for cookies).
    Returns list of [name, url].
    """
    import time
    base = 'https://www.moneydj.com/kmdj/wiki/wikisubjectlist.aspx'

    # 使用 Session 維持 ASP.NET 的 session cookie
    sess = requests.Session()
    sess.headers.update({
        **HEADERS,
        'Referer': 'https://www.moneydj.com/kmdj/wiki/wikiList.djhtm',
    })

    # 先打首頁取得 session cookie，並讀取總頁數
    total_pages = 19  # 預設值
    try:
        r0 = sess.get(base + '?op=1', timeout=20)
        r0.raise_for_status()
        soup0 = BeautifulSoup(r0.text, 'html.parser')
        # 找分頁連結中最大的 index1 值
        max_idx = 0
        for a in soup0.find_all('a', href=True):
            m = re.search(r'index1=(\d+)', a['href'])
            if m:
                max_idx = max(max_idx, int(m.group(1)))
        if max_idx > 0:
            total_pages = max_idx + 1
        print(f'[wiki-index] total pages detected: {total_pages}', flush=True)
    except Exception as e:
        print(f'[wiki-index] init request failed: {e}', flush=True)

    entries = []
    seen_urls = set()   # 去除重複

    for page in range(0, total_pages):
        url = f'{base}?op=1' if page == 0 else f'{base}?index1={page}&op=1'
        try:
            r = sess.get(url, timeout=20)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, 'html.parser')
            count = 0
            for a in soup.find_all('a', href=True):
                href = a['href']
                if 'wikiviewer.aspx?keyid=' in href:
                    name = a.get_text(strip=True)
                    if name and len(name) > 2:
                        full_url = (href if href.startswith('http')
                                    else 'https://www.moneydj.com' + href)
                        if full_url not in seen_urls:
                            seen_urls.add(full_url)
                            entries.append([name, full_url])
                            count += 1
            print(f'[wiki-index] page {page}: +{count} entries (total {len(entries)})', flush=True)
        except Exception as e:
            print(f'[wiki-index] page {page} error: {e}', flush=True)
        time.sleep(0.5)   # 避免對伺服器造成過大壓力

    return entries


def save_wiki_index(entries):
    ws = _wiki_index_ws()
    ws.clear()
    ws.update('A1', [['公司全名', 'wiki_url']])
    if entries:
        ws.update('A2', entries)


def get_wiki_index():
    """Returns dict: full_name → wiki_url"""
    try:
        ws = _wiki_index_ws()
        rows = ws.get_all_values()
        return {r[0]: r[1] for r in rows[1:] if len(r) >= 2 and r[0] and r[1]}
    except Exception:
        return {}


def find_wiki_url(stock_name, index_dict):
    """Match stock short name to full company name (exact → partial)."""
    for full, url in index_dict.items():
        if stock_name == full:
            return url
    for full, url in index_dict.items():
        if stock_name in full:
            return url
    return None


# ── 抓取 wiki 頁面並解析四欄 ──
def fetch_wiki_profile(wiki_url):
    """Fetch MoneyDJ wiki page, extract 4 fields. Returns dict or None."""
    try:
        r = requests.get(wiki_url, headers=HEADERS, timeout=20)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, 'html.parser')

        # 嘗試找主要內容 div
        content = (
            soup.find('div', class_=lambda c: c and 'content' in c.lower()) or
            soup.find('td', id=lambda i: i and 'content' in (i or '').lower()) or
            soup.body
        )
        text = (content or soup.body).get_text(separator='\n', strip=True)
        lines = [l.strip() for l in text.split('\n') if l.strip() and len(l.strip()) > 2]

        def extract_after(keywords, max_chars=350):
            for i, line in enumerate(lines):
                if any(kw in line for kw in keywords):
                    parts = []
                    for j in range(i + 1, min(i + 30, len(lines))):
                        l = lines[j]
                        # 遇到下一個段落標題則停止
                        if re.match(r'^[（(]?[一二三四五六七八九十\d]+[）)\.、]', l) and len(l) < 40:
                            if parts:
                                break
                        parts.append(l)
                        if sum(len(p) for p in parts) >= max_chars:
                            break
                    result = '　'.join(parts)
                    return result[:max_chars] if result else ''
            return ''

        return {
            '產業':   extract_after(['產業特性', '產業概況', '行業概述', '沿革', '成立']) or '—',
            '營收分布': extract_after(['營收比重', '營業比重', '業務比重', '產品比重', '銷售比重', '業務結構', '主要業務']) or '—',
            '核心產品': extract_after(['產品與技術', '主要產品', '核心產品', '產品簡介', '技術簡介', '產品項目']) or '—',
            '主要客戶': extract_after(['主要客戶', '客戶結構', '銷售對象', '重要客戶']) or '—',
        }
    except Exception as e:
        print(f'[wiki-profile] {wiki_url}: {e}', flush=True)
        return None


# ── Sheet4 快取讀寫 ──
def get_wiki_cache():
    """Returns dict: code → {產業, 營收分布, 核心產品, 主要客戶, 抓取日期}"""
    try:
        ws = _wiki_cache_ws()
        rows = ws.get_all_values()
        if len(rows) < 2:
            return {}
        headers = rows[0]
        return {row[0]: dict(zip(headers, row)) for row in rows[1:] if row and row[0]}
    except Exception:
        return {}


def save_wiki_profile_to_cache(code, profile):
    """Upsert one stock's profile in Sheet4."""
    try:
        ws = _wiki_cache_ws()
        rows = ws.get_all_values()
        today = date.today().isoformat()
        new_row = [
            code,
            profile.get('產業', ''),
            profile.get('營收分布', ''),
            profile.get('核心產品', ''),
            profile.get('主要客戶', ''),
            today,
        ]
        header = ['代號', '產業', '營收分布', '核心產品', '主要客戶', '抓取日期']
        if not rows:
            ws.update('A1', [header])
            ws.append_row(new_row)
            return
        # 確保有標頭
        if rows[0][0] != '代號':
            ws.insert_row(header, 1)
            rows = [header] + rows
        # 找既有列並更新
        for i, row in enumerate(rows[1:], start=2):
            if row and row[0] == code:
                ws.update(f'A{i}', [new_row])
                return
        ws.append_row(new_row)
    except Exception as e:
        print(f'[wiki-cache] save error: {e}', flush=True)


GSHEET_COLS = ['排序','代號','名稱','市場','股價','漲跌幅','外資','投信',
               '月(YOY)','月-1(YOY)','開盤','最高','最低','資金(億)','產業類型']

def save_compare_to_gsheet(df, date_str):
    """寫入 Google Sheet：第1列=日期，第2列=欄位名，第3列起=資料（含🔒漲停保留）。"""
    ws = _get_gsheet()
    ws.clear()
    # 第1列：日期
    ws.update('A1', [[f'更新日期:{date_str}']])
    # 第2列：欄位名稱
    ws.update('A2', [GSHEET_COLS])
    # 第3列起：全部資料（含🔒）
    rows = []
    for _, r in df.iterrows():
        rows.append([str(r.get(c, '') or '') for c in GSHEET_COLS])
    if rows:
        ws.update(f'A3', rows)

def get_compare_from_gsheet():
    """從 Google Sheet 讀取對照排行榜，回傳 (records_list, date_str)。"""
    try:
        ws = _get_gsheet()
        all_values = ws.get_all_values()
        if len(all_values) < 3:
            return [], None
        date_str = all_values[0][0].replace('更新日期:', '').strip()
        headers  = all_values[1]
        records  = []
        for row in all_values[2:]:
            if any(row):
                rec = {}
                for h, v in zip(headers, row):
                    # 數字欄位轉型
                    if h in ('排序','外資','投信'):
                        try: rec[h] = int(float(v)) if v != '' else None
                        except: rec[h] = None
                    elif h in ('股價','漲跌幅','月(YOY)','月-1(YOY)','開盤','最高','最低','資金(億)'):
                        try: rec[h] = float(v) if v != '' else None
                        except: rec[h] = None
                    else:
                        rec[h] = v
                records.append(rec)
        return records, date_str
    except Exception as e:
        return [], None

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
    # Migration: add columns to older snapshots tables
    for col_def in ('market TEXT', 'foreign_inv INTEGER'):
        try:
            con.execute(f'ALTER TABLE snapshots ADD COLUMN {col_def}')
        except sqlite3.OperationalError:
            pass
    for col_def in ('foreign_inv INTEGER',):
        try:
            con.execute(f'ALTER TABLE compare_snapshot ADD COLUMN {col_def}')
        except sqlite3.OperationalError:
            pass

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
    """Replace the crown reference with current data（含🔒漲停保留，排序存0）。"""
    con = sqlite3.connect(DB_PATH)
    con.execute('DELETE FROM crown_ref')
    rows = []
    for _, r in df.iterrows():
        try:
            rank = int(r['排序'])
        except (ValueError, TypeError):
            rank = 0  # 🔒 漲停保留列存排序 0
        rows.append((str(r['代號']), r['名稱'], rank))
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
            int(r['投信'])     if r.get('投信')     is not None else None,
            int(r['外資'])     if r.get('外資')     is not None else None,
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
        '''INSERT OR REPLACE INTO compare_snapshot
           (rank,code,name,market,price,change_pct,trust,foreign_inv,
            yoy,yoy1,open,high,low,capital,industry)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
        rows
    )
    con.execute('INSERT OR REPLACE INTO compare_meta VALUES (1, ?, ?)',
                (date_str, datetime.now().isoformat()))
    con.commit()
    con.close()


def get_compare_snapshot():
    """Return compare snapshot records."""
    con = sqlite3.connect(DB_PATH)
    rows = con.execute(
        '''SELECT rank,code,name,market,price,change_pct,trust,foreign_inv,yoy,yoy1,
                  open,high,low,capital,industry
           FROM compare_snapshot ORDER BY rank'''
    ).fetchall()
    con.close()
    cols = ['排序','代號','名稱','市場','股價','漲跌幅','投信','外資','月(YOY)','月-1(YOY)','開盤','最高','最低','資金(億)','產業類型']
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

    rows = []
    for _, r in df.iterrows():
        try:
            rank = int(r['排序'])
        except (ValueError, TypeError):
            rank = 0  # 🔒 漲停保留列存排序 0
        rows.append((
            date_str,
            rank,
            str(r['代號']),
            r['名稱'],
            r.get('市場'),
            r['股價'],
            r['漲跌幅'],
            int(r['投信']) if r.get('投信') is not None else None,
            int(r['外資']) if r.get('外資') is not None else None,
            r['月(YOY)'],
            r['月-1(YOY)'],
            r['開盤'],
            r['最高'],
            r['最低'],
            r['資金(億)'],
            r['產業類型'],
        ))
    con.executemany(
        '''INSERT OR REPLACE INTO snapshots
           (date,rank,code,name,market,price,change_pct,trust,foreign_inv,
            yoy,yoy1,open,high,low,capital,industry)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
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
        '''SELECT rank,code,name,market,price,change_pct,trust,foreign_inv,yoy,yoy1,
                  open,high,low,capital,industry
           FROM snapshots WHERE date=? ORDER BY rank''',
        (date_str,)
    ).fetchall()
    con.close()
    cols = ['排序','代號','名稱','市場','股價','漲跌幅','投信','外資','月(YOY)','月-1(YOY)','開盤','最高','最低','資金(億)','產業類型']
    return [dict(zip(cols, r)) for r in rows]


# ---------- In-memory cache ----------
_last_df = None
_wespai_cache = {'data': None, 'date': None}
_wespai_lock = threading.Lock()

# ---------- 漲停保留：記錄當日曾進前100的代號 ----------
_daily_seen_top100 = set()   # 當日累積曾進前100的代號
_daily_seen_date   = None    # 記錄日期，換日時重置


def get_wespai_data():
    with _wespai_lock:
        now = datetime.now()
        # Wespai 每晚 20:00 更新，cache key 區分 20:00 前後
        # 20:00 前：用 {date}_pre，20:00 後：用 {date}_post（觸發重抓）
        slot = 'post' if now.hour >= 20 else 'pre'
        cache_key = now.strftime('%Y-%m-%d') + '_' + slot
        if _wespai_cache['data'] is not None and _wespai_cache['date'] == cache_key:
            return _wespai_cache['data']

        url = 'https://stock.wespai.com/p/75789'
        r = requests.get(url, headers=HEADERS, timeout=20)
        r.raise_for_status()
        df_all = pd.read_html(io.StringIO(r.text))[0]
        # Flatten multi-level columns if present
        if isinstance(df_all.columns, pd.MultiIndex):
            df_all.columns = [' '.join(str(c) for c in col).strip() for col in df_all.columns]
        want = ['代號', '公司', '外資買賣超', '投信買賣超',
                '(月)營收年增率(%)', '(月-1)營收年增率(%)', '產業類型']
        available = [c for c in want if c in df_all.columns]
        df = df_all[available].copy()
        df['代號'] = df['代號'].astype(str)

        _wespai_cache['data'] = df
        _wespai_cache['date'] = cache_key
        return df


def get_histock_codes():
    """Get stock codes + price info + volume (億) from HiStock.
    Fetches top-150 so that after filtering out no-YOY stocks we still have 100."""
    url = 'https://histock.tw/stock/rank.aspx?m=13&p=all'
    r = requests.get(url, headers=HEADERS, timeout=20)
    r.raise_for_status()
    df_all = pd.read_html(io.StringIO(r.text))[0]
    df_all.columns = df_all.columns.str.replace('▼', '', regex=False)
    # 依欄位索引取值，避免編碼問題
    # col[0]=代號, col[1]=名稱, col[2]=股價, col[4]=漲跌%
    # col[7]=開盤, col[8]=最高, col[9]=最低, col[12]=成交值(億)
    df = df_all.iloc[:, [0, 1, 2, 4, 7, 8, 9, 12]].copy()
    df.columns = ['代號', '名稱', '股價', '漲跌幅%', '開盤', '最高', '最低', '成交值(億)']
    df['代號'] = df['代號'].astype(str)
    df['股價']    = pd.to_numeric(df['股價'],    errors='coerce')
    df['漲跌幅%'] = (df['漲跌幅%'].astype(str)
                     .str.replace('%', '', regex=False).str.strip()
                     .pipe(pd.to_numeric, errors='coerce'))
    df['開盤']    = pd.to_numeric(df['開盤'],    errors='coerce')
    df['最高']    = pd.to_numeric(df['最高'],    errors='coerce')
    df['最低']    = pd.to_numeric(df['最低'],    errors='coerce')
    df['成交值(億)'] = pd.to_numeric(df['成交值(億)'], errors='coerce')
    return df.head(200)  # 多抓 buffer，確保漲停鎖住的標的仍在範圍內


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
            resp = requests.get(url, headers=TWSE_HDR, timeout=20, verify=False)
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
        except Exception as e:
            print(f'[TWSE ERROR] batch {i}: {e}', flush=True)

    return result


def run_stock_update():
    global _daily_seen_top100, _daily_seen_date
    # 1. Top-100 codes + 股價/漲跌幅/開高低 from HiStock
    df_codes = get_histock_codes()
    codes = df_codes['代號'].tolist()

    # 2. Market type for each code (上市 / 上櫃)
    market_map = {c: get_stock_market(c) for c in codes}

    # 3. Wespai: 投信 + YOY
    df_wes = get_wespai_data()
    wes_idx = df_wes.set_index('代號')

    # 4. Merge（股價來自 HiStock）
    rows = []
    for _, row in df_codes.iterrows():
        code  = row['代號']
        cap   = row['成交值(億)']
        if isinstance(cap, float) and (cap != cap):
            cap = None
        price  = row['股價']
        chg    = row['漲跌幅%']
        open_p = row['開盤']
        high_p = row['最高']
        low_p  = row['最低']
        if pd.isna(price):
            continue

        name     = row['名稱']
        trust    = 0
        foreign  = 0
        yoy      = None
        yoy1     = None
        industry = ''

        if code in wes_idx.index:
            w = wes_idx.loc[code]
            # If duplicate codes in wespai, take first row
            if isinstance(w, pd.DataFrame):
                w = w.iloc[0]
            company = str(w.get('公司', '') or '')
            if company:
                name = company
            t = pd.to_numeric(w.get('投信買賣超'), errors='coerce')
            trust = int(round(t)) if pd.notna(t) else 0
            f = pd.to_numeric(w.get('外資買賣超'), errors='coerce')
            foreign = int(round(f)) if pd.notna(f) else 0
            yv = pd.to_numeric(w.get('(月)營收年增率(%)'), errors='coerce')
            yoy = float(yv) if pd.notna(yv) else None
            yv1 = pd.to_numeric(w.get('(月-1)營收年增率(%)'), errors='coerce')
            yoy1 = float(yv1) if pd.notna(yv1) else None
            industry = str(w.get('產業類型', '') or '')

        rows.append({
            '代號':      code,
            '名稱':      name,
            '市場':      market_map.get(code, '上市'),
            '股價':      price,
            '漲跌幅':    chg if pd.notna(chg) else 0.0,
            '開盤':      None if pd.isna(open_p) else open_p,
            '最高':      None if pd.isna(high_p) else high_p,
            '最低':      None if pd.isna(low_p)  else low_p,
            '投信':      trust,
            '外資':      foreign,
            '月(YOY)':   yoy,
            '月-1(YOY)': yoy1,
            '資金(億)':  cap,
            '產業類型':  industry,
        })

    if not rows:
        raise ValueError('無法取得任何股票資料')

    df = pd.DataFrame(rows)

    # ── 換日重置 ──
    import datetime as _dt
    today = _dt.date.today().isoformat()
    if _daily_seen_date != today:
        _daily_seen_top100.clear()
        _daily_seen_date = today  # type: ignore

    # ── 前100名（需有YOY，過濾ETF）──
    df_yoy    = df[df['月(YOY)'].notna()].copy()
    df_sorted = df_yoy.sort_values('資金(億)', ascending=False).reset_index(drop=True)
    top100    = df_sorted.head(100).copy()

    # 累積當日曾進前100的代號
    _daily_seen_top100.update(top100['代號'].tolist())

    top100['漲停保留'] = False
    top100.insert(0, '排序', range(1, len(top100) + 1))

    # ── 漲停保留：曾進前100、現在不在前100、且仍漲停（漲跌幅 >= 9.5%）──
    top100_codes  = set(top100['代號'])
    dropped_codes = _daily_seen_top100 - top100_codes   # 曾在但現在掉出的

    # 從 buffer（200筆，含無YOY的）中找回這些代號，確認仍漲停
    limitup_extra = df[
        df['代號'].isin(dropped_codes) &
        (df['漲跌幅'] >= 9.5)
    ].copy()

    if not limitup_extra.empty:
        limitup_extra['漲停保留'] = True
        limitup_extra.insert(0, '排序', ['🔒'] * len(limitup_extra))
        df_final = pd.concat([top100, limitup_extra], ignore_index=True)
    else:
        df_final = top100

    final_cols = ['排序','代號','名稱','市場','股價','漲跌幅','外資','投信',
                  '月(YOY)','月-1(YOY)','開盤','最高','最低','資金(億)','產業類型','漲停保留']
    # 確保所有欄位存在
    for c in final_cols:
        if c not in df_final.columns:
            df_final[c] = None
    return df_final[final_cols]


# ---------- Routes ----------

@app.route('/')
def index():
    return render_template('index.html')


@app.route('/api/compare', methods=['POST'])
def api_save_compare():
    """Copy current ranking to 對照排行榜 (Google Sheets)."""
    global _last_df
    try:
        df = _last_df if _last_df is not None else run_stock_update()
        _last_df = df
    except Exception as e:
        return jsonify({'success': False, 'error': f'資料抓取失敗：{str(e)}'}), 500
    try:
        target_date = last_trading_day()
        save_compare_to_gsheet(df, target_date)  # 寫入 Google Sheets
        save_compare_snapshot(df, target_date)   # 同時保留本機備份
        save_crown_ref(df)
        return jsonify({'success': True, 'date': target_date})
    except Exception as e:
        return jsonify({'success': False, 'error': f'儲存失敗：{str(e)}'}), 500


@app.route('/api/compare', methods=['GET'])
def api_get_compare():
    """Return 對照排行榜 snapshot (從 Google Sheets 讀取)."""
    records, date_str = get_compare_from_gsheet()
    if not date_str or not records:
        # 若 Google Sheets 讀取失敗，退回本機備份
        date_str = get_compare_meta()
        if not date_str:
            return jsonify({'success': False, 'error': '尚無對照資料'}), 404
        records = get_compare_snapshot()
        if not records:
            return jsonify({'success': False, 'error': '查無對照資料'}), 404
    return jsonify({'success': True, 'data': records, 'date': date_str})


@app.route('/api/refresh-wespai', methods=['POST'])
def api_refresh_wespai():
    """強制清除 Wespai 快取並重新抓取最新基本面資料。"""
    global _last_df
    with _wespai_lock:
        _wespai_cache['data'] = None
        _wespai_cache['date'] = None
    try:
        get_wespai_data()          # 重新抓取
        _last_df = None            # 清除股票快取，下次刷新重新合併
        return jsonify({'success': True, 'message': 'Wespai 基本面資料已更新'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/gsheet-test')
def api_gsheet_test():
    """診斷用：測試 Google Sheets 連線是否正常。"""
    try:
        ws = _get_gsheet()
        title = ws.spreadsheet.title
        return jsonify({'success': True, 'sheet_title': title})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/compare-status')
def api_compare_status():
    """Return whether a compare snapshot exists (檢查 Google Sheets)."""
    _, date_str = get_compare_from_gsheet()
    if not date_str:
        date_str = get_compare_meta()  # 退回本機備份
    return jsonify({'exists': date_str is not None, 'date': date_str})


@app.route('/api/sectors', methods=['GET'])
def api_get_sectors():
    """Return saved sector card configurations (Google Sheets first, SQLite fallback)."""
    # Try Google Sheets first (cross-device sync)
    try:
        gs_data = get_sectors_from_gsheet()
        if gs_data is not None:
            # Also update local SQLite cache
            save_sector_configs(json.dumps(gs_data, ensure_ascii=False))
            return jsonify({'success': True, 'sectors': gs_data, 'source': 'gsheet'})
    except Exception:
        pass
    # Fallback to SQLite
    data = get_sector_configs()
    if data:
        return jsonify({'success': True, 'sectors': json.loads(data), 'source': 'sqlite'})
    return jsonify({'success': True, 'sectors': [], 'source': 'empty'})


@app.route('/api/sectors', methods=['POST'])
def api_save_sectors():
    """Save sector card configurations (SQLite + Google Sheets)."""
    body = request.get_json()
    if not body or 'sectors' not in body:
        return jsonify({'success': False, 'error': 'invalid body'}), 400
    sectors_data = body['sectors']
    # Always save to SQLite
    save_sector_configs(json.dumps(sectors_data, ensure_ascii=False))
    # Also save to Google Sheets (best-effort)
    try:
        save_sectors_to_gsheet(sectors_data)
    except Exception:
        pass
    return jsonify({'success': True})


@app.route('/api/crown-ref')
def api_crown_ref():
    """Return codes from Google Sheets compare snapshot (for 👑 comparison)."""
    records, date_str = get_compare_from_gsheet()
    if records:
        codes = [str(r.get('代號', '')) for r in records if r.get('代號')]
    else:
        # 退回本機 SQLite 備份
        codes = get_crown_ref()
    return jsonify({'success': True, 'codes': codes})


@app.route('/api/limit-up')
def api_limit_up():
    """漲停股列表：從 HiStock 抓全部股票，過濾漲跌幅>9%，合併 Wespai 外資/投信/產業。"""
    try:
        # 1. 抓全部股票
        url = 'https://histock.tw/stock/rank.aspx?m=1&p=all'
        r = requests.get(url, headers=HEADERS, timeout=20)
        r.raise_for_status()
        df_all = pd.read_html(io.StringIO(r.text))[0]
        df_all.columns = df_all.columns.str.replace('▼', '', regex=False)
        # col[0]=代號,col[1]=名稱,col[2]=股價,col[4]=漲跌%,col[7]=開盤,col[8]=最高,col[9]=最低,col[12]=成交值
        df_all = df_all.iloc[:, [0, 1, 2, 4, 7, 8, 9, 12]].copy()
        df_all.columns = ['代號', '名稱', '股價', '漲跌幅%', '開盤', '最高', '最低', '成交值(億)']
        df_all['代號'] = df_all['代號'].astype(str)
        for col in ['股價', '開盤', '最高', '最低', '成交值(億)']:
            df_all[col] = pd.to_numeric(df_all[col], errors='coerce')
        df_all['漲跌幅%'] = (df_all['漲跌幅%'].astype(str)
                             .str.replace('%', '', regex=False)
                             .str.replace('+', '', regex=False)
                             .str.strip()
                             .pipe(pd.to_numeric, errors='coerce'))

        # 2. 過濾漲跌幅 > 9%
        df_limit = df_all[df_all['漲跌幅%'] > 9].copy().reset_index(drop=True)

        # 3. 合併 Wespai 外資/投信/產業
        df_wes = get_wespai_data()
        wes_idx = df_wes.set_index('代號')

        rows = []
        for _, row in df_limit.iterrows():
            code = row['代號']
            trust = 0; foreign = 0; industry = ''
            if code in wes_idx.index:
                w = wes_idx.loc[code]
                if isinstance(w, pd.DataFrame): w = w.iloc[0]
                t = pd.to_numeric(w.get('投信買賣超'), errors='coerce')
                trust = int(round(t)) if pd.notna(t) else 0
                f = pd.to_numeric(w.get('外資買賣超'), errors='coerce')
                foreign = int(round(f)) if pd.notna(f) else 0
                industry = str(w.get('產業類型', '') or '')
            rows.append({
                '代號':     code,
                '名稱':     row['名稱'],
                '股價':     row['股價'],
                '漲跌幅':   row['漲跌幅%'],
                '開盤':     row['開盤'],
                '最高':     row['最高'],
                '最低':     row['最低'],
                '外資':     foreign,
                '投信':     trust,
                '資金(億)': row['成交值(億)'],
                '產業類型': industry,
            })

        records = json.loads(pd.DataFrame(rows).to_json(orient='records', force_ascii=False))
        return jsonify({'success': True, 'data': records, 'count': len(records)})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


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


@app.route('/api/wiki-index/build', methods=['POST'])
def api_build_wiki_index():
    """掃描 MoneyDJ wiki 列表所有頁（19頁）並存入 Sheet3。"""
    try:
        entries = build_wiki_index_from_moneydj()
        save_wiki_index(entries)
        return jsonify({'success': True, 'count': len(entries)})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/stock-wiki', methods=['POST'])
def api_stock_wiki():
    """查詢個股 wiki 摘要。先查 Sheet4 快取，沒有則從 MoneyDJ 抓取後存入 Sheet4。"""
    body = request.get_json()
    code = str(body.get('code', '')).strip()
    name = str(body.get('name', '')).strip()
    if not code or not name:
        return jsonify({'success': False, 'error': 'missing code or name'}), 400

    # 1. 查 Sheet4 快取
    cache = get_wiki_cache()
    if code in cache:
        return jsonify({'success': True, 'data': cache[code], 'source': 'cache'})

    # 2. 在 Sheet3 索引中找 wiki URL
    index = get_wiki_index()
    if not index:
        return jsonify({'success': False, 'error': '索引尚未建立，請先按「建立Wiki索引」'}), 404
    wiki_url = find_wiki_url(name, index)
    if not wiki_url:
        return jsonify({'success': False, 'error': f'找不到「{name}」，可能尚未收錄於 MoneyDJ'}), 404

    # 3. 抓取並解析 wiki 頁面
    profile = fetch_wiki_profile(wiki_url)
    if not profile:
        return jsonify({'success': False, 'error': '無法解析 wiki 頁面'}), 500

    # 4. 存入 Sheet4
    save_wiki_profile_to_cache(code, profile)
    profile['代號'] = code
    profile['抓取日期'] = date.today().isoformat()
    return jsonify({'success': True, 'data': profile, 'source': 'fetched'})


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
