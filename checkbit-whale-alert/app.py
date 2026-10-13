"""CheckBit Whale Alert: a single-process FastAPI companion service.

Monitors a configured watchlist of Bitcoin ADDRESSES (not ownership entities).
Price data: Upbit KRW-BTC REST, polled every PRICE_POLL_SECONDS.
Address data: Blockstream Esplora, polled on a controlled rotating schedule.
For large watchlists use the authenticated ingestion endpoint and your existing indexer.
"""
import asyncio
import contextlib
import csv
import io
import logging
import os
import re
import sqlite3
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import httpx
from dotenv import load_dotenv
from fastapi import Depends, FastAPI, File, Header, HTTPException, Query, UploadFile
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

ROOT = Path(__file__).resolve().parent
load_dotenv(ROOT / '.env')
DATABASE = Path(os.getenv('DATABASE_PATH', str(ROOT / 'whales.sqlite3')))
ADMIN_TOKEN = os.getenv('ADMIN_TOKEN', '').strip()
if ADMIN_TOKEN in ('REPLACE_WITH_LONG_RANDOM_SECRET', 'changeme', 'password'):
    ADMIN_TOKEN = ''  # Fail closed until an administrator supplies a real secret.
ESPLORA_BASE = os.getenv('ESPLORA_BASE', 'https://blockstream.info/api').rstrip('/')
UPBIT_TICKER_URL = 'https://api.upbit.com/v1/ticker'
PRICE_POLL_SECONDS = max(5, int(os.getenv('PRICE_POLL_SECONDS', '8')))
WALLET_POLL_SECONDS = max(30, int(os.getenv('WALLET_POLL_SECONDS', '120')))
WALLET_REQUEST_GAP = max(0.3, float(os.getenv('WALLET_REQUEST_GAP', '0.7')))
PRICE_ALERT_PCT_5M = max(0.1, float(os.getenv('PRICE_ALERT_PCT_5M', '1.5')))
WEBHOOK_URL = os.getenv('ALERT_WEBHOOK_URL', '').strip()
TELEGRAM_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', '').strip()
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID', '').strip()
ADDRESS_RE = re.compile(r'^(?:bc1[ac-hj-np-z02-9]{11,87}|[13][a-km-zA-HJ-NP-Z1-9]{25,34})$', re.I)
log = logging.getLogger('checkbit.whales')
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

subscribers: set[asyncio.Queue] = set()
service_state = {'price_status': 'waiting', 'wallet_status': 'waiting', 'last_price_at': None,
                 'last_wallet_at': None, 'last_error': None, 'started_at': None}
client: httpx.AsyncClient | None = None
main_loop: asyncio.AbstractEventLoop | None = None


def timestamp():
    return datetime.now(timezone.utc).isoformat(timespec='seconds')


def connect():
    DATABASE.parent.mkdir(parents=True, exist_ok=True)
    c = sqlite3.connect(str(DATABASE), timeout=10)
    c.row_factory = sqlite3.Row
    c.execute('PRAGMA busy_timeout=10000')
    c.execute('PRAGMA journal_mode=WAL')
    return c


def init_db():
    with connect() as c:
        c.executescript('''
        CREATE TABLE IF NOT EXISTS wallets (
          address TEXT PRIMARY KEY, label TEXT NOT NULL DEFAULT '', balance_btc REAL,
          previous_balance_btc REAL, tier TEXT NOT NULL DEFAULT 'UNCLASSIFIED',
          last_checked TEXT, last_changed TEXT, last_txid TEXT,
          source TEXT NOT NULL DEFAULT 'esplora', enabled INTEGER NOT NULL DEFAULT 1,
          next_check_epoch REAL NOT NULL DEFAULT 0, error TEXT);
        CREATE TABLE IF NOT EXISTS alerts (
          id INTEGER PRIMARY KEY AUTOINCREMENT, address TEXT, label TEXT, tier TEXT,
          event_type TEXT NOT NULL, severity TEXT NOT NULL, direction TEXT,
          delta_btc REAL, balance_before REAL, balance_after REAL, price_krw REAL,
          price_change_pct REAL, txid TEXT, source TEXT NOT NULL, detected_at TEXT NOT NULL,
          acknowledged INTEGER NOT NULL DEFAULT 0, acknowledged_at TEXT);
        CREATE INDEX IF NOT EXISTS idx_alerts_detected ON alerts(id DESC);
        CREATE TABLE IF NOT EXISTS price_ticks (
          epoch INTEGER PRIMARY KEY, price_krw REAL NOT NULL, change_24h_pct REAL,
          recorded_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS movements (
          id INTEGER PRIMARY KEY AUTOINCREMENT, address TEXT NOT NULL,
          balance_before REAL, balance_after REAL, delta_btc REAL, txid TEXT,
          price_krw REAL, observed_at TEXT NOT NULL, source TEXT NOT NULL);
        CREATE INDEX IF NOT EXISTS idx_movements_address ON movements(address,id DESC);
        ''')


def readrows(sql, args=()):
    with connect() as c:
        return [dict(r) for r in c.execute(sql, args).fetchall()]


def readone(sql, args=()):
    with connect() as c:
        r = c.execute(sql, args).fetchone()
        return dict(r) if r else None


def tier_for(balance):
    if balance is None: return 'UNCLASSIFIED'
    if balance >= 10000: return 'TIER1'
    if balance >= 1000: return 'TIER2'
    return 'BELOW'


def publish(topic):
    def do_publish():
        for queue in list(subscribers):
            if not queue.full():
                queue.put_nowait(topic)
    if main_loop and main_loop.is_running():
        main_loop.call_soon_threadsafe(do_publish)


def latest_price():
    return readone('SELECT * FROM price_ticks ORDER BY epoch DESC LIMIT 1')


def insert_alert(**fields):
    fields.setdefault('detected_at', timestamp())
    with connect() as c:
        cols = ','.join(fields)
        q = ','.join('?' for _ in fields)
        cur = c.execute(f'INSERT INTO alerts ({cols}) VALUES ({q})', tuple(fields.values()))
        alert_id = cur.lastrowid
    publish('alert')
    if (WEBHOOK_URL or (TELEGRAM_TOKEN and TELEGRAM_CHAT_ID)) and main_loop and main_loop.is_running():
        asyncio.run_coroutine_threadsafe(deliver_notification(alert_id), main_loop)
    return alert_id


async def deliver_notification(alert_id):
    if not client: return
    row = readone('SELECT * FROM alerts WHERE id=?', (alert_id,))
    if not row: return
    direction = row['direction'] or row['event_type']
    msg = f"CheckBit {row['severity']} | {row['tier'] or 'BTC'} | {direction} | " \
          f"{abs(row['delta_btc'] or 0):,.4f} BTC | {row['address'] or 'BTC market'} | {row['detected_at']}"
    try:
        if WEBHOOK_URL:
            r = await client.post(WEBHOOK_URL, json={'text': msg, 'alert': row}, timeout=10)
            r.raise_for_status()
        if TELEGRAM_TOKEN and TELEGRAM_CHAT_ID:
            r = await client.post(f'https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage',
                                  json={'chat_id': TELEGRAM_CHAT_ID, 'text': msg}, timeout=10)
            r.raise_for_status()
    except Exception as exc:
        log.warning('External alert delivery failed: %s', exc)


def add_or_update_wallet(address: str, label: str = '', balance: Optional[float] = None,
                         source='esplora', from_snapshot=False):
    address = address.strip()
    if not ADDRESS_RE.fullmatch(address):
        raise ValueError('비트코인 메인넷 주소 형식이 아닙니다.')
    if balance is not None and (not (0 <= balance <= 21000000)):
        raise ValueError('잔액 범위가 잘못되었습니다.')
    with connect() as c:
        prev = c.execute('SELECT * FROM wallets WHERE address=?', (address,)).fetchone()
        if not prev:
            c.execute('''INSERT INTO wallets
            (address,label,balance_btc,previous_balance_btc,tier,source,next_check_epoch,last_checked)
            VALUES (?,?,?,?,?,?,?,?)''',
            (address, label[:100], balance, None, tier_for(balance), source,
             time.time() + WALLET_POLL_SECONDS if from_snapshot else 0,
             timestamp() if from_snapshot else None))
        elif not from_snapshot:
            c.execute('UPDATE wallets SET label=?,enabled=1,next_check_epoch=0 WHERE address=?',
                      (label[:100] or prev['label'], address))
    publish('wallet')
    return address


def record_balance(address: str, balance: float, source: str, txid: Optional[str] = None,
                   observation_id: Optional[str] = None):
    """Take a snapshot; first snapshot establishes baseline, future changes may trigger alerts.

    observation_id is stored as last_txid if provided; equal repeat snapshots are idempotent.
    A balance change is an observed *net change*, not verified sale/trade volume.
    """
    if balance < 0 or balance > 21000000: raise ValueError('Invalid balance')
    old = readone('SELECT * FROM wallets WHERE address=?', (address,))
    if old is None:
        add_or_update_wallet(address, balance=None, source=source)
        old = readone('SELECT * FROM wallets WHERE address=?', (address,))
    before = old['balance_btc']
    updated_at = timestamp()
    change = round(balance - before, 8) if before is not None else 0.0
    with connect() as c:
        c.execute('''UPDATE wallets SET balance_btc=?, previous_balance_btc=?,tier=?,
           last_checked=?,last_changed=CASE WHEN ? != 0 THEN ? ELSE last_changed END,
           last_txid=COALESCE(?,last_txid),source=?,error=NULL,next_check_epoch=?
           WHERE address=?''',
           (balance, before, tier_for(balance), updated_at, change, updated_at,
            txid, source, time.time() + WALLET_POLL_SECONDS, address))
        if before is not None and abs(change) >= 0.00000001:
            c.execute('''INSERT INTO movements (address,balance_before,balance_after,delta_btc,txid,price_krw,observed_at,source)
                VALUES(?,?,?,?,?,?,?,?)''',
                (address, before, balance, change, txid, (latest_price() or {}).get('price_krw'), updated_at, source))
    publish('wallet')
    if before is None or abs(change) < 0.00000001:
        return None
    # When crossing tier thresholds, use the PRE-CHANGE tier so leaving the tier still alerts.
    tier = tier_for(before)
    if tier not in ('TIER1', 'TIER2'):
        return None
    threshold = max(100 if tier == 'TIER1' else 30, before * (0.005 if tier == 'TIER1' else 0.01))
    if abs(change) < threshold:
        return None
    magnitude = abs(change)
    severe = magnitude >= threshold * 3 or (tier == 'TIER1' and magnitude >= 500)
    severity = 'CRITICAL' if severe else 'WARNING'
    price = latest_price()
    return insert_alert(address=address, label=old['label'], tier=tier,
                        event_type='BALANCE_MOVEMENT', severity=severity,
                        direction='INFLOW' if change > 0 else 'OUTFLOW', delta_btc=change,
                        balance_before=before, balance_after=balance,
                        price_krw=price['price_krw'] if price else None,
                        txid=txid, source=source)


def record_price(price, change_24h_pct, now_epoch=None):
    now_epoch = int(now_epoch or time.time())
    if price <= 0: return
    # 10s resolution: do not explode DB size with duplicate polling records.
    now_bucket = now_epoch // 10 * 10
    with connect() as c:
        c.execute('INSERT OR REPLACE INTO price_ticks(epoch,price_krw,change_24h_pct,recorded_at) VALUES(?,?,?,?)',
                  (now_bucket, price, change_24h_pct, timestamp()))
        c.execute('DELETE FROM price_ticks WHERE epoch < ?', (now_epoch - 7*24*3600,))
        base = c.execute('''SELECT price_krw FROM price_ticks
               WHERE epoch <= ? ORDER BY epoch DESC LIMIT 1''', (now_epoch - 300,)).fetchone()
        existing = c.execute('''SELECT 1 FROM alerts WHERE event_type='PRICE_SPIKE'
                AND detected_at >= ? LIMIT 1''',
                ((datetime.now(timezone.utc)-timedelta(minutes=10)).isoformat(timespec='seconds'),)).fetchone()
    publish('price')
    if base and base['price_krw'] > 0 and not existing:
        pct = (price / base['price_krw'] - 1) * 100
        if abs(pct) >= PRICE_ALERT_PCT_5M:
            insert_alert(event_type='PRICE_SPIKE', severity='WARNING',
                         direction='UP' if pct > 0 else 'DOWN', price_krw=price,
                         price_change_pct=round(pct, 3), source='upbit')


async def price_worker():
    while True:
        try:
            r = await client.get(UPBIT_TICKER_URL, params={'markets':'KRW-BTC'}, timeout=10)
            r.raise_for_status()
            data = r.json()[0]
            record_price(float(data['trade_price']), float(data['signed_change_rate'])*100)
            service_state['last_price_at'] = timestamp()
            service_state['price_status'] = 'online'
            service_state['last_error'] = None
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            service_state['price_status'] = 'error'
            service_state['last_error'] = f'Upbit: {str(exc)[:160]}'
            log.warning('Price fetch error: %s', exc)
        await asyncio.sleep(PRICE_POLL_SECONDS)


async def check_wallet(row):
    address = row['address']
    try:
        r = await client.get(f'{ESPLORA_BASE}/address/{address}', timeout=18)
        r.raise_for_status()
        stats = r.json()
        chain, mempool = stats['chain_stats'], stats['mempool_stats']
        # Includes pending UTXO delta. A reorg/confirmation can adjust this later.
        sats = sum(int(s.get('funded_txo_sum', 0)) - int(s.get('spent_txo_sum', 0))
                   for s in (chain, mempool))
        btc = sats / 100_000_000
        # Only look up a txid on a changed address; best-effort context, not proof of counterparty.
        txid = None
        if row['balance_btc'] is not None and abs(row['balance_btc'] - btc) > 0.00000001:
            with contextlib.suppress(Exception):
                t = await client.get(f'{ESPLORA_BASE}/address/{address}/txs', timeout=15)
                t.raise_for_status()
                if t.json(): txid = t.json()[0]['txid']
        record_balance(address, btc, 'esplora', txid)
        service_state['last_wallet_at'] = timestamp()
        service_state['wallet_status'] = 'online'
    except Exception as exc:
        with connect() as c:
            c.execute('UPDATE wallets SET error=?, last_checked=?,next_check_epoch=? WHERE address=?',
                      (str(exc)[:250], timestamp(), time.time() + WALLET_POLL_SECONDS*2, address))
        service_state['wallet_status'] = 'degraded'
        service_state['last_error'] = f'Esplora: {str(exc)[:160]}'
        log.warning('Wallet %s check error: %s', address[:12], exc)
        publish('wallet')


async def wallet_worker():
    while True:
        row = readone('''SELECT * FROM wallets WHERE enabled=1 AND source != 'collector' AND next_check_epoch <= ?
                       ORDER BY next_check_epoch ASC LIMIT 1''', (time.time(),))
        if row:
            await check_wallet(row)
            await asyncio.sleep(WALLET_REQUEST_GAP)
        else:
            await asyncio.sleep(2)


@asynccontextmanager
async def lifespan(_app):
    global client, main_loop
    main_loop = asyncio.get_running_loop()
    init_db()
    service_state['started_at'] = timestamp()
    client = httpx.AsyncClient(headers={'User-Agent':'CheckBit-WhaleAlert/1.0'})
    tasks = [asyncio.create_task(price_worker()), asyncio.create_task(wallet_worker())]
    yield
    for task in tasks: task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    await client.aclose()
    main_loop = None


app = FastAPI(title='CheckBit Whale Alert', version='1.0.0', lifespan=lifespan)
app.mount('/static', StaticFiles(directory=str(ROOT / 'static')), name='static')


def require_admin(x_api_key: Optional[str] = Header(default=None)):
    if not ADMIN_TOKEN or not x_api_key or x_api_key != ADMIN_TOKEN:
        raise HTTPException(401, '관리자 키가 필요합니다. 서버의 ADMIN_TOKEN 환경변수를 설정하세요.')


class WalletCreate(BaseModel):
    address: str
    label: str = Field(default='', max_length=100)


class IngestSnapshot(BaseModel):
    address: str
    balance_btc: float = Field(ge=0, le=21000000)
    label: str = Field(default='', max_length=100)
    txid: Optional[str] = None


@app.get('/')
@app.get('/whales')
async def home():
    return FileResponse(ROOT/'static'/'index.html')


@app.get('/api/whales/summary')
def summary():
    wallets = readrows('SELECT * FROM wallets WHERE enabled=1 ORDER BY balance_btc DESC')
    price = latest_price()
    total_btc = sum(w['balance_btc'] or 0 for w in wallets)
    return {'monitored_wallets':len(wallets),
            'tier1_count':sum(w['tier']=='TIER1' for w in wallets),
            'tier2_count':sum(w['tier']=='TIER2' for w in wallets),
            'unclassified_count':sum(w['balance_btc'] is None for w in wallets),
            'monitored_btc':total_btc,
            'monitored_krw':total_btc * price['price_krw'] if price else None,
            'alerts_24h':(readone('''SELECT COUNT(*) as n FROM alerts WHERE detected_at >= ?''',
                  ((datetime.now(timezone.utc)-timedelta(hours=24)).isoformat(timespec='seconds'),)) or {}).get('n',0),
            'unacknowledged':(readone('SELECT COUNT(*) as n FROM alerts WHERE acknowledged=0') or {}).get('n',0),
            'price':price, 'status':service_state,
            'source_note':'Blockstream 주소별 순잔액 변화 (매도 여부 확인 불가)'}


@app.get('/api/whales/wallets')
def wallets():
    return {'items':readrows('SELECT * FROM wallets WHERE enabled=1 ORDER BY balance_btc DESC, address')}


@app.get('/api/whales/alerts')
def alerts(limit:int=Query(100,ge=1,le=500), tier:Optional[str]=None,
           direction:Optional[str]=None, severity:Optional[str]=None):
    where, args = [], []
    if tier and tier!='ALL': where.append('tier=?'); args.append(tier)
    if direction and direction!='ALL': where.append('direction=?'); args.append(direction)
    if severity and severity!='ALL': where.append('severity=?'); args.append(severity)
    sql = 'SELECT * FROM alerts' + (' WHERE ' + ' AND '.join(where) if where else '') + ' ORDER BY id DESC LIMIT ?'
    return {'items':readrows(sql, (*args,limit))}


@app.get('/api/whales/price-history')
def price_history(hours:int=Query(6,ge=1,le=168)):
    since = int(time.time()-hours*3600)
    return {'items':readrows('SELECT epoch,price_krw,change_24h_pct FROM price_ticks WHERE epoch >= ? ORDER BY epoch', (since,))}


@app.get('/api/whales/{address}/history')
def wallet_history(address:str):
    row = readone('SELECT * FROM wallets WHERE address=?', (address,))
    if not row: raise HTTPException(404, '주소가 등록되지 않았습니다.')
    return {'wallet':row, 'movements':readrows('SELECT * FROM movements WHERE address=? ORDER BY id DESC LIMIT 200',(address,)),
            'alerts':readrows('SELECT * FROM alerts WHERE address=? ORDER BY id DESC LIMIT 50',(address,))}


@app.post('/api/whales/wallets', dependencies=[Depends(require_admin)])
def create_wallet(body:WalletCreate):
    try: address = add_or_update_wallet(body.address,body.label)
    except ValueError as e: raise HTTPException(422,str(e))
    return {'address':address,'ok':True}


@app.delete('/api/whales/wallets/{address}', dependencies=[Depends(require_admin)])
def delete_wallet(address:str):
    with connect() as c:
        result = c.execute('UPDATE wallets SET enabled=0 WHERE address=?',(address,))
        if not result.rowcount: raise HTTPException(404,'없음')
    publish('wallet')
    return {'ok':True}


@app.post('/api/whales/ingest', dependencies=[Depends(require_admin)])
def ingest(body:IngestSnapshot):
    if not ADDRESS_RE.fullmatch(body.address.strip()): raise HTTPException(422,'주소 형식 확인 필요')
    if not readone('SELECT address FROM wallets WHERE address=?',(body.address,)):
        add_or_update_wallet(body.address,body.label,source='collector')
    alert_id = record_balance(body.address,body.balance_btc,'collector',body.txid)
    return {'ok':True,'alert_id':alert_id,'first_snapshot_is_baseline':True}


@app.post('/api/whales/alerts/{alert_id}/ack', dependencies=[Depends(require_admin)])
def ack(alert_id:int):
    with connect() as c:
        result = c.execute('UPDATE alerts SET acknowledged=1, acknowledged_at=? WHERE id=?', (timestamp(),alert_id))
        if not result.rowcount: raise HTTPException(404,'알림 없음')
    publish('alert')
    return {'ok':True}


@app.post('/api/whales/import-csv', dependencies=[Depends(require_admin)])
async def import_csv(file: UploadFile = File(...)):
    raw = await file.read(5_000_001)
    if len(raw)>5_000_000: raise HTTPException(413,'웹 업로드는 5MB 이하. 대용량 CSV는 제공된 CLI 도구를 사용하세요.')
    try: stream = io.StringIO(raw.decode('utf-8-sig'))
    except UnicodeDecodeError: raise HTTPException(422,'UTF-8 CSV로 저장해서 다시 업로드하세요.')
    reader = csv.DictReader(stream)
    fields = {key.lower().strip():key for key in (reader.fieldnames or []) if key}
    column = next((fields[k] for k in ('wallet_address','address','btc_address') if k in fields),None)
    if not column: raise HTTPException(422,'address 또는 wallet_address 컬럼이 필요합니다.')
    label_col = next((fields[k] for k in ('label','name') if k in fields),None)
    balance_col = next((fields[k] for k in ('balance_btc','btc_balance') if k in fields),None)
    accepted=0; rejected=0
    for row in reader:
        try:
            addr=(row.get(column) or '').strip()
            balance = float(row[balance_col]) if balance_col and row.get(balance_col) else None
            add_or_update_wallet(addr,(row.get(label_col) or '') if label_col else '',
                                 balance=balance,source='csv',from_snapshot=balance is not None)
            accepted+=1
        except (ValueError,TypeError): rejected+=1
    return {'ok':True,'imported':accepted,'rejected':rejected,
            'note':'CSV는 초기 베이스라인에만 사용합니다. 이후 체인 조회로 검증됩니다.'}


@app.get('/api/whales/stream')
async def event_stream():
    async def event_generator():
        queue = asyncio.Queue(maxsize=50)
        subscribers.add(queue)
        try:
            yield 'event: connected\\ndata: {"ok":true}\\n\\n'.replace('\\n','\n')
            while True:
                try:
                    topic = await asyncio.wait_for(queue.get(),timeout=20)
                    yield f'event: refresh\ndata: {{"type":"{topic}"}}\n\n'
                except asyncio.TimeoutError:
                    yield ': keepalive\n\n'
        finally:
            subscribers.discard(queue)
    return StreamingResponse(event_generator(), media_type='text/event-stream',
                             headers={'Cache-Control':'no-cache, no-transform','X-Accel-Buffering':'no'})


@app.get('/health')
def health():
    return {'ok':True,'status':service_state}
