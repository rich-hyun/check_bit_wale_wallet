import importlib
import os
from pathlib import Path

import pytest

ADDRESS = '1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa'

@pytest.fixture()
def service(tmp_path, monkeypatch):
    monkeypatch.setenv('DATABASE_PATH', str(tmp_path/'test.sqlite3'))
    monkeypatch.setenv('ADMIN_TOKEN', 'local-test-secret')
    import app
    app = importlib.reload(app)
    app.init_db()
    return app


def test_first_snapshot_creates_no_alert(service):
    s=service
    s.add_or_update_wallet(ADDRESS,'테스트 지갑')
    assert s.record_balance(ADDRESS,15000,'collector') is None
    assert s.readrows('SELECT * FROM alerts') == []
    assert s.readone('SELECT balance_btc FROM wallets WHERE address=?', (ADDRESS,))['balance_btc'] == 15000


def test_large_outflow_and_no_duplicate(service):
    s=service
    s.add_or_update_wallet(ADDRESS,'고래')
    s.record_balance(ADDRESS,15000,'collector')
    id1=s.record_balance(ADDRESS,14800,'collector',txid='a'*64)
    assert id1 is not None
    rows=s.readrows('SELECT * FROM alerts')
    assert len(rows)==1 and rows[0]['direction']=='OUTFLOW'
    assert rows[0]['tier']=='TIER1' and rows[0]['delta_btc']==-200
    assert s.record_balance(ADDRESS,14800,'collector',txid='a'*64) is None
    assert len(s.readrows('SELECT * FROM alerts')) == 1


def test_small_move_not_alerted(service):
    s=service
    s.add_or_update_wallet(ADDRESS)
    s.record_balance(ADDRESS,1300,'collector')
    assert s.record_balance(ADDRESS,1290,'collector') is None
    assert len(s.readrows('SELECT * FROM movements')) == 1
    assert len(s.readrows('SELECT * FROM alerts')) == 0
    assert s.record_balance(ADDRESS,1250,'collector') is not None


def test_cross_tier_down_still_alerted(service):
    s=service
    s.add_or_update_wallet(ADDRESS)
    s.record_balance(ADDRESS,10020,'collector')
    assert s.record_balance(ADDRESS,9960,'collector') is None
    assert s.record_balance(ADDRESS,9700,'collector') is not None
    assert s.readone('SELECT tier FROM alerts')['tier'] == 'TIER2'


def test_wallet_address_rejection(service):
    with pytest.raises(ValueError):
        service.add_or_update_wallet('bc1not-a-real-address')


def test_price_change_alarm(service):
    s=service
    # Five minutes ago -> now within ten-minute runtime; base is available.
    import time
    epoch=int(time.time())
    s.record_price(100000000, 0.0, now_epoch=epoch-301)
    s.record_price(102100000, 2.1, now_epoch=epoch)
    al=s.readrows('SELECT * FROM alerts')
    assert len(al)==1 and al[0]['event_type']=='PRICE_SPIKE'
    s.record_price(102300000, 2.3, now_epoch=epoch+10)
    assert len(s.readrows('SELECT * FROM alerts'))==1
