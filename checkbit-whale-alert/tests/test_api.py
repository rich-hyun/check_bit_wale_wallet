import importlib

import pytest
from fastapi.testclient import TestClient

ADDRESS = '1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa'

@pytest.fixture()
def rest(tmp_path, monkeypatch):
    monkeypatch.setenv('DATABASE_PATH', str(tmp_path/'service.sqlite3'))
    monkeypatch.setenv('ADMIN_TOKEN','super-secret-test')
    import app
    app=importlib.reload(app)
    app.init_db()
    return TestClient(app.app)


def test_endpoints_and_auth(rest):
    assert rest.get('/whales').status_code==200
    assert rest.get('/api/whales/summary').status_code==200
    assert rest.get('/api/whales/wallets').json()['items']==[]
    assert rest.post('/api/whales/wallets',json={'address':ADDRESS}).status_code==401
    headers={'X-API-Key':'super-secret-test'}
    assert rest.post('/api/whales/wallets',json={'address':ADDRESS,'label':'test'},headers=headers).status_code==200
    assert rest.get('/api/whales/summary').json()['monitored_wallets']==1
    assert rest.post('/api/whales/ingest',json={'address':ADDRESS,'balance_btc':15000},headers=headers).json()['alert_id'] is None
    assert rest.post('/api/whales/ingest',json={'address':ADDRESS,'balance_btc':14800},headers=headers).json()['alert_id'] is not None
    alerts=rest.get('/api/whales/alerts').json()['items']
    assert len(alerts)==1 and alerts[0]['direction']=='OUTFLOW'
    assert rest.post(f"/api/whales/alerts/{alerts[0]['id']}/ack",headers=headers).status_code==200
    assert rest.get('/api/whales/alerts').json()['items'][0]['acknowledged']==1


def test_csv_upload(rest):
    headers={'X-API-Key':'super-secret-test'}
    csvdata=f'wallet_address,label,balance_btc\n{ADDRESS},loaded,1280\n'
    r=rest.post('/api/whales/import-csv',files={'file':('wallets.csv',csvdata,'text/csv')},headers=headers)
    assert r.status_code==200, r.text
    assert r.json()['imported']==1
    assert rest.get('/api/whales/summary').json()['tier2_count']==1
