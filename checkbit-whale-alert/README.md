# CheckBit Whale Alert (실행 가능한 웹 페이지 + 알림 백엔드)

CheckBit의 `/whales` 메뉴에 붙이는 BTC 고래 지갑 주소 모니터링 앱입니다.

- **Tier 1**: 최초 관측 기준 잔액 10,000 BTC 이상 (이동 감지: max(100 BTC, 0.5%))
- **Tier 2**: 최초 관측 기준 잔액 1,000~9,999 BTC (이동 감지: max(30 BTC, 1%))
- **BTC 현재가**: Upbit KRW-BTC REST 시세를 기본 8초마다 갱신, 가격 그래프 / 보유 BTC 원화 평가액
- **주소별 잔액**: Blockstream Esplora를 기본 120초마다 **순차적으로** 조회 (외부 API 호출 과다 방지)
- **알림**: 주소 순잔액 변화 / BTC 5분 가격 급등락(초기 1.5% 기준) / SSE 업데이트 / 브라우저 알림 / 선택적 Telegram 알림
- **DB**: SQLite 내구 저장 (주소, 스냅샷, 이동 이력, 알림, BTC 가격)
- **운영**: 브라우저에서 지갑 추가, CSV 지갑 가져오기, 알림 확인 처리 / `X-API-Key` 관리자 보호
- **오프라인 HTML 미리보기**: `static/index.html`을 직접 더블클릭 → 명시적인 데모 데이터 화면

> **중요:** 온체인 특정 주소의 잔액 증감은 *실제 매수·매도*를 증명하지 않습니다. UTXO 거스름돈, 거래소 내부 이동, 동일 소유자의 다른 지갑 이동일 수 있습니다. 이 구현은 주소 기반이고 소유자(Entity) 기반 분석이나 전체 BTC 네트워크를 전수 스캔하지 않습니다. `/api/whales/ingest`로 수집기/인덱서 데이터를 받을 수 있습니다.

## 1. 윈도우에서 실행 (PowerShell)

```powershell
cd checkbit-whale-alert
py -3 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
Copy-Item .env.example .env
# .env 파일의 ADMIN_TOKEN을 충분히 긴 랜덤값으로 변경
python -m uvicorn app:app --host 127.0.0.1 --port 8010
```

브라우저에서 http://127.0.0.1:8010/whales 로 접속합니다. **관리자 설정** 버튼에서 `.env`에 입력한 `ADMIN_TOKEN`을 입력하고 지갑을 추가합니다. 최신 BTC 가격을 가져오려면 서버에서 Upbit 인터넷 연결이 가능해야 합니다. 데이터 없이 UI를 구경하려면 `데모 보기`를 누르세요.

## 2. Ubuntu / AWS Lightsail 서버에서 실행

```bash
cd /opt/checkbit-whale-alert
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
nano .env      # ADMIN_TOKEN 교체
uvicorn app:app --host 127.0.0.1 --port 8010
```

Nginx의 기존 CheckBit `server { ... }` 블록에 아래 **location**들을 추가합니다(기존 `/api/` 프록시와 충돌하지 않게 더 구체적인 위치 지정).

```nginx
location = /whales {
    proxy_pass http://127.0.0.1:8010/whales;
    proxy_set_header Host $host;
}
location /api/whales/ {
    proxy_pass http://127.0.0.1:8010;
    proxy_http_version 1.1;
    proxy_set_header Host $host;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_buffering off;
    proxy_read_timeout 3600;
}
```

배포한 뒤 `sudo nginx -t && sudo systemctl reload nginx`으로 적용합니다. 화면 HTML은 CSS/JavaScript를 자체 포함하므로 Nginx의 기존 `/static/` 경로를 변경할 필요가 없습니다. 위 명령은 **예제 구성**이므로 현재 Nginx 파일 백업 후 적용하세요. 운영에서는 uvicorn을 별도의 `systemd` 서비스에 등록하고 `Restart=always`, 로그 관리, 서비스 전용 계정, `.env` 600 권한을 설정하세요. 단일 인스턴스(uvicorn 워커 1개)로 동작하도록 설계되었습니다.

**기존 CheckBit 메뉴에 추가:**

```html
<a href="/whales">Whale Alert</a>
```

## 3. 지갑 데이터 연결하기

### A. 페이지에서 지갑 추가

관리자 키 설정 → `＋ 지갑 등록` → 실제 비트코인 메인넷 주소 입력. **첫 조회는 기준값 저장만** 하며, 그 이후 변동부터 감지합니다.

### B. CSV 업로드 (5MB 이하)

CSV 컬럼 예시:

```csv
wallet_address,label,balance_btc
1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa,지갑A,
```

웹 업로드는 주소 목록을 등록하기 위한 기능입니다. CSV에 잔액을 포함하면 최초 기준 잔액으로 저장됩니다. 86만 행 규모의 과거 데이터는 브라우저에 업로드하지 말고, 4번 서버의 수집기에서 **최신 주소별 스냅샷만** 추려서 `POST /api/whales/ingest`로 연결하세요. 실제 CSV 컬럼 확인 전에는 바로 변환이 불가능합니다.

### C. 기존 데이터 수집기의 스냅샷을 전송 (권장)

```bash
curl -X POST http://127.0.0.1:8010/api/whales/ingest \
  -H "Content-Type: application/json" \
  -H "X-API-Key: <.env에 설정한 관리자 키>" \
  -d '{"address":"1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa","balance_btc":15000,"label":"고래 지갑 A"}'
```

같은 주소에서 다음에 14800 BTC가 수신되면 이전 15000 BTC와의 차이 **-200 BTC**를 감지해 알림을 만듭니다. API 전송은 HTTPS/내부 망으로 제한하고 키를 소스코드나 클라이언트에 넣지 마세요.

> 주의: `ingest` 사용 시에도 기본 Esplora 폴링이 동작합니다. 동일 주소는 하나의 정규화된 출처만 지정하여 업데이트하는 것이 바람직하며, 대규모 수집기 연결 시 `collector` 전용 주소는 Esplora 폴링을 중단하도록 설정할 것을 권장합니다.

## 4. 페이지를 닫아도 알림 받기

`.env`에 아래 값을 설정하고 서버를 재시작합니다.

```dotenv
TELEGRAM_BOT_TOKEN=<Telegram BotFather에서 발급받은 Bot Token>
TELEGRAM_CHAT_ID=<수신할 채팅 ID>
```

알림이 생성되면 Telegram으로 전송합니다. 브라우저 알림은 페이지가 열려 있고 알림 권한이 허용된 경우에만 동작합니다.

## 5. API 목록

| Method | URL | 용도 |
|--|--|--|
| GET | `/api/whales/summary` | 감시 지갑/시세 현황 |
| GET | `/api/whales/wallets` | 지갑 목록 |
| GET | `/api/whales/alerts` | 알림 내역(필터/최대 500건) |
| GET | `/api/whales/price-history?hours=6` | 가격 이력 |
| GET | `/api/whales/stream` | 실시간 SSE 업데이트 |
| GET | `/api/whales/{address}/history` | 주소별 순잔액 이력 |
| POST | `/api/whales/wallets` | 관리자: 주소 추가 |
| POST | `/api/whales/import-csv` | 관리자: 주소 CSV 업로드 |
| POST | `/api/whales/ingest` | 관리자: 외부 수집기 잔액 스냅샷 |
| POST | `/api/whales/alerts/{id}/ack` | 관리자: 알림 확인 |

## 6. 범위와 주의점

1. **실시간 시세 / 준실시간 주소:** 시세는 8초 갱신, 주소는 기본 120초 재조회 **+ 주소당 간격**입니다. 관측의 실시간성은 네트워크/제공처/API 제한에 따라 달라집니다.
2. **가격 변동과 고래 이동 분리:** 고래 지갑의 BTC 평가액은 BTC 가격으로 계산하며, 가격 변화만으로 고래가 보유 BTC를 매도했다고 표시하지 않습니다.
3. **모니터링 범위:** 1K/10K 고래의 전체 목록을 자동 발굴하지는 않습니다. 등록된 주소만 대상으로 합니다.
4. **임계값:** 실험용 규칙이며 통계적으로 검증된 매매 신호가 아닙니다.
5. **규모:** 여러 만 주소 이상을 지속 감시하려면 공개 REST API보다 자체 Bitcoin 인덱서 또는 기존 데이터 수집 서버가 필요합니다.
6. **보안:** 관리자 키 유출 방지, 관리자 API 방화벽/추가 인증, TLS, 서버 권한/접근 로그 제한, API rate limit을 적용하세요. 앱 데이터는 공개 읽기용 API입니다.
7. **DB/배포:** SQLite 단일 프로세스 데모·소규모 운영 기준; 운영 규모가 커지면 PostgreSQL + 별도 수집 워커 + 큐로 분리해야 합니다.

API 참고: [Upbit Ticker](https://docs.upbit.com/kr/reference/websocket-ticker), [Blockstream Esplora](https://github.com/Blockstream/esplora/blob/master/API.md).
