# TB AI Data Aggregation API

MariaDB 테이블들의 데이터를 조합하여 AI 테이블에 적재하는 용도 API입니다.

## 기능

3개의 독립적인 데이터 집계 서비스를 제공합니다:

1. **Solar Power** - 태양광 발전 데이터
2. **ESS Charge** - ESS 충전량 데이터
3. **Power Usage** - 전력 사용량 데이터 (미정)

각 서비스는 별도의 엔드포인트로 관리되며, **지정된 날짜(YYYY-MM-DD) 하루분의 데이터**를 ymdhms 기준으로 조인하여 UPSERT 방식으로 적재합니다.

---

## 데이터 매핑

### 1. Solar Power (tb_ai_solar_power)

#### 소스 테이블
- **tb_solar_day**: ymdhms, forcast_quantity, today_generation, accum_generation
- **tb_weather_info**: ymdhms, tmn, tmx, ics

#### 컬럼 매핑
| 컬럼명 | 설명 | 소스 |
|--------|------|------|
| ymdhms | 날짜시간 | tb_solar_day.ymdhms |
| tmn | 최저기온 | tb_weather_info.tmn |
| tmx | 최고기온 | tb_weather_info.tmx |
| ics | 일조량 | tb_weather_info.ics |
| pre_pwr_generation | 예측 발전량 | tb_solar_day.forcast_quantity |
| today_generation | 당일 발전량 | tb_solar_day.today_generation |
| accum_generation | 누적 발전량 | tb_solar_day.accum_generation |
| reg_dt | 등록일자 | NOW() |

### 2. ESS Charge (tb_ai_ess_charge_amt)

#### 소스 테이블
- **tb_solar_day**: ymdhms, forcast_quantity, today_generation
- **tb_nrt_bms_daily_stat**: ymdhms, forecast_quantity, CHARGE_AMOUNT

#### 컬럼 매핑
| 컬럼명 | 설명 | 소스 |
|--------|------|------|
| ymdhms | 날짜시간 | tb_solar_day.ymdhms |
| pre_pwr_generation | 예측 발전량 | tb_solar_day.forcast_quantity |
| today_generation | 당일 발전량 | tb_solar_day.today_generation |
| pre_charge | 예측 충전량 | tb_nrt_bms_daily_stat.forecast_quantity |
| charge_amount | 충전량 | tb_nrt_bms_daily_stat.CHARGE_AMOUNT |
| reg_dt | 등록일자 | NOW() |

### 3. Power Usage (tb_ai_pwr_usage) - 미정

데이터 매핑이 아직 확정되지 않았습니다.

---

## 설치 및 실행

### 1. 의존성 설치

```bash
cd "F:\2.프로젝트\[BMT] 수요 맞춤형AI\project\tb_ai_table_api"
pip install -r requirements.txt
```

### 2. 데이터베이스 설정

[app/core/config.py](app/core/config.py) 파일에서 데이터베이스 접속 정보를 수정하세요:

```python
database_config: Dict[str, Any] = {
    'host': 'localhost',
    'user': 'root',
    'password': '9758',
    'database': 'solar_mokup',
    'charset': 'utf8mb4'
}
```

### 3. API 실행

```bash
python run.py
```

또는

```bash
python -m uvicorn app.main:app --host 0.0.0.0 --port 8001 --reload
```

### 4. API 문서 확인

브라우저에서 다음 URL로 접속:
- **Swagger UI**: http://localhost:8001/docs
- **ReDoc**: http://localhost:8001/redoc
- **루트**: http://localhost:8001/

---

## API 엔드포인트

### 기본 엔드포인트

#### GET `/`
루트 엔드포인트 - API 정보 및 사용 가능한 엔드포인트 목록

#### GET `/health`
헬스 체크 - API 서버 상태 확인

---

### Solar Power 엔드포인트

#### POST `/api/v1/solar-power/aggregate`
태양광 발전 데이터 집계 및 적재 (지정된 날짜 하루분)

**Request Body:**
```json
{
  "target_date": "2024-01-15"  // 필수 (YYYY-MM-DD)
}
```

**Response:**
```json
{
  "success": true,
  "inserted_count": 24,
  "target_date": "2024-01-15",
  "message": "2024-01-15 날짜의 24건의 Solar Power 데이터를 적재했습니다."
}
```

#### GET `/api/v1/solar-power/verify?limit=10`
적재된 Solar Power 데이터 확인

**Response:**
```json
[
  {
    "ymdhms": "2024-01-15 14:00:00",
    "tmn": 5.2,
    "tmx": 15.8,
    "ics": 8.5,
    "pre_pwr_generation": 250.5,
    "today_generation": 1500.0,
    "accum_generation": 50000.0,
    "reg_dt": "2024-01-15 15:30:00"
  }
]
```

---

### ESS Charge 엔드포인트

#### POST `/api/v1/ess-charge/aggregate`
ESS 충전량 데이터 집계 및 적재 (지정된 날짜 하루분)

**Request Body:**
```json
{
  "target_date": "2024-01-15"  // 필수 (YYYY-MM-DD)
}
```

**Response:**
```json
{
  "success": true,
  "inserted_count": 24,
  "target_date": "2024-01-15",
  "message": "2024-01-15 날짜의 24건의 ESS Charge 데이터를 적재했습니다."
}
```

#### GET `/api/v1/ess-charge/verify?limit=10`
적재된 ESS Charge 데이터 확인

**Response:**
```json
[
  {
    "ymdhms": "2024-01-15 14:00:00",
    "pre_pwr_generation": 250.5,
    "today_generation": 1500.0,
    "pre_charge": 180.0,
    "charge_amount": 175.5,
    "reg_dt": "2024-01-15 15:30:00"
  }
]
```

---

### Power Usage 엔드포인트 (미정)

#### POST `/api/v1/power-usage/aggregate`
전력 사용량 데이터 집계 및 적재 (미정)

**주의**: 데이터 매핑이 아직 확정되지 않았습니다.

#### GET `/api/v1/power-usage/verify?limit=10`
적재된 Power Usage 데이터 확인 (미정)

---

## 사용 예시

### cURL

```bash
# Solar Power 데이터 집계 (2024-01-15 날짜 하루분)
curl -X POST "http://localhost:8001/api/v1/solar-power/aggregate" \
  -H "Content-Type: application/json" \
  -d '{"target_date": "2024-01-15"}'

# Solar Power 데이터 확인
curl -X GET "http://localhost:8001/api/v1/solar-power/verify?limit=5"

# ESS Charge 데이터 집계 (2024-01-15 날짜 하루분)
curl -X POST "http://localhost:8001/api/v1/ess-charge/aggregate" \
  -H "Content-Type: application/json" \
  -d '{"target_date": "2024-01-15"}'

# ESS Charge 데이터 확인
curl -X GET "http://localhost:8001/api/v1/ess-charge/verify?limit=5"
```

### Python

```python
import requests

# Solar Power 데이터 집계 (2024-01-15 날짜 하루분)
response = requests.post(
    "http://localhost:8001/api/v1/solar-power/aggregate",
    json={"target_date": "2024-01-15"}
)
print(response.json())

# Solar Power 결과 확인
response = requests.get("http://localhost:8001/api/v1/solar-power/verify?limit=10")
print(response.json())

# ESS Charge 데이터 집계 (2024-01-15 날짜 하루분)
response = requests.post(
    "http://localhost:8001/api/v1/ess-charge/aggregate",
    json={"target_date": "2024-01-15"}
)
print(response.json())
```

### 테스트 스크립트

```bash
python test_api.py
```

---

## 프로젝트 구조

```
tb_ai_table_api/
├── app/
│   ├── __init__.py
│   ├── main.py                           # FastAPI 애플리케이션
│   ├── api/                              # API 엔드포인트
│   │   ├── __init__.py
│   │   ├── solar_power_endpoints.py     # Solar Power API
│   │   ├── ess_charge_endpoints.py      # ESS Charge API
│   │   └── power_usage_endpoints.py     # Power Usage API (미정)
│   ├── core/                             # 핵심 모듈
│   │   ├── __init__.py
│   │   ├── config.py                    # 설정
│   │   └── database.py                  # 데이터베이스 연결
│   ├── services/                         # 비즈니스 로직
│   │   ├── __init__.py
│   │   ├── solar_power_service.py       # Solar Power 서비스
│   │   ├── ess_charge_service.py        # ESS Charge 서비스
│   │   └── power_usage_service.py       # Power Usage 서비스 (미정)
│   └── models/                           # 데이터 모델
│       ├── __init__.py
│       └── schemas.py                   # Pydantic 스키마
├── requirements.txt                      # 의존성 패키지
├── run.py                                # 실행 스크립트
├── test_api.py                           # 테스트 스크립트
└── README.md                             # 문서
```

---

## 주의사항

1. **하루 단위 처리**: 지정된 날짜(YYYY-MM-DD)의 00:00:00 ~ 23:59:59 데이터만 처리합니다.
2. **UPSERT 방식**: 동일한 ymdhms가 이미 존재하면 데이터를 업데이트합니다.
3. **INNER JOIN**: 각 소스 테이블의 ymdhms가 정확히 일치하는 데이터만 적재됩니다.
4. **reg_dt**: 적재 시점의 현재 시간으로 자동 설정됩니다.
5. **독립적 서비스**: 각 데이터 타입(Solar Power, ESS Charge, Power Usage)은 독립적으로 관리됩니다.

---

## 문제 해결

### 데이터베이스 연결 실패
- 데이터베이스 접속 정보를 확인하세요
- MariaDB 서버가 실행 중인지 확인하세요
- 방화벽 설정을 확인하세요

### 데이터가 적재되지 않음
- 소스 테이블에 ymdhms가 일치하는 데이터가 있는지 확인하세요
- 테이블명과 컬럼명이 올바른지 확인하세요
- 로그를 확인하여 오류 메시지를 확인하세요

### Power Usage API 사용 불가
- Power Usage 데이터 매핑이 아직 확정되지 않았습니다
- 데이터 매핑 확정 후 [app/services/power_usage_service.py](app/services/power_usage_service.py)를 업데이트하세요

---

## 라이선스

MIT License
