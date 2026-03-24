# Kafka 학습 예제 (Python + Docker)

## 실행 환경
- Windows + Docker Desktop
- Python 3.11+

---

## 시작하기

### 1. Kafka 실행
```bash
docker-compose up -d
```

컨테이너 상태 확인:
```bash
docker-compose ps
```

### 2. Python 패키지 설치
```bash
pip install -r requirements.txt
```

### 3. Kafka UI 확인
브라우저에서 http://localhost:8080 접속
→ 토픽 / 메시지 / 컨슈머 그룹 시각적으로 확인 가능

---

## 예제 실행 순서

### 예제 1+2: 기본 Producer / Consumer
터미널 두 개 열어서 동시에 실행:
```bash
# 터미널 1 - Consumer 먼저 실행
python 02_consumer_basic.py

# 터미널 2 - Producer 실행
python 01_producer_basic.py
```

### 예제 3: CDC 시뮬레이션
```bash
python 03_cdc_simulation.py
```
Producer/Consumer가 동시에 실행되며 INSERT/UPDATE/DELETE 이벤트 흐름 확인

### 예제 4: 멀티 토픽 (여러 DB 통합)
```bash
python 04_multi_topic.py
```
PostgreSQL / MariaDB / Redis 이벤트를 하나의 Consumer로 처리

---

## 학습 포인트

| 예제 | 개념 |
|------|------|
| 01+02 | Producer/Consumer 기본 구조, offset, group_id |
| 03 | CDC 이벤트 포맷 (op, before, after), Debezium 구조 이해 |
| 04 | 멀티 토픽 구독, 이벤트 라우팅, DB 통합 패턴 |

---

## 종료
```bash
docker-compose down
```

데이터도 삭제하려면:
```bash
docker-compose down -v
```