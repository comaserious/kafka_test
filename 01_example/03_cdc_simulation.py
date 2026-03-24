"""
예제 3: DB 변경 이벤트 시뮬레이션 (CDC 개념 학습용)
실제 Debezium 없이 CDC 이벤트 구조를 파이썬으로 시뮬레이션
"""
import asyncio
import json
import random
import time
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer


# ──────────────────────────────────────────
# CDC 이벤트 생성 헬퍼
# ──────────────────────────────────────────

def make_cdc_event(op: str, table: str, before: dict | None, after: dict | None) -> dict:
    """Debezium 형식의 CDC 이벤트 생성"""
    return {
        "op": op,           # c=create, u=update, d=delete
        "table": table,
        "before": before,
        "after": after,
        "source": {
            "db": "mydb",
            "ts_ms": int(time.time() * 1000)
        }
    }


# ──────────────────────────────────────────
# Producer: DB 변경 이벤트 발행
# ──────────────────────────────────────────

async def db_event_producer():
    producer = AIOKafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8')
    )
    await producer.start()
    print("[Producer] DB 이벤트 시뮬레이터 시작\n")

    # 샘플 유저 DB 상태
    users = {
        1: {"id": 1, "name": "홍길동", "email": "hong@test.com"},
        2: {"id": 2, "name": "김철수", "email": "kim@test.com"},
    }

    operations = [
        # INSERT
        lambda: (
            "c",
            None,
            {"id": 3, "name": "이영희", "email": "lee@test.com"}
        ),
        # UPDATE
        lambda: (
            "u",
            users[1].copy(),
            {**users[1], "email": f"hong_{random.randint(1,99)}@new.com"}
        ),
        # DELETE
        lambda: (
            "d",
            users[2].copy(),
            None
        ),
    ]

    try:
        for op_fn in operations:
            op, before, after = op_fn()
            event = make_cdc_event(op, "users", before, after)

            topic = f"db.mydb.users"
            await producer.send_and_wait(topic, value=event)

            op_label = {"c": "INSERT", "u": "UPDATE", "d": "DELETE"}[op]
            print(f"[Producer] {op_label} 이벤트 발행")
            print(f"           before={before}")
            print(f"           after={after}\n")
            await asyncio.sleep(1)

    finally:
        await producer.stop()
        print("[Producer] 종료됨")


# ──────────────────────────────────────────
# Consumer: 이벤트 수신 및 처리
# ──────────────────────────────────────────

async def db_event_consumer():
    consumer = AIOKafkaConsumer(
        "db.mydb.users",
        bootstrap_servers='localhost:9092',
        group_id="cdc-processor",
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        auto_offset_reset='earliest'
    )
    await consumer.start()
    print("[Consumer] CDC 이벤트 리스너 시작\n")

    try:
        async for msg in consumer:
            event = msg.value
            op = event["op"]

            if op == "c":
                print(f"[Consumer] ✅ INSERT 감지 → 다른 DB에 동기화: {event['after']}")
            elif op == "u":
                print(f"[Consumer] 🔄 UPDATE 감지 → 캐시 무효화: id={event['after']['id']}")
            elif op == "d":
                print(f"[Consumer] ❌ DELETE 감지 → 관련 데이터 정리: {event['before']}")

            print()

    except asyncio.CancelledError:
        pass
    finally:
        await consumer.stop()
        print("[Consumer] 종료됨")


# ──────────────────────────────────────────
# Producer / Consumer 동시 실행
# ──────────────────────────────────────────

async def main():
    consumer_task = asyncio.create_task(db_event_consumer())

    # Consumer가 준비될 시간
    await asyncio.sleep(2)

    await db_event_producer()

    # 남은 메시지 처리 후 종료
    await asyncio.sleep(2)
    consumer_task.cancel()
    await asyncio.gather(consumer_task, return_exceptions=True)


if __name__ == "__main__":
    asyncio.run(main())