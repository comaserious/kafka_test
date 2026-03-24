"""
예제 4: 멀티 토픽 Consumer
PostgreSQL / MariaDB / Redis 이벤트를 하나의 Consumer로 통합 처리
"""
import asyncio
import json
import time
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer


TOPICS = {
    "pg.public.orders":    "PostgreSQL",
    "maria.shop.products": "MariaDB",
    "redis.changes":       "Redis",
}


# ──────────────────────────────────────────
# 각 DB 이벤트 시뮬레이터
# ──────────────────────────────────────────

async def simulate_events(producer: AIOKafkaProducer):
    events = [
        ("pg.public.orders",    {"op": "c", "after": {"order_id": 101, "user_id": 1, "amount": 50000}}),
        ("maria.shop.products", {"op": "u", "after": {"product_id": 55, "stock": 99}}),
        ("redis.changes",       {"op": "set", "key": "session:user:1", "ttl": 3600}),
        ("pg.public.orders",    {"op": "u", "after": {"order_id": 101, "status": "paid"}}),
        ("maria.shop.products", {"op": "d", "before": {"product_id": 99, "name": "단종상품"}}),
    ]

    await asyncio.sleep(1)  # Consumer 준비 대기

    for topic, payload in events:
        payload["ts_ms"] = int(time.time() * 1000)
        await producer.send_and_wait(topic, value=payload)
        print(f"[발행] {topic} → {payload}\n")
        await asyncio.sleep(0.8)


# ──────────────────────────────────────────
# 통합 이벤트 핸들러
# ──────────────────────────────────────────

def handle_event(topic: str, event: dict):
    source = TOPICS.get(topic, "Unknown")

    if "pg.public.orders" in topic:
        op = event.get("op")
        if op == "c":
            print(f"  📦 [주문생성] order_id={event['after']['order_id']} "
                  f"amount={event['after']['amount']:,}원")
        elif op == "u":
            print(f"  🔄 [주문변경] order_id={event['after']['order_id']} "
                  f"status={event['after'].get('status', '-')}")

    elif "maria.shop.products" in topic:
        op = event.get("op")
        if op == "u":
            print(f"  📦 [재고변경] product_id={event['after']['product_id']} "
                  f"stock={event['after']['stock']}")
        elif op == "d":
            print(f"  ❌ [상품삭제] product_id={event['before']['product_id']} "
                  f"name={event['before']['name']}")

    elif "redis.changes" in topic:
        print(f"  🔑 [캐시변경] key={event['key']} ttl={event.get('ttl', '-')}s")

    print()


# ──────────────────────────────────────────
# 멀티 토픽 Consumer
# ──────────────────────────────────────────

async def multi_topic_consumer():
    consumer = AIOKafkaConsumer(
        *TOPICS.keys(),                               # 여러 토픽 동시 구독
        bootstrap_servers='localhost:9092',
        group_id="db-integrator",
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        auto_offset_reset='earliest'
    )
    await consumer.start()
    print(f"[Consumer] {len(TOPICS)}개 토픽 구독 시작")
    print(f"           {list(TOPICS.keys())}\n")

    try:
        async for msg in consumer:
            source = TOPICS.get(msg.topic, "Unknown")
            print(f"[수신] {source} | topic={msg.topic}")
            handle_event(msg.topic, msg.value)

    except asyncio.CancelledError:
        pass
    finally:
        await consumer.stop()


# ──────────────────────────────────────────
# 메인
# ──────────────────────────────────────────

async def main():
    producer = AIOKafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8')
    )
    await producer.start()

    consumer_task = asyncio.create_task(multi_topic_consumer())
    await simulate_events(producer)

    await asyncio.sleep(2)
    consumer_task.cancel()
    await asyncio.gather(consumer_task, return_exceptions=True)

    await producer.stop()
    print("완료")


if __name__ == "__main__":
    asyncio.run(main())