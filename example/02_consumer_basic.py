"""
예제 2: 기본 Consumer
Kafka 토픽에서 메시지를 수신
"""
import asyncio
import json
from aiokafka import AIOKafkaConsumer


async def main():
    consumer = AIOKafkaConsumer(
        "test-topic",
        bootstrap_servers='localhost:9092',
        group_id="my-group",                          # 같은 group_id끼리 파티션 분배
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        auto_offset_reset='earliest'                  # 처음부터 읽기
    )

    await consumer.start()
    print("Consumer 시작됨 - 메시지 대기중...")

    try:
        async for msg in consumer:
            print(f"[수신] partition={msg.partition} offset={msg.offset}")
            print(f"       value={msg.value}")
            print()

    except asyncio.CancelledError:
        pass
    finally:
        await consumer.stop()
        print("Consumer 종료됨")


if __name__ == "__main__":
    asyncio.run(main())