"""
예제 1: 기본 Producer
메시지를 Kafka 토픽에 전송
"""
import asyncio
import json
from aiokafka import AIOKafkaProducer


async def main():
    producer = AIOKafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8')
    )

    await producer.start()
    print("Producer 시작됨")

    try:
        # 단순 메시지 전송
        for i in range(5):
            message = {
                "id": i,
                "message": f"안녕하세요 메시지 {i}번",
                "type": "basic"
            }
            await producer.send_and_wait("test-topic", value=message)
            print(f"[전송] {message}")
            await asyncio.sleep(0.5)

    finally:
        await producer.stop()
        print("Producer 종료됨")


if __name__ == "__main__":
    asyncio.run(main())