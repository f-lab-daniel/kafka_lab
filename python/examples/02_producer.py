"""Producer: key / value(JSON) / headers 를 담아 전송하고 저장된 partition, offset 확인.

    uv run examples/02_producer.py                 # 기본 acks=1
    uv run examples/02_producer.py --acks all -n 20
    uv run examples/02_producer.py --no-key        # key=null -> 여러 파티션에 분산
"""

import argparse
import json
import time

from confluent_kafka import Producer

from common import BOOTSTRAP_SERVERS, DEFAULT_TOPIC, delivery_report

parser = argparse.ArgumentParser()
parser.add_argument("--topic", default=DEFAULT_TOPIC)
parser.add_argument("-n", "--count", type=int, default=10)
parser.add_argument("--acks", default="1", choices=["0", "1", "-1", "all"])
parser.add_argument("--no-key", action="store_true", help="key 없이(null) 전송")
args = parser.parse_args()

producer = Producer(
    {
        # 실제 운영에서는 브로커 장애에 대비해 2개 이상 적는다: "b0:9092,b1:9092"
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        # 0: 확인 안 함 / 1: 리더 저장 확인 / all(-1): 리더 + ISR 팔로워 모두 확인
        "acks": args.acks,
        # Java 클라이언트와 같은 key 해시(murmur2)를 사용 (librdkafka 기본값은 crc32)
        "partitioner": "murmur2_random",
    }
)

users = ["alice", "bob", "carol"]
for i in range(args.count):
    user = users[i % len(users)]
    value = {"seq": i, "user": user, "event": "click", "ts": time.time()}
    producer.produce(
        topic=args.topic,
        key=None if args.no_key else user,  # 같은 key -> 항상 같은 파티션 (순서 보장)
        value=json.dumps(value).encode(),  # value 는 보통 JSON 으로 직렬화
        headers={"schema-version": b"1"},  # 스키마 버전 등 부가 정보
        on_delivery=delivery_report,
    )
    producer.poll(0)  # 콜백 처리

# 버퍼에 남은 메시지를 모두 전송할 때까지 대기
remaining = producer.flush(10)
print(f"done (acks={args.acks}, 미전송={remaining})")
