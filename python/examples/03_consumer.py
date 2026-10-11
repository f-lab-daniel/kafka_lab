"""Consumer: group.id / auto.offset.reset / 커밋 방식에 따른 동작 확인.

    uv run examples/03_consumer.py                          # group=my-group, earliest
    uv run examples/03_consumer.py --group other-group      # 다른 그룹은 처음부터 다시 읽는다
    uv run examples/03_consumer.py --reset latest           # 새 그룹이면 지금 이후 메시지만
    uv run examples/03_consumer.py --manual-commit          # 처리 후 직접 커밋

Ctrl+C 로 종료.
"""

import argparse
import json

from confluent_kafka import Consumer, KafkaError

from common import BOOTSTRAP_SERVERS, DEFAULT_TOPIC

parser = argparse.ArgumentParser()
parser.add_argument("--topic", default=DEFAULT_TOPIC)
parser.add_argument("--group", default="my-group")
parser.add_argument("--reset", default="earliest", choices=["earliest", "latest"])
parser.add_argument("--manual-commit", action="store_true")
args = parser.parse_args()

consumer = Consumer(
    {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": args.group,
        # 그룹에 커밋된 오프셋이 없을 때만 적용된다
        "auto.offset.reset": args.reset,
        "enable.auto.commit": not args.manual_commit,
    }
)


def on_assign(c, partitions):
    print(f"[rebalance] assigned: {[p.partition for p in partitions]}")


def on_revoke(c, partitions):
    print(f"[rebalance] revoked : {[p.partition for p in partitions]}")


consumer.subscribe([args.topic], on_assign=on_assign, on_revoke=on_revoke)
print(f"consume topic={args.topic} group={args.group} reset={args.reset}")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                print(f"error: {msg.error()}")
            continue

        key = msg.key().decode() if msg.key() else None
        value = json.loads(msg.value())
        _, ts = msg.timestamp()  # (timestamp type, ms) - 기본은 CreateTime
        headers = dict(msg.headers() or [])
        print(
            f"p={msg.partition()} offset={msg.offset():<4} ts={ts} "
            f"key={key!s:<6} headers={headers} value={value}"
        )

        if args.manual_commit:
            consumer.commit(message=msg, asynchronous=False)
except KeyboardInterrupt:
    pass
finally:
    consumer.close()  # 그룹에서 정상 탈퇴 -> 즉시 리밸런스
