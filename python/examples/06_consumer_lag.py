"""Consumer Lag 확인 (kafka-consumer-groups.sh --describe 의 Python 버전).

LAG = LOG-END-OFFSET - CURRENT-OFFSET(커밋된 오프셋)

    uv run examples/06_consumer_lag.py --group my-group
"""

import argparse

from confluent_kafka import Consumer, TopicPartition

from common import BOOTSTRAP_SERVERS, DEFAULT_TOPIC

parser = argparse.ArgumentParser()
parser.add_argument("--topic", default=DEFAULT_TOPIC)
parser.add_argument("--group", default="my-group")
args = parser.parse_args()

# 조회만 하고 그룹에 subscribe 하지 않으므로 리밸런스를 일으키지 않는다
consumer = Consumer({"bootstrap.servers": BOOTSTRAP_SERVERS, "group.id": args.group})

partitions = consumer.list_topics(args.topic, timeout=10).topics[args.topic].partitions
tps = [TopicPartition(args.topic, p) for p in sorted(partitions)]
committed = consumer.committed(tps, timeout=10)

print(f"{'PARTITION':<10}{'CURRENT':>10}{'LOG-END':>10}{'LAG':>8}")
total = 0
for tp in committed:
    low, high = consumer.get_watermark_offsets(tp, timeout=10)  # (가장 오래된, 가장 최신+1)
    current = tp.offset if tp.offset >= 0 else None  # 커밋 이력이 없으면 음수
    lag = high - (current if current is not None else low)
    total += lag
    print(f"{tp.partition:<10}{current if current is not None else '-':>10}{high:>10}{lag:>8}")
print(f"total lag = {total}")

consumer.close()
