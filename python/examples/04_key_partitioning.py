"""Key 에 따른 파티션 분배 확인.

같은 key 는 항상 같은 파티션으로, key=null 이면 여러 파티션으로 흩어진다.

    uv run examples/04_key_partitioning.py
"""

from collections import defaultdict

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from common import BOOTSTRAP_SERVERS

TOPIC = "hello.partitioning"

admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
if TOPIC not in admin.list_topics(timeout=10).topics:
    admin.create_topics([NewTopic(TOPIC, num_partitions=3, replication_factor=1)])[TOPIC].result()

result = defaultdict(lambda: defaultdict(int))  # key -> partition -> count


def record(err, msg):
    if err is None:
        key = msg.key().decode() if msg.key() else "(null)"
        result[key][msg.partition()] += 1


# Java 클라이언트와 동일한 해시(murmur2)를 쓰도록 지정
producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS, "partitioner": "murmur2_random"})

for i in range(30):
    for key in ("order-1", "order-2", "order-3"):
        producer.produce(TOPIC, key=key, value=f"{key}-{i}", on_delivery=record)
    producer.produce(TOPIC, key=None, value=f"no-key-{i}", on_delivery=record)
    # key=null 은 배치 단위로 파티션이 바뀌므로(sticky) 일부러 조금씩 flush
    producer.flush()

for key, partitions in sorted(result.items()):
    print(f"{key:<8} -> {dict(sorted(partitions.items()))}")
