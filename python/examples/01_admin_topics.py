"""토픽 관리 (kafka-topics.sh / kafka-configs.sh 의 Python 버전).

    uv run examples/01_admin_topics.py
"""

from confluent_kafka import KafkaException
from confluent_kafka.admin import (
    AdminClient,
    AlterConfigOpType,
    ConfigEntry,
    ConfigResource,
    NewPartitions,
    NewTopic,
)

from common import BOOTSTRAP_SERVERS, DEFAULT_TOPIC

admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})


def list_topics():
    md = admin.list_topics(timeout=10)
    print("== 토픽 목록 ==")
    for name in sorted(md.topics):
        print(f"  {name} (partitions={len(md.topics[name].partitions)})")


def create_topic(name, partitions, replication_factor=1):
    futures = admin.create_topics(
        [
            NewTopic(
                name,
                num_partitions=partitions,
                replication_factor=replication_factor,
                config={"retention.ms": "172800000"},  # 2일
            )
        ]
    )
    try:
        futures[name].result()
        print(f"토픽 생성: {name} (partitions={partitions})")
    except KafkaException as e:
        print(f"토픽 생성 실패/이미 존재: {e.args[0].str()}")


def describe_topic(name):
    t = admin.list_topics(topic=name, timeout=10).topics[name]
    print(f"== {name} ==")
    for p in sorted(t.partitions.values(), key=lambda p: p.id):
        print(f"  Partition {p.id}: leader={p.leader} replicas={p.replicas} isr={p.isrs}")


def set_partitions(name, total):
    """파티션 개수 변경. 늘리는 것만 가능하고 줄이면 InvalidPartitionsException(INVALID_PARTITIONS)."""
    futures = admin.create_partitions([NewPartitions(name, total)])
    try:
        futures[name].result()
        print(f"파티션 변경 성공: {name} -> {total}")
    except KafkaException as e:
        print(f"파티션 변경 실패: {e.args[0].name()} - {e.args[0].str()}")


def alter_config(name, key, value):
    resource = ConfigResource(
        ConfigResource.Type.TOPIC,
        name,
        incremental_configs=[ConfigEntry(key, value, incremental_operation=AlterConfigOpType.SET)],
    )
    admin.incremental_alter_configs([resource])[resource].result()
    print(f"config 변경: {name} {key}={value}")

    describe = ConfigResource(ConfigResource.Type.TOPIC, name)
    configs = admin.describe_configs([describe])[describe].result()
    for k in (key, "retention.ms", "cleanup.policy"):
        print(f"  {k} = {configs[k].value}")


if __name__ == "__main__":
    list_topics()
    create_topic(DEFAULT_TOPIC, partitions=3)
    describe_topic(DEFAULT_TOPIC)
    set_partitions(DEFAULT_TOPIC, 2)  # 3 -> 2 : 실패해야 정상
    alter_config(DEFAULT_TOPIC, "min.insync.replicas", "1")
