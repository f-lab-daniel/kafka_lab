"""Consumer Group 과 리밸런스 관찰.

컨슈머 N 개를 같은 그룹으로 띄운 뒤 하나씩 종료하면서 파티션 재할당을 확인한다.

    uv run examples/05_consumer_group.py --consumers 2   # 파티션 3개 / 컨슈머 2개
    uv run examples/05_consumer_group.py --consumers 4   # 컨슈머 1개는 놀게 된다
"""

import argparse
import multiprocessing as mp
import time

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from common import BOOTSTRAP_SERVERS

TOPIC = "hello.group"
GROUP = "group-demo"


def ensure_topic():
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    if TOPIC not in admin.list_topics(timeout=10).topics:
        admin.create_topics([NewTopic(TOPIC, num_partitions=3, replication_factor=1)])[TOPIC].result()


def run_consumer(name, stop):
    c = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": GROUP,
            "auto.offset.reset": "latest",
        }
    )
    c.subscribe(
        [TOPIC],
        on_assign=lambda _, ps: print(f"  [{name}] assigned {sorted(p.partition for p in ps)}", flush=True),
        on_revoke=lambda _, ps: print(f"  [{name}] revoked  {sorted(p.partition for p in ps)}", flush=True),
    )
    try:
        while not stop.is_set():
            msg = c.poll(0.5)
            if msg is not None and not msg.error():
                print(f"  [{name}] p={msg.partition()} {msg.value().decode()}", flush=True)
    except KeyboardInterrupt:
        pass
    finally:
        c.close()  # LeaveGroup 전송 -> 남은 컨슈머들이 즉시 리밸런스
        print(f"  [{name}] closed", flush=True)


def produce_round(producer, label):
    for i in range(6):
        producer.produce(TOPIC, key=f"k{i}", value=f"{label}-{i}")
    producer.flush()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--consumers", type=int, default=3)
    args = parser.parse_args()

    ensure_topic()
    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS, "partitioner": "murmur2_random"})

    workers = []
    for i in range(args.consumers):
        stop = mp.Event()
        p = mp.Process(target=run_consumer, args=(f"consumer-{i}", stop), daemon=True)
        p.start()
        workers.append((p, stop))

    print(f"== 컨슈머 {args.consumers}개 시작, 그룹 합류 대기 ==")
    time.sleep(8)
    produce_round(producer, "round1")
    time.sleep(3)

    # 컨슈머를 하나씩 내리면서 리밸런스 확인
    while len(workers) > 1:
        p, stop = workers.pop()
        print(f"\n== 컨슈머 1개 종료 (남은 컨슈머 {len(workers)}개) ==")
        stop.set()
        p.join()
        time.sleep(5)
        produce_round(producer, f"after-{len(workers)}")
        time.sleep(3)

    p, stop = workers.pop()
    stop.set()
    p.join()
    print("\n== 종료 ==")
