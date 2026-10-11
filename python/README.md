# Python 예제

[confluent-kafka](https://github.com/confluentinc/confluent-kafka-python) (librdkafka 기반)로 CLI 실습 내용을 코드로 확인한다.
호스트에서 `localhost:9092` 로 접속하므로 먼저 레포 루트에서 `docker compose up -d` 로 브로커를 띄워 둔다.

## 준비

[uv](https://docs.astral.sh/uv/) 를 사용한다.

```bash
cd python
uv sync
```

pip 를 쓴다면:

```bash
cd python
python3 -m venv .venv && source .venv/bin/activate
pip install confluent-kafka
python examples/01_admin_topics.py
```

브로커 주소를 바꾸려면 `BOOTSTRAP_SERVERS=host:9092 uv run ...` 처럼 환경변수를 준다.

## 예제

| 파일 | 내용 | 관련 CLI |
| --- | --- | --- |
| `01_admin_topics.py` | 토픽 목록·생성·조회, 파티션 증가/감소 실패, config 변경 | `kafka-topics.sh`, `kafka-configs.sh` |
| `02_producer.py` | key / JSON value / headers 전송, `acks`, 전송 결과(partition, offset) 확인 | `kafka-console-producer.sh` |
| `03_consumer.py` | `group.id`, `auto.offset.reset`, 자동/수동 커밋, 리밸런스 콜백 | `kafka-console-consumer.sh` |
| `04_key_partitioning.py` | 같은 key 는 같은 파티션으로, key 가 null 이면 분산 | - |
| `05_consumer_group.py` | 컨슈머 N개를 띄우고 하나씩 종료하면서 리밸런스 관찰 | `--group` |
| `06_consumer_lag.py` | 파티션별 CURRENT / LOG-END / LAG 계산 | `kafka-consumer-groups.sh --describe` |

## 실습 순서

```bash
cd python

# 1) 토픽 hello.python (파티션 3개) 생성 + 파티션 감소 실패 확인
uv run examples/01_admin_topics.py

# 2) 메시지 전송: alice/bob/carol 을 key 로 보내므로 key 별로 파티션이 고정된다
uv run examples/02_producer.py
uv run examples/02_producer.py --acks all -n 20
uv run examples/02_producer.py --no-key -n 5

# 3) 컨슈머 실행 (Ctrl+C 로 종료) - 다시 실행하면 커밋된 오프셋 이후부터 읽는다
uv run examples/03_consumer.py
uv run examples/03_consumer.py --group other-group     # 다른 그룹은 처음부터
uv run examples/03_consumer.py --group new-group --reset latest  # 새 그룹 + latest: 이후 메시지만

# 4) LAG 확인: 컨슈머를 끈 상태에서 producer 를 실행한 뒤 lag 을 보면 늘어나 있다
uv run examples/02_producer.py -n 5
uv run examples/06_consumer_lag.py --group my-group
docker exec -it kafka-lab /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group my-group --describe

# 5) key 와 파티션
uv run examples/04_key_partitioning.py

# 6) 컨슈머 그룹 리밸런스 (파티션 3개)
uv run examples/05_consumer_group.py --consumers 2
uv run examples/05_consumer_group.py --consumers 4   # 1개는 파티션을 못 받는다
```

### 리밸런스 직접 해보기

터미널 여러 개에서 같은 그룹으로 컨슈머를 띄우고, 하나씩 종료하면서 `[rebalance] assigned / revoked` 로그를 본다.

```bash
# 터미널 1, 2, 3
uv run examples/03_consumer.py --group rb-group --reset latest

# 터미널 4
uv run examples/02_producer.py -n 30
```

## 예시 출력

`02_producer.py`
```
[OK] key=alice    -> partition=1 offset=0
[OK] key=bob      -> partition=0 offset=0
[OK] key=carol    -> partition=0 offset=1
...
```

`04_key_partitioning.py` (key -> {partition: 건수})
```
(null)   -> {0: 8, 1: 9, 2: 13}
order-1  -> {1: 30}
order-2  -> {0: 30}
order-3  -> {0: 30}
```

`05_consumer_group.py --consumers 4`
```
== 컨슈머 4개 시작, 그룹 합류 대기 ==
  [consumer-0] assigned []
  [consumer-2] assigned [2]
  [consumer-3] assigned [0]
  [consumer-1] assigned [1]
...
== 컨슈머 1개 종료 (남은 컨슈머 2개) ==
  [consumer-1] assigned [0, 1]
  [consumer-0] assigned [2]
```

## 참고

- librdkafka 의 기본 파티셔너는 `consistent_random`(CRC32) 이라 Java 클라이언트와 key → 파티션 결과가 다르다.
  예제에서는 Java 와 맞추기 위해 `partitioner: murmur2_random` 을 지정했다.
- `05_consumer_group.py` 를 강제 종료(`kill -9`)하면 자식 프로세스가 남아 그룹에 계속 붙어 있을 수 있다.
  이러면 다음 실행에서 파티션 할당이 이상하게 보인다. `ps aux | grep multiprocessing` 으로 남은 프로세스를 정리하거나,
  `session.timeout.ms`(기본 45초)가 지나 그룹에서 빠질 때까지 기다린다.
