# kafka-lab

Apache Kafka 실습 레포. Docker 로 단일 브로커(KRaft 모드)를 띄우고 CLI 와 Python 으로 실습한다.

## 구성

```
.
├── docker-compose.yaml    # apache/kafka:4.3.1 단일 노드 (broker + controller)
├── data/                  # 브로커 log.dirs (/var/lib/kafka/data) 마운트
├── docs/
│   ├── 01-concepts.md     # Kafka 핵심 개념 정리
│   └── 02-cli-lab.md      # docker exec 로 하는 CLI 실습
└── python/                # confluent-kafka 예제
```

## 시작하기

```bash
docker compose up -d

# 동작 확인
docker exec -it kafka-lab /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 | head -1
docker exec -it kafka-lab /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

## 실습 순서

1. [Kafka 핵심 개념](docs/01-concepts.md)
2. [CLI 실습](docs/02-cli-lab.md): 토픽 / config / producer / consumer / consumer group / lag / 세그먼트
3. [Python 예제](python/README.md): admin / producer / consumer / key 파티셔닝 / 리밸런스 / lag

## 접속 정보

| 위치 | bootstrap server |
| --- | --- |
| 컨테이너 내부 (`docker exec`) | `localhost:9092` |
| 호스트 (Python 등) | `localhost:9092` |
