# 02. Kafka CLI 실습

모든 명령은 실행 중인 `kafka-lab` 컨테이너 안의 Kafka 스크립트를 호출한다.

```bash
docker exec -it kafka-lab /opt/kafka/bin/<스크립트>.sh [옵션]
```

- `--bootstrap-server localhost:9092` 의 `localhost` 는 **컨테이너 내부** 기준이다.
- 강의 자료의 `./bin/xxx.sh` 는 모두 `docker exec -it kafka-lab /opt/kafka/bin/xxx.sh` 로 바꿔서 실행한다.
- 출력만 보는 명령은 `-it` 없이 실행해도 되지만, 키보드 입력이 필요한 producer 나 Ctrl+C 로 끝내는 consumer 는 `-it` 가 꼭 있어야 한다.

> 개념 정리는 [01-concepts.md](01-concepts.md) 참고.

---

## 0. 브로커 실행 / 상태 확인

```bash
# 레포 루트에서
docker compose up -d
docker compose ps
docker logs -f kafka-lab          # Ctrl+C 로 로그 보기 종료
```

강의 자료의 `kafka-server-start.sh config/server.properties` 는 컨테이너가 시작될 때 자동으로 실행된다.
JDK 와 ZooKeeper 를 따로 설치할 필요도 없다 (Kafka 4.x 는 ZooKeeper 없이 KRaft 모드로 동작).

```bash
# 실행 여부 확인: 브로커가 지원하는 API 버전이 출력되면 정상
docker exec -it kafka-lab /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 | head -1

# KRaft 컨트롤러 쿼럼 상태 (ZooKeeper 대신)
docker exec -it kafka-lab /opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server localhost:9092 describe --status
```

### 브로커 설정 (server.properties)

`docker-compose.yaml` 의 `KAFKA_*` 환경변수가 컨테이너 시작 시 `server.properties` 로 변환된다.
(`KAFKA_LOG_DIRS` → `log.dirs`, `KAFKA_NUM_PARTITIONS` → `num.partitions` …)

```bash
docker exec -it kafka-lab cat /opt/kafka/config/server.properties
docker exec -it kafka-lab grep -E "log.dirs|num.partitions|log.retention|log.segment" /opt/kafka/config/server.properties
```

| 설정 | 의미 | 이 레포의 값 |
| --- | --- | --- |
| `log.dirs` | 데이터 저장 경로 | `/var/lib/kafka/data` (= 호스트 `./data`) |
| `num.partitions` | 토픽 생성 시 기본 파티션 수 | `1` |
| `log.retention.hours` | 데이터 보존 기간 | `168` (7일) |
| `log.segment.bytes` | 세그먼트 파일 최대 크기 | `1073741824` (1GB) |
| `log.retention.check.interval.ms` | 삭제 대상 체크 주기 | `300000` (5분) |

---

## 1. 토픽

### 토픽 목록 / 생성 / 정보 확인

```bash
# 토픽 목록
docker exec -it kafka-lab /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

# 토픽 생성 (파티션 수, 복제 수 등을 지정하지 않으면 브로커 기본값 사용)
docker exec -it kafka-lab /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic hello.kafka

# 토픽 정보 확인
docker exec -it kafka-lab /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic hello.kafka
```

> 토픽 이름에 `.` 이 있으면 `WARNING: ... topics with a period ('.') or underscore ('_') could collide` 경고가 나온다.
> 메트릭 이름에서 `.` 과 `_` 가 같게 취급되기 때문으로, 둘 중 하나만 쓰면 무시해도 된다.

`--describe` 출력 읽는 법:

```
Topic: hello.kafka  PartitionCount: 1  ReplicationFactor: 1  Configs: ...
    Topic: hello.kafka  Partition: 0  Leader: 1  Replicas: 1  Isr: 1
```

- `Leader`: 리더 파티션이 있는 브로커 ID
- `Replicas`: 레플리카가 있는 브로커 목록
- `Isr`: 리더와 동기화된 레플리카 목록

### 옵션을 지정해서 생성

```bash
# 파티션 2개, 복제 1개, 보존기간 2일(172800000ms)
docker exec -it kafka-lab /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic hello.kafka2 --partitions 2 --replication-factor 1 --config retention.ms=172800000

docker exec -it kafka-lab /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
docker exec -it kafka-lab /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic hello.kafka2
```

> 브로커가 1대뿐이라 `--replication-factor` 는 1 까지만 가능하다. 2 이상을 주면 `InvalidReplicationFactorException` 이 발생한다.

### 파티션 개수 변경 (ALTER)

```bash
# 1 -> 2 로 늘리기
docker exec -it kafka-lab /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --topic hello.kafka --alter --partitions 2
docker exec -it kafka-lab /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic hello.kafka

# 2 -> 1 로 줄이기: 실패해야 정상
docker exec -it kafka-lab /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --topic hello.kafka --alter --partitions 1
```

줄이려고 하면 다음 에러가 발생한다. **파티션은 늘리는 것만 가능**하다.

```
ERROR org.apache.kafka.common.errors.InvalidPartitionsException: The topic hello.kafka currently has 2 partition(s); 1 would not be an increase.
```

---

## 2. Config 변경

```bash
# 토픽 설정 추가/변경
docker exec -it kafka-lab /opt/kafka/bin/kafka-configs.sh --bootstrap-server localhost:9092 \
  --alter --add-config min.insync.replicas=1 --topic hello.kafka

# 확인 (Configs: 항목에 min.insync.replicas=1 이 보인다)
docker exec -it kafka-lab /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic hello.kafka

# 토픽에 직접 설정된(dynamic) config 만 보기
docker exec -it kafka-lab /opt/kafka/bin/kafka-configs.sh --bootstrap-server localhost:9092 --describe --topic hello.kafka

# 설정 삭제 (브로커 기본값으로 돌아감)
docker exec -it kafka-lab /opt/kafka/bin/kafka-configs.sh --bootstrap-server localhost:9092 \
  --alter --delete-config min.insync.replicas --topic hello.kafka
```

---

## 3. Producer

```bash
docker exec -it kafka-lab /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic hello.kafka
```

`>` 프롬프트가 나오면 한 줄에 메시지 하나씩 입력한다. 종료는 `Ctrl+C`.

```
>hello
>kafka
>world
```

### key 와 함께 보내기

```bash
docker exec -it kafka-lab /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic hello.kafka \
  --reader-property parse.key=true --reader-property key.separator=:
```

```
>k1:first
>k2:second
>k1:third
```

> 같은 key(`k1`)는 항상 같은 파티션으로 들어간다. 아래 consumer 실습에서 `print.partition` 으로 확인해 보자.
> 강의 자료나 블로그에 나오는 `--property` 옵션은 Kafka 4.x 에서 deprecated 되었고, 대신 `--reader-property` 를 쓴다.

### acks 지정

```bash
# acks=0 / 1 / all(-1, 기본값)
docker exec -it kafka-lab /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic hello.kafka \
  --request-required-acks 1
```

---

## 4. Consumer

```bash
# 처음부터 모두 읽기 (Ctrl+C 로 종료)
docker exec -it kafka-lab /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic hello.kafka --from-beginning

# 특정 파티션만
docker exec -it kafka-lab /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic hello.kafka --from-beginning --partition 0
```

### key / partition / offset / timestamp 같이 출력

```bash
docker exec -it kafka-lab /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic hello.kafka --from-beginning \
  --formatter-property print.key=true \
  --formatter-property print.partition=true \
  --formatter-property print.offset=true \
  --formatter-property print.timestamp=true
```

```
CreateTime:1791694196804  Partition:1  Offset:0  k1  first
CreateTime:1791694196810  Partition:0  Offset:0  k2  second
CreateTime:1791694196815  Partition:1  Offset:1  k1  third
```

### Consumer Group

- `--group` 으로 목적이 같은 컨슈머들을 하나로 묶는다.
- 그룹이 토픽을 어디까지 읽었는지(오프셋)는 브로커가 관리한다.
- 지정한 이름의 그룹이 없으면 새로 만든다.

```bash
docker exec -it kafka-lab /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic hello.kafka --group my-group --from-beginning
```

**실습: 오프셋이 그룹 단위로 관리되는 것 확인하기**
1. 위 명령으로 메시지를 다 읽은 뒤 `Ctrl+C` 로 종료한다.
2. 같은 명령을 다시 실행하면 `--from-beginning` 을 줬는데도 **아무것도 출력되지 않는다.** 커밋된 오프셋 이후부터 읽기 때문이다.
   (`--from-beginning` 은 `auto.offset.reset=earliest` 와 같아서, 커밋된 오프셋이 **없을 때만** 적용된다.)
3. `--group my-group2` 처럼 다른 그룹으로 실행하면 처음부터 다시 읽는다.

**실습: 리밸런스 확인하기** (`hello.kafka` 가 파티션 2개인 상태에서)
1. 터미널 2개에서 같은 `--group my-group` 컨슈머를 각각 실행한다.
2. 터미널 3에서 producer 로 key 를 바꿔 가며 메시지를 보낸다. 두 컨슈머가 나눠서 받는 것을 확인한다.
3. 컨슈머 하나를 종료하면 남은 컨슈머가 모든 파티션을 받는다.
4. 컨슈머를 3개 띄우면 1개는 아무것도 받지 못한다 (파티션 2개 < 컨슈머 3개).

---

## 5. Consumer Groups 조회 / Lag

```bash
# 그룹 목록
docker exec -it kafka-lab /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list

# 그룹 상세: 파티션별 오프셋과 LAG
docker exec -it kafka-lab /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group my-group --describe

# 그룹 멤버(컨슈머)별 할당된 파티션
docker exec -it kafka-lab /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group my-group --describe --members --verbose
```

```
GROUP     TOPIC        PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG  CONSUMER-ID  HOST  CLIENT-ID
my-group  hello.kafka  0          3               5               2    -            -     -
my-group  hello.kafka  1          4               4               0    -            -     -
```

| 컬럼 | 의미 |
| --- | --- |
| `CURRENT-OFFSET` | 그룹이 커밋한 오프셋 (다음에 읽을 위치) |
| `LOG-END-OFFSET` | 파티션의 마지막 레코드 오프셋 + 1 |
| `LAG` | `LOG-END-OFFSET - CURRENT-OFFSET` = 아직 처리하지 못한 레코드 수 |

**실습: LAG 만들기**
1. 컨슈머가 없는 상태에서 producer 로 메시지를 몇 개 더 보낸다.
2. `--describe` 를 다시 보면 LAG 이 늘어나 있다.
3. 같은 그룹으로 컨슈머를 실행하면 밀린 메시지를 읽고 LAG 이 0 이 된다.

> LAG 이 계속 늘어난다면 컨슈머의 처리량이 부족한 것이다. 파티션과 컨슈머 수를 함께 늘린다.

### 오프셋 리셋

컨슈머 그룹을 종료한 상태(멤버 없음)에서만 가능하다.

```bash
# 미리보기 (--dry-run 이 기본)
docker exec -it kafka-lab /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --group my-group --topic hello.kafka --reset-offsets --to-earliest

# 실제 적용
docker exec -it kafka-lab /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --group my-group --topic hello.kafka --reset-offsets --to-earliest --execute
```

`--to-earliest` 대신 `--to-latest`, `--to-offset 3`, `--shift-by -2`, `--to-datetime 2026-10-11T00:00:00.000` 등을 쓸 수 있다.

### 파티션별 오프셋 조회 (-1 / -2)

```bash
# -1: 가장 최신 오프셋 (LOG-END-OFFSET)
docker exec -it kafka-lab /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic hello.kafka --time -1

# -2: 가장 오래된 오프셋 (리텐션으로 지워지기 전의 시작점)
docker exec -it kafka-lab /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic hello.kafka --time -2
```

출력 형식은 `토픽:파티션:오프셋` 이다.

---

## 6. 저장된 데이터 들여다보기

브로커의 `log.dirs` 가 호스트의 `./data` 에 마운트되어 있으므로 호스트에서 바로 볼 수 있다.

```bash
# <토픽>-<파티션> 디렉토리
ls data/ | grep hello
ls -l data/hello.kafka-0/

# 컨테이너 안에서 보기
docker exec -it kafka-lab ls -l /var/lib/kafka/data/hello.kafka-0/
```

```
00000000000000000000.index      # 오프셋 -> 파일 위치 인덱스
00000000000000000000.log        # 실제 레코드 (세그먼트, 이름 = 시작 오프셋)
00000000000000000000.timeindex  # 타임스탬프 -> 오프셋 인덱스
leader-epoch-checkpoint
partition.metadata
```

세그먼트 파일 내용 덤프:

```bash
docker exec -it kafka-lab /opt/kafka/bin/kafka-dump-log.sh --print-data-log \
  --files /var/lib/kafka/data/hello.kafka-0/00000000000000000000.log
```

각 레코드의 `offset`, `CreateTime`, `key`, `payload` 를 확인할 수 있다.

**실습: 세그먼트 분할 보기** — 세그먼트 크기를 작게 잡은 토픽에 데이터를 넣으면 `.log` 파일이 여러 개로 나뉘고,
파일 이름이 각 세그먼트의 시작 오프셋이 된다. (`segment.bytes` 최소값은 1MB = 1048576)

```bash
docker exec -it kafka-lab /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic hello.segment --partitions 1 --config segment.bytes=1048576

# 약 100 byte 짜리 메시지 30,000개 (약 3MB) 전송
docker exec -i kafka-lab sh -c 'seq 1 30000 | sed "s/$/ ........................................................................................./" | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic hello.segment'

docker exec -it kafka-lab ls -l /var/lib/kafka/data/hello.segment-0/
```

```
00000000000000000000.log   # offset 0 ~ 10059
00000000000000010060.log   # offset 10060 ~ 20043
00000000000000020044.log   # offset 20044 ~       <- ACTIVE 세그먼트
```

---

## 7. 파티션 재분배 (참고)

리더 파티션이 특정 브로커에 쏠렸을 때 `kafka-reassign-partitions.sh` 로 다시 분배한다.
이 실습 환경은 브로커가 1대라 실제로 옮길 곳이 없으므로 명령 형태만 확인한다.

```bash
docker exec -it kafka-lab /opt/kafka/bin/kafka-reassign-partitions.sh --help

# 선호 리더(Replicas 목록의 첫 번째 브로커)로 리더를 되돌리기
docker exec -it kafka-lab /opt/kafka/bin/kafka-leader-election.sh --bootstrap-server localhost:9092 \
  --election-type preferred --all-topic-partitions
```

---

## 8. 정리

```bash
# 토픽 삭제
docker exec -it kafka-lab /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic hello.kafka2
docker exec -it kafka-lab /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic hello.segment

# 컨슈머 그룹 삭제 (멤버가 없어야 함)
docker exec -it kafka-lab /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --delete --group my-group2

# 브로커 중지 (데이터는 ./data 에 남음)
docker compose down
```

---

## 트러블슈팅

**브로커가 `AccessDeniedException` / `Shutdown broker because all log dirs ... have failed` 로 종료됨**

macOS Docker Desktop 에서 호스트 디렉토리(`./data`)를 bind mount 할 때 가끔 파일 rename 이 권한 오류로 실패하면서
브로커가 스스로 종료된다. 데이터는 남아 있으므로 다시 띄우면 대부분 복구된다.

```bash
docker start kafka-lab     # 또는 docker compose up -d
docker logs --tail 50 kafka-lab
```

자주 반복되면 `docker-compose.yaml` 의 `./data:/var/lib/kafka/data` 를 named volume 으로 바꾸는 것을 고려한다.
