"""실습 공통 설정."""

import os

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
DEFAULT_TOPIC = "hello.python"


def delivery_report(err, msg):
    """Producer 전송 결과 콜백: 브로커가 붙여준 partition / offset 을 확인할 수 있다."""
    if err is not None:
        print(f"[FAIL] key={msg.key()} err={err}")
        return
    key = msg.key().decode() if msg.key() else None
    print(f"[OK] key={key!s:<8} -> partition={msg.partition()} offset={msg.offset()}")
