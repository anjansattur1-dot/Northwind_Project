import time
import json
import logging
import random
from kafka import KafkaProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [PRODUCER] %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = "localhost:9092"   # change if needed
TOPIC_NAME = "orders"

CUSTOMERS = [
    "ALFKI", "ANATR", "ANTON", "AROUT", "BERGS",
    "BLAUS", "BOLID", "BONAP", "BOTTM", "BSBEV"
]

EMPLOYEE_IDS = [1, 2, 3, 4, 5, 6, 7, 8, 9]
SHIPPER_IDS = [1, 2, 3]


def generate_order(order_id: int) -> dict:
    order_date = time.strftime("%Y-%m-%d")
    required_date = order_date

    # 70% shipped same day, 30% pending
    shipped_date = order_date if random.random() < 0.7 else None

    return {
        "order_id": order_id,
        "customer_id": random.choice(CUSTOMERS),
        "employee_id": random.choice(EMPLOYEE_IDS),
        "order_date": order_date,
        "required_date": required_date,
        "shipped_date": shipped_date,
        "shipper_id": random.choice(SHIPPER_IDS),
        "freight": round(random.uniform(10.0, 150.0), 2)
    }


def stream_data():
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        max_block_ms=5000
    )

    curr_time = time.time()
    count = 0
    order_id = 20001

    logger.info("Streaming started -> topic: %s (runs for 60 sec)", TOPIC_NAME)
    logger.info("-" * 60)

    while True:
        if time.time() > curr_time + 60:
            break

        try:
            order = generate_order(order_id)
            producer.send(TOPIC_NAME, value=order)
            count += 1

            print(
                "[{0:>3}] SENT -> order_id={1} | customer_id={2} | freight={3}".format(
                    count, order["order_id"], order["customer_id"], order["freight"]
                )
            )

            order_id += 1
            time.sleep(2)

        except Exception as e:
            logger.error("An error occurred: %s", e)

    producer.flush()
    producer.close()
    logger.info("Done. %s messages sent in 60 seconds.", count)


if __name__ == "__main__":
    stream_data()
