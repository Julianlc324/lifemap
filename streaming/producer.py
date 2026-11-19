"""Productor Kafka con reloj acelerado (E2.2).

Publica en el topic `reviews` (3 particiones, clave = business_id) SOLO reseñas posteriores a T,
en orden de fecha. El reloj simulado avanza `--days-per-tick` días cada `--tick-seconds` segundos
(por defecto, un mes de reseñas cada 10 segundos).

Ejemplos:
  docker compose run --rm producer --days-per-tick 30 --tick-seconds 10
  docker compose run --rm producer --limit 100 --id-prefix smoke-42   # prueba de humo
"""
import argparse
import json
import math
import time
from datetime import date, datetime, time as dtime, timedelta, timezone

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic, NewPartitions
from kafka.errors import TopicAlreadyExistsError

from lifemap import config, db
from lifemap.metrics import cutoff_exclusive


def parse_args(argv=None):
    p = argparse.ArgumentParser(description="Productor acelerado de reseñas posteriores a T")
    p.add_argument("--days-per-tick", type=float, default=30.0, help="días simulados por tick")
    p.add_argument("--tick-seconds", type=float, default=10.0, help="segundos reales por tick")
    p.add_argument("--start", type=date.fromisoformat, default=None, help="inicio (> T); por defecto T")
    p.add_argument("--end", type=date.fromisoformat, default=None, help="fin opcional")
    p.add_argument("--limit", type=int, default=0, help="publica N mensajes y termina (0 = sin límite)")
    p.add_argument("--id-prefix", default="", help="prefijo para review_id (pruebas de humo)")
    p.add_argument("--topic", default=config.KAFKA_TOPIC)
    p.add_argument("--bootstrap", default=config.KAFKA_BOOTSTRAP)
    args = p.parse_args(argv)
    if not math.isfinite(args.days_per_tick) or args.days_per_tick <= 0:
        p.error("--days-per-tick debe ser finito y positivo")
    if not math.isfinite(args.tick_seconds) or args.tick_seconds < 0 or args.limit < 0:
        p.error("--tick-seconds debe ser finito/no negativo y --limit no negativo")
    return args


def ensure_topic(bootstrap, topic, partitions):
    partitions = max(3, partitions)
    admin = KafkaAdminClient(bootstrap_servers=bootstrap)
    try:
        admin.create_topics([NewTopic(topic, num_partitions=partitions, replication_factor=1)])
        print(f"Topic {topic} creado con {partitions} particiones")
    except TopicAlreadyExistsError:
        count = len(admin.describe_topics([topic])[0]["partitions"])
        if count < partitions:
            admin.create_partitions({topic: NewPartitions(total_count=partitions)})
    finally:
        admin.close()


def to_message(doc, prefix=""):
    d = doc["date"]
    return {
        "review_id": f"{prefix}{doc['_id']}",
        "business_id": doc["business_id"],
        "stars": int(doc["stars"]),
        "text": doc["text"],
        "date": d.isoformat(),
        "produced_at": datetime.now(timezone.utc).isoformat(),
    }


def main(argv=None):
    args = parse_args(argv)
    boundary = cutoff_exclusive(config.CUTOFF_T)
    start = datetime.combine(args.start, dtime.min) if args.start else boundary
    if start < boundary:
        raise SystemExit(f"--start debe ser posterior al día T={config.CUTOFF_T}")
    query = {"date": {"$gte": start}}
    if args.end:
        end = cutoff_exclusive(args.end)
        if end <= start:
            raise SystemExit("--end debe ser igual o posterior a --start")
        query["date"]["$lt"] = end

    ensure_topic(args.bootstrap, args.topic, config.KAFKA_PARTITIONS)
    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap, acks="all", linger_ms=20, retries=5, max_in_flight_requests_per_connection=1,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"))
    cursor = (db.get_db().reviews.find(query, {"business_id": 1, "stars": 1, "text": 1, "date": 1})
              .sort([("date", 1), ("_id", 1)]).batch_size(2000))

    sent, pending = 0, None
    if args.limit:
        for doc in cursor.limit(args.limit):
            producer.send(args.topic, key=doc["business_id"], value=to_message(doc, args.id_prefix)).get(timeout=60)
            sent += 1
        producer.flush()
        print(f"Publicados {sent} mensajes (modo límite) en '{args.topic}'")
        cursor.close()
        producer.close()
        return sent

    first = next(cursor, None)
    if first is None:
        print("No hay reseñas posteriores a T para publicar")
        cursor.close()
        producer.close()
        return 0
    sim = first["date"]
    pending = first
    print(f"Reloj simulado desde {sim:%Y-%m-%d}: {args.days_per_tick} días cada {args.tick_seconds} s")
    while pending is not None:
        sim = sim + timedelta(days=args.days_per_tick)
        n_tick = 0
        while pending is not None and pending["date"] < sim:
            producer.send(args.topic, key=pending["business_id"], value=to_message(pending, args.id_prefix)).get(timeout=60)
            sent += 1
            n_tick += 1
            pending = next(cursor, None)
        producer.flush()
        db.set_meta("producer", sim_time=sim, sent=sent, updated_at=datetime.now(timezone.utc))
        print(f"[{sim:%Y-%m-%d}] +{n_tick} reseñas (total {sent})")
        if pending is not None:
            time.sleep(args.tick_seconds)
    cursor.close()
    producer.close()
    print(f"Fin de la reproducción: {sent} reseñas publicadas")
    return sent


if __name__ == "__main__":
    main()
