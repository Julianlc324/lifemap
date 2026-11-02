"""Control de pasos idempotentes del pipeline (marca en la colección meta)."""
import os
import time
from datetime import datetime, timezone

from . import db


def forced(name):
    raw = os.environ.get("FORCE_STEPS", "")
    items = {x.strip() for x in raw.replace(" ", ",").split(",") if x.strip()}
    return "all" in items or name in items


def should_run(name):
    if forced(name):
        print(f"[{name}] forzado por FORCE_STEPS")
        return True
    done = db.get_meta(f"step:{name}").get("done", False)
    if done:
        print(f"[{name}] ya ejecutado; se omite (FORCE_STEPS={name} para repetir)")
    return not done


def mark_done(name, **info):
    db.set_meta(f"step:{name}", done=True, finished_at=datetime.now(timezone.utc), **info)


def wait_for_mongo(retries=60):
    for _ in range(retries):
        try:
            db.client().admin.command("ping")
            return
        except Exception as exc:  # noqa: BLE001
            print("Esperando MongoDB...", exc)
            time.sleep(3)
    raise RuntimeError("MongoDB no responde")
