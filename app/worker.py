"""Worker penghubung Queue Service (Redis) dan EHR Service (MongoDB).

Membaca Redis Stream events:{tenant:poli} lewat consumer group:
  PatientCalled         -> prefetch rekam medis pasien ke cache Redis
  ConsultationFinished  -> arsipkan data antrean ke MongoDB queue_histories, hapus dari Redis
  QueueCancelled        -> sama, dengan status batal

Event baru di-ACK setelah berhasil diproses. Kalau worker mati di tengah jalan,
event yang belum di-ACK dibaca ulang saat worker hidup lagi (tidak ada yang hilang).
"""
import socket
import time

from db import ehr_db, redis_client, reset_redis, wait_until_ready
import ehr_service as ehr
import queue_service as qs

CONSUMER = socket.gethostname()


def handle(fields):
    t, p, d, mrn = fields["tenant"], fields["poli"], fields["date"], fields["mrn"]
    kind = fields["type"]
    if kind == "PatientCalled":
        ehr.cache_patient(t, mrn)
        print(f"[worker] PatientCalled        {t}/{p} {mrn} -> rekam medis di-prefetch ke Redis", flush=True)
    elif kind in ("ConsultationFinished", "QueueCancelled"):
        key = qs.k_patient(t, p, d, mrn)
        entry = redis_client().hgetall(key)
        if entry:
            doc = ehr.archive_queue_entry(entry, t, p, d)
            redis_client().delete(key)
            print(f"[worker] {kind:<20} {t}/{p} {mrn} -> diarsipkan ke MongoDB "
                  f"(status={doc['status']}, tunggu={doc['wait_seconds'] if doc['wait_seconds'] is not None else '-'} dtk)", flush=True)


def stream_keys():
    keys = []
    for f in ehr_db().facilities.find({}, {"tenant_id": 1, "poli.code": 1}):
        for p in f["poli"]:
            keys.append(qs.k_events(f["tenant_id"], p["code"]))
    return keys


def run():
    wait_until_ready()
    keys = stream_keys()
    print(f"[worker] siap, memantau {len(keys)} stream event", flush=True)
    pending_first = True
    while True:
        try:
            r = redis_client()
            got = False
            for key in keys:
                # "0" = baca event milik worker ini yang belum di-ACK (setelah restart),
                # ">" = baca event baru
                start = "0" if pending_first else ">"
                resp = r.xreadgroup(qs.GROUP, CONSUMER, {key: start}, count=50)
                for _stream, messages in resp or []:
                    for msg_id, fields in messages:
                        if not fields:
                            continue
                        handle(fields)
                        r.xack(key, qs.GROUP, msg_id)
                        got = True
            pending_first = False
            if not got:
                time.sleep(0.3)
        except Exception as e:  # noqa: BLE001  (misal saat failover node Redis/Mongo)
            print(f"[worker] gangguan koneksi: {type(e).__name__}: {e}. Coba lagi 2 detik...", flush=True)
            reset_redis()
            pending_first = True  # setelah pulih, proses dulu event yang belum sempat di-ACK
            time.sleep(2)


if __name__ == "__main__":
    run()
