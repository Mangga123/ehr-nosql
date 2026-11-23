"""EHR Service: rekam medis pasien di MongoDB (satu dokumen per pasien)."""
import json
import time
from datetime import datetime, timezone

from bson import json_util

from db import ehr_db, ehr_db_analytics, redis_client
import queue_service as qs

CACHE_TTL = 900  # cache hasil prefetch berlaku 15 menit


def get_patient(tenant, mrn, use_cache=True):
    """FR-04/FR-10: Single-View Patient. Cek cache Redis dulu, kalau tidak ada ambil dari MongoDB.

    Return (dokumen, sumber, waktu_ms)
    """
    t0 = time.perf_counter()
    if use_cache:
        cached = redis_client().get(qs.k_cache(tenant, mrn))
        if cached:
            doc = json_util.loads(cached)
            return doc, "cache Redis (hasil prefetch)", (time.perf_counter() - t0) * 1000
    doc = ehr_db().patients.find_one({"tenant_id": tenant, "mrn": mrn}, {"_id": 0})
    return doc, "MongoDB primary", (time.perf_counter() - t0) * 1000


def cache_patient(tenant, mrn):
    """Dipakai worker saat event PatientCalled: simpan salinan rekam medis ke Redis."""
    doc = ehr_db().patients.find_one({"tenant_id": tenant, "mrn": mrn}, {"_id": 0})
    if doc:
        redis_client().set(qs.k_cache(tenant, mrn), json_util.dumps(doc), ex=CACHE_TTL)
    return doc is not None


def search_by_family(tenant, family):
    return list(ehr_db().patients.find(
        {"tenant_id": tenant, "name.family": family},
        {"_id": 0, "mrn": 1, "name": 1, "birth_date": 1},
    ).limit(20))


def add_encounter(tenant, poli, mrn, doctor, anamnesis, icd10, diagnosis, drug, frequency, days):
    """FR-11/12: dokter menyimpan hasil pemeriksaan. Tanda vital diambil dari data triage di Redis."""
    entry = qs.get_entry(tenant, poli, mrn)
    vitals = {}
    for f in ("systolic", "diastolic", "pulse"):
        if f in entry:
            vitals[f] = int(entry[f])
    if "temperature" in entry:
        vitals["temperature"] = float(entry["temperature"])
    now = datetime.now(timezone.utc)
    encounter = {
        "encounter_id": f"ENC-{mrn}-{int(now.timestamp())}",
        "date": now,
        "poli": poli,
        "doctor": doctor,
        "chief_complaint": entry.get("complaint", ""),
        "vital_signs": vitals,
        "anamnesis": anamnesis,
        "diagnoses": [{"icd10": icd10, "description": diagnosis}],
        "prescriptions": [{"drug": drug, "frequency": frequency, "duration_days": days}],
    }
    res = ehr_db().patients.update_one(
        {"tenant_id": tenant, "mrn": mrn},
        {"$push": {"encounters": encounter}, "$set": {"updated_at": now}},
    )
    if res.matched_count != 1:
        raise ValueError("Pasien tidak ditemukan di MongoDB")
    redis_client().delete(qs.k_cache(tenant, mrn))  # cache lama tidak boleh dibaca lagi
    return encounter


def archive_queue_entry(entry, tenant, poli, date):
    """Dipakai worker: memindahkan data antrean dari Redis (sementara) ke MongoDB (permanen)."""
    def ts(key):
        return datetime.fromisoformat(entry[key]) if entry.get(key) else None

    reg, called, fin = ts("registered_at"), ts("called_at"), ts("finished_at")
    doc = {
        "tenant_id": tenant,
        "poli": poli,
        "date": date,
        "mrn": entry.get("mrn"),
        "ticket": int(entry.get("ticket", 0)),
        "status": entry.get("status"),
        "priority": entry.get("priority"),
        "complaint": entry.get("complaint"),
        "registered_at": reg,
        "called_at": called,
        "finished_at": fin,
        "wait_seconds": int((called - reg).total_seconds()) if called and reg else None,
    }
    ehr_db().queue_histories.update_one(
        {"tenant_id": tenant, "poli": poli, "date": date, "ticket": doc["ticket"]},
        {"$set": doc}, upsert=True,
    )
    return doc


def queue_report(tenant, date):
    """Laporan harian (dibaca dari secondary supaya tidak membebani primary)."""
    pipeline = [
        {"$match": {"tenant_id": tenant, "date": date}},
        {"$group": {"_id": {"poli": "$poli", "status": "$status"},
                    "jumlah": {"$sum": 1}, "rata_tunggu_detik": {"$avg": "$wait_seconds"}}},
        {"$sort": {"_id.poli": 1}},
    ]
    return list(ehr_db_analytics().queue_histories.aggregate(pipeline))


def to_json(doc):
    return json.dumps(doc, indent=2, ensure_ascii=False, default=str)
