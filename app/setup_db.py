"""Menyiapkan struktur database dan data dummy. Aman dijalankan berulang kali.

MongoDB : koleksi facilities, patients (dengan validator), queue_histories + index
Redis   : kuota poli per hari dan consumer group untuk stream event
"""
import random
from datetime import datetime, timedelta, timezone

from pymongo import ASCENDING
from pymongo.errors import CollectionInvalid
from redis.exceptions import ResponseError

from db import ehr_db, redis_client, wait_until_ready
from queue_service import k_events, k_quota, GROUP

FACILITIES = [
    {
        "tenant_id": "rsa",
        "name": "RS Sehat Sentosa",
        "region": "tengah",
        "poli": [
            {"code": "umum", "name": "Poli Umum", "quota": 40},
            {"code": "gigi", "name": "Poli Gigi", "quota": 20},
            {"code": "anak", "name": "Poli Anak", "quota": 30},
        ],
    },
    {
        "tenant_id": "klb",
        "name": "Klinik Bunda Asih",
        "region": "timur",
        "poli": [
            {"code": "umum", "name": "Poli Umum", "quota": 30},
            {"code": "kia", "name": "Poli KIA", "quota": 20},
        ],
    },
]

PATIENT_VALIDATOR = {
    "$jsonSchema": {
        "bsonType": "object",
        "required": ["tenant_id", "mrn", "name", "birth_date", "gender"],
        "properties": {
            "tenant_id": {"bsonType": "string"},
            "mrn": {"bsonType": "string"},
            "name": {
                "bsonType": "object",
                "required": ["given", "family"],
            },
            "birth_date": {"bsonType": "date"},
            "gender": {"enum": ["male", "female"]},
            "allergies": {"bsonType": "array"},
            "encounters": {"bsonType": "array"},
            "lab_results": {"bsonType": "array"},
        },
    }
}

GIVEN_M = ["Budi", "Agus", "Rizky", "Dimas", "Fajar", "Hendra", "Joko", "Bayu", "Arif", "Yoga"]
GIVEN_F = ["Siti", "Dewi", "Ayu", "Rina", "Putri", "Nur", "Fitri", "Indah", "Lestari", "Wulan"]
FAMILY = ["Santoso", "Wijaya", "Pratama", "Saputra", "Hidayat", "Kusuma", "Nugroho", "Rahmawati", "Setiawan", "Utami"]
ALLERGENS = [
    ("Amoksisilin", "ruam kulit", "sedang"),
    ("Penisilin", "sesak napas", "berat"),
    ("Udang", "gatal", "ringan"),
    ("Kacang tanah", "bengkak bibir", "sedang"),
]
# (kode ICD-10, diagnosis, keluhan, obat, aturan pakai)
CASES = [
    ("J06.9", "ISPA akut tidak spesifik", "batuk pilek", "Paracetamol 500 mg", "3x1"),
    ("K29.7", "Gastritis", "nyeri ulu hati", "Omeprazol 20 mg", "2x1"),
    ("I10", "Hipertensi esensial", "pusing dan tengkuk berat", "Amlodipin 5 mg", "1x1"),
    ("E11.9", "Diabetes melitus tipe 2", "sering haus dan sering kencing", "Metformin 500 mg", "2x1"),
    ("A09", "Diare dan gastroenteritis", "BAB cair 4 kali sehari", "Oralit", "tiap BAB cair"),
]
LABS = [
    ("Gula darah puasa", "mg/dL", 70, 160, "70-100"),
    ("Hemoglobin", "g/dL", 10, 16, "12-16"),
    ("Kolesterol total", "mg/dL", 150, 260, "<200"),
]


def make_patient(tenant_id, n, rng):
    gender = rng.choice(["male", "female"])
    given = rng.choice(GIVEN_M if gender == "male" else GIVEN_F)
    now = datetime.now(timezone.utc)
    doc = {
        "tenant_id": tenant_id,
        "mrn": f"{tenant_id.upper()}-{n:06d}",
        "resourceType": "Patient",
        "name": {"given": given, "family": rng.choice(FAMILY)},
        "birth_date": datetime(rng.randint(1955, 2005), rng.randint(1, 12), rng.randint(1, 28), tzinfo=timezone.utc),
        "gender": gender,
        "phone": f"08{rng.randint(1000000000, 9999999999)}",
        "allergies": [],
        "encounters": [],
        "lab_results": [],
        "created_at": now,
        "updated_at": now,
    }
    if rng.random() < 0.35:
        sub, reac, sev = rng.choice(ALLERGENS)
        doc["allergies"].append({"substance": sub, "reaction": reac, "severity": sev})
    for i in range(rng.randint(0, 3)):
        code, desc, complaint, drug, freq = rng.choice(CASES)
        day = now - timedelta(days=rng.randint(10, 400))
        doc["encounters"].append({
            "encounter_id": f"ENC-{tenant_id.upper()}-{n:06d}-{i+1}",
            "date": day,
            "poli": "umum",
            "doctor": "dr. Andi",
            "chief_complaint": complaint,
            "vital_signs": {"systolic": rng.randint(105, 160), "diastolic": rng.randint(65, 100),
                            "temperature": round(rng.uniform(36.2, 38.5), 1), "pulse": rng.randint(65, 105)},
            "anamnesis": "keluhan sejak 3 hari",
            "diagnoses": [{"icd10": code, "description": desc}],
            "prescriptions": [{"drug": drug, "frequency": freq, "duration_days": 5}],
        })
    doc["encounters"].sort(key=lambda e: e["date"])
    if rng.random() < 0.5:
        test, unit, lo, hi, ref = rng.choice(LABS)
        doc["lab_results"].append({"test": test, "value": rng.randint(lo, hi), "unit": unit,
                                   "ref_range": ref, "date": now - timedelta(days=rng.randint(5, 200))})
    return doc


def setup_mongo(patients_per_tenant=40):
    db = ehr_db()
    for name, opts in [("patients", {"validator": PATIENT_VALIDATOR}),
                       ("facilities", {}), ("queue_histories", {})]:
        try:
            db.create_collection(name, **opts)
        except CollectionInvalid:
            pass
    db.patients.create_index([("tenant_id", ASCENDING), ("mrn", ASCENDING)], unique=True, name="tenant_mrn_unique")
    db.patients.create_index([("tenant_id", ASCENDING), ("name.family", ASCENDING)], name="tenant_family")
    db.facilities.create_index("tenant_id", unique=True)
    db.queue_histories.create_index(
        [("tenant_id", ASCENDING), ("date", ASCENDING), ("poli", ASCENDING)], name="tenant_date_poli")

    for f in FACILITIES:
        db.facilities.replace_one({"tenant_id": f["tenant_id"]}, f, upsert=True)

    rng = random.Random(42)
    for f in FACILITIES:
        if db.patients.count_documents({"tenant_id": f["tenant_id"]}) > 0:
            continue
        docs = [make_patient(f["tenant_id"], i + 1, rng) for i in range(patients_per_tenant)]
        db.patients.insert_many(docs)
    print(f"[mongo] facilities={db.facilities.count_documents({})} "
          f"patients={db.patients.count_documents({})}")


def setup_redis():
    r = redis_client()
    for f in FACILITIES:
        for p in f["poli"]:
            r.set(k_quota(f["tenant_id"], p["code"]), p["quota"])
            try:
                r.xgroup_create(k_events(f["tenant_id"], p["code"]), GROUP, id="0", mkstream=True)
            except ResponseError as e:
                if "BUSYGROUP" not in str(e):
                    raise
    print(f"[redis] kuota & stream event siap untuk {sum(len(f['poli']) for f in FACILITIES)} poli")


if __name__ == "__main__":
    wait_until_ready()
    setup_mongo()
    setup_redis()
    print("[setup] selesai")
