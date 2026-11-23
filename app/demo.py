"""Menu demo sistem EHR + Antrean Real-Time.

Jalankan:  docker compose exec app python demo.py
Skenario otomatis (tanpa input):  docker compose exec app python demo.py --auto
"""
import os
import random
import statistics
import sys
import time
from datetime import datetime

from pymongo import MongoClient
from pymongo.errors import PyMongoError

import db
import ehr_service as ehr
import queue_service as qs

TENANT, POLI = "rsa", "umum"
CURRENT = {"mrn": None}

B, G, R, Y, C, D, X = "\033[1m", "\033[32m", "\033[31m", "\033[33m", "\033[36m", "\033[2m", "\033[0m"


def title(text):
    print(f"\n{B}{C}== {text} =={X}")


def ask(label, default=None):
    hint = f" [{default}]" if default not in (None, "") else ""
    val = input(f"{label}{hint}: ").strip()
    return val or (default if default is not None else "")


def node_names():
    names = {}
    for pair in os.getenv("REDIS_NAMES", "").split(","):
        if "=" in pair:
            addr, name = pair.split("=", 1)
            names[addr.strip()] = name.strip()
    return names


# ------------------------------------------------------------------- 1 status
def status_cluster():
    title("Status cluster")
    st = db.mongo_client().admin.command("replSetGetStatus")
    print(f"{B}MongoDB replica set '{st['set']}'{X}")
    for m in st["members"]:
        color = G if m["stateStr"] == "PRIMARY" else (Y if m["stateStr"] == "SECONDARY" else R)
        print(f"  {m['name']:<16} {color}{m['stateStr']:<10}{X} health={int(m['health'])}")

    print(f"\n{B}Redis Cluster{X} (16384 slot)")
    names = node_names()
    nodes = db.redis_client().cluster_nodes()
    masters = {v["node_id"]: k for k, v in nodes.items() if "master" in v["flags"]}
    for addr, v in sorted(nodes.items(), key=lambda kv: names.get(kv[0], kv[0])):
        name = names.get(addr, addr)
        if "fail" in v["flags"]:
            print(f"  {name:<10} {R}DOWN{X}")
        elif "master" in v["flags"]:
            slots = ", ".join(f"{s[0]}-{s[1]}" for s in v["slots"]) or "-"
            print(f"  {name:<10} {G}MASTER{X}  slot {slots}")
        else:
            m = names.get(masters.get(v["master_id"], ""), "?")
            print(f"  {name:<10} {Y}REPLICA{X} dari {m}")
    print(f"  cluster_state = {db.redis_client().cluster_info()['cluster_state']}")


# ------------------------------------------------------------------- antrean
def sample_mrns(n=5):
    docs = db.ehr_db().patients.find({"tenant_id": TENANT}, {"mrn": 1, "name": 1, "_id": 0}).limit(n)
    return [f"{d['mrn']} ({d['name']['given']} {d['name']['family']})" for d in docs]


def do_register(mrn=None, complaint=None):
    title(f"Daftar antrean {TENANT}/{POLI}")
    if mrn is None:
        print(D + "Contoh MRN: " + ", ".join(sample_mrns()) + X)
        mrn = ask("MRN pasien").upper()
        complaint = ask("Keluhan awal", "demam dan batuk")
    p = db.ehr_db().patients.find_one({"tenant_id": TENANT, "mrn": mrn}, {"name": 1})
    if not p:
        print(f"{R}MRN tidak ditemukan{X}")
        return
    name = f"{p['name']['given']} {p['name']['family']}"
    try:
        ticket, pos = qs.register(TENANT, POLI, mrn, name, complaint)
    except ValueError as e:
        print(f"{R}{e}{X}")
        return
    est = qs.position(TENANT, POLI, mrn)["estimated_wait_min"]
    print(f"{G}Terdaftar{X}: {name} | nomor tiket {B}{ticket}{X} | posisi {pos} | estimasi tunggu {est} menit")


def show_queue():
    title(f"Antrean {TENANT}/{POLI} hari ini ({qs.today()})")
    rows = qs.list_queue(TENANT, POLI)
    if not rows:
        print("  (kosong)")
        return
    print(f"  {'Pos':<4}{'Tiket':<7}{'MRN':<13}{'Nama':<20}{'Prioritas':<10}Keluhan")
    for r in rows:
        pr = f"{R}darurat{X}  " if r.get("priority") == "darurat" else "normal   "
        print(f"  {r['position']:<4}{r['ticket']:<7}{r['mrn']:<13}{r['name']:<20}{pr} {r.get('complaint', '')}")
    key = qs.k_queue(TENANT, POLI, qs.today())
    print(D + f"  key: {key}  (TTL sisa {db.redis_client().ttl(key)} detik)" + X)


def do_triage(mrn=None, priority=None, vitals=None):
    title("Input tanda vital & triage (staf/perawat)")
    if mrn is None:
        mrn = ask("MRN pasien").upper()
        priority = ask("Prioritas (normal/darurat)", "normal")
        vitals = [ask("Sistolik", "120"), ask("Diastolik", "80"), ask("Suhu", "36.8"), ask("Nadi", "80")]
    try:
        pos = qs.triage(TENANT, POLI, mrn, priority, *vitals)
    except (ValueError, KeyError) as e:
        print(f"{R}{e}{X}")
        return
    print(f"{G}Tersimpan{X}. {mrn} sekarang di posisi {B}{pos}{X} (prioritas {priority})")


def do_cancel(mrn=None):
    title("Batalkan antrean")
    mrn = mrn or ask("MRN pasien").upper()
    ok = qs.cancel(TENANT, POLI, mrn)
    print(f"{G}Dibatalkan{X}, event QueueCancelled dikirim ke worker" if ok else f"{R}Gagal: tidak berstatus menunggu{X}")


def do_call():
    title("Panggil pasien berikutnya (dokter)")
    mrn = qs.call_next(TENANT, POLI)
    if not mrn:
        print("Antrean kosong")
        return
    CURRENT["mrn"] = mrn
    e = qs.get_entry(TENANT, POLI, mrn)
    print(f"Memanggil nomor {B}{e['ticket']}{X}: {e['name']} ({mrn}), prioritas {e['priority']}")
    print(D + "Event PatientCalled dikirim. Worker mem-prefetch rekam medisnya ke Redis." + X)


def do_open_ehr(mrn=None):
    title("Buka rekam medis (Single-View Patient)")
    mrn = mrn or ask("MRN pasien", CURRENT["mrn"]).upper()
    doc, source, ms = ehr.get_patient(TENANT, mrn)
    if not doc:
        print(f"{R}Tidak ditemukan{X}")
        return
    age = datetime.now().year - doc["birth_date"].year
    print(f"{B}{doc['name']['given']} {doc['name']['family']}{X} | {doc['mrn']} | {doc['gender']} | {age} th")
    al = ", ".join(f"{a['substance']} ({a['severity']})" for a in doc.get("allergies", [])) or "tidak ada"
    print(f"Alergi : {R if doc.get('allergies') else ''}{al}{X}")
    print(f"Riwayat kunjungan: {len(doc.get('encounters', []))}")
    for enc in doc.get("encounters", [])[-3:]:
        dx = enc["diagnoses"][0]
        rx = enc["prescriptions"][0]
        print(f"  - {enc['date']:%Y-%m-%d} {dx['icd10']} {dx['description']} | resep: {rx['drug']} {rx['frequency']}")
    for lab in doc.get("lab_results", []):
        print(f"Lab    : {lab['test']} = {lab['value']} {lab['unit']} (normal {lab['ref_range']})")
    print(D + f"Sumber: {source} | waktu baca {ms:.2f} ms" + X)


def do_finish(mrn=None, data=None):
    title("Selesaikan konsultasi (dokter)")
    mrn = mrn or ask("MRN pasien", CURRENT["mrn"]).upper()
    if data is None:
        data = [ask("Anamnesis", "demam 2 hari, batuk berdahak"), ask("Kode ICD-10", "J06.9"),
                ask("Diagnosis", "ISPA akut"), ask("Obat", "Paracetamol 500 mg"), ask("Aturan pakai", "3x1"),
                int(ask("Lama (hari)", "5"))]
    entry = qs.get_entry(TENANT, POLI, mrn)
    if entry.get("status") != "dipanggil":
        print(f"{R}Pasien belum dipanggil{X}")
        return
    try:
        enc = ehr.add_encounter(TENANT, POLI, mrn, entry.get("doctor", "dr. Andi"), *data)
    except PyMongoError as e:
        print(f"{R}Gagal menyimpan ke MongoDB: {e}{X}")
        return
    qs.finish(TENANT, POLI, mrn)
    print(f"{G}Tersimpan di MongoDB (w=majority){X}: {enc['encounter_id']}")
    print(D + "Event ConsultationFinished dikirim. Worker memindahkan data antrean ke queue_histories." + X)


def show_history():
    title(f"Riwayat antrean {TENANT} hari ini (MongoDB queue_histories)")
    rows = db.ehr_db_analytics().queue_histories.find(
        {"tenant_id": TENANT, "date": qs.today()}, {"_id": 0}).sort("ticket", 1)
    for r in rows:
        w = f"{r['wait_seconds']} dtk" if r["wait_seconds"] is not None else "-"
        print(f"  {r['poli']:<6} tiket {r['ticket']:<3} {r['mrn']:<12} {r['status']:<8} tunggu {w}")
    print(f"\n{B}Ringkasan (aggregation, dibaca dari secondary){X}")
    for g in ehr.queue_report(TENANT, qs.today()):
        avg = g["rata_tunggu_detik"]
        print(f"  {g['_id']['poli']:<6} {g['_id']['status']:<8} jumlah {g['jumlah']}"
              + (f", rata-rata tunggu {avg:.0f} dtk" if avg is not None else ""))


def show_hash_tag():
    title("Bukti hash tag: key satu poli selalu di slot yang sama")
    r = db.redis_client()
    names = node_names()
    d = qs.today()
    for t, p in [("rsa", "umum"), ("rsa", "gigi"), ("rsa", "anak"), ("klb", "umum"), ("klb", "kia")]:
        keys = [qs.k_queue(t, p, d), qs.k_patient(t, p, d, "RSA-000001"), qs.k_counter(t, p, d), qs.k_events(t, p)]
        slots = {r.keyslot(k) for k in keys}
        slot = slots.pop()
        node = r.get_node_from_key(keys[0])
        addr = f"{node.host}:{node.port}"
        print(f"  {{{t}:{p}}}  -> slot {slot:<5} di {names.get(addr, addr):<8} (4 key, {'1 slot' if not slots else 'BEDA SLOT!'})")
    print(D + "Tanpa hash tag, 4 key itu tersebar ke slot berbeda dan script Lua ditolak (error CROSSSLOT)." + X)


def show_document():
    title("Contoh dokumen pasien lengkap (MongoDB)")
    mrn = ask("MRN pasien", CURRENT["mrn"] or "RSA-000001").upper()
    doc = db.ehr_db().patients.find_one({"tenant_id": TENANT, "mrn": mrn})
    print(ehr.to_json(doc))


# ----------------------------------------------------------------- benchmark
def pct(values, p):
    values = sorted(values)
    return values[min(len(values) - 1, int(len(values) * p / 100))]


def benchmark(n=300):
    title(f"Uji kinerja ({n} operasi)")
    coll = db.ehr_db().patients
    mrns = [d["mrn"] for d in coll.find({"tenant_id": TENANT}, {"mrn": 1})]
    lat = []
    for _ in range(n):
        m = random.choice(mrns)
        t0 = time.perf_counter()
        coll.find_one({"tenant_id": TENANT, "mrn": m})
        lat.append((time.perf_counter() - t0) * 1000)
    print(f"MongoDB baca rekam medis by MRN : p50 {statistics.median(lat):.2f} ms | p95 {pct(lat, 95):.2f} ms (target < 100 ms)")

    ex = coll.find({"tenant_id": TENANT, "mrn": mrns[0]}).explain()
    stats = ex["executionStats"]
    plan = ex["queryPlanner"]["winningPlan"]
    stage = plan.get("inputStage", plan).get("stage")
    print(f"  explain(): stage {B}{stage}{X}, dokumen diperiksa {stats['totalDocsExamined']}")
    ex2 = coll.find({"phone": "0800"}).explain()
    stage2 = ex2["queryPlanner"]["winningPlan"].get("stage")
    print(f"  pembanding tanpa index (cari by phone): stage {stage2}, "
          f"dokumen diperiksa {ex2['executionStats']['totalDocsExamined']}")

    r = db.redis_client()
    key = qs.k_queue(TENANT, POLI, qs.today())
    lat = []
    for _ in range(n):
        t0 = time.perf_counter()
        r.zrank(key, "RSA-000001")
        lat.append((time.perf_counter() - t0) * 1000)
    print(f"Redis cek posisi antrean (ZRANK)  : p50 {statistics.median(lat):.3f} ms | p95 {pct(lat, 95):.3f} ms (target < 1 ms)")
    lat = []
    for _ in range(n):
        t0 = time.perf_counter()
        r.zadd("bench:{rsa:umum}", {f"m{random.randint(1, 50)}": time.time()})
        lat.append((time.perf_counter() - t0) * 1000)
    r.delete("bench:{rsa:umum}")
    print(f"Redis update antrean (ZADD)       : p50 {statistics.median(lat):.3f} ms | p95 {pct(lat, 95):.3f} ms")
    print(D + "Waktu diukur dari sisi aplikasi, sudah termasuk perjalanan jaringan antar container." + X)


# ------------------------------------------------------------- uji failover
def failover_test():
    title("Uji failover (Ctrl+C untuk berhenti)")
    print("Di terminal lain jalankan:  docker kill mongo1   (simulasi server crash)")
    print("                       atau:  docker kill redis-1")
    print("Program menulis ke MongoDB dan Redis tiap 0,5 detik dan mencatat kapan gagal.\n")
    mc = MongoClient(db.MONGO_URI, serverSelectionTimeoutMS=1000, w="majority", wTimeoutMS=2000)
    hb = mc.ehr.heartbeat
    down = {"mongo": None, "redis": None}
    total = {"mongo": 0.0, "redis": 0.0}

    def mark(name, ok, now, took):
        if not ok and down[name] is None:
            down[name] = now
        if ok and down[name] is not None:
            gap = now + took - down[name]
            total[name] += gap
            print(f"   {G}{name} pulih setelah {gap:.1f} detik{X}")
            down[name] = None
        elif ok and took > 1.0:
            # perintah tidak error, tapi tertahan menunggu failover selesai
            total[name] += took
            print(f"   {Y}{name} tertahan {took:.1f} detik menunggu failover, lalu berhasil{X}")

    try:
        while True:
            now = time.time()
            stamp = datetime.now().strftime("%H:%M:%S")
            t_m = time.time()
            try:
                hb.insert_one({"t": now})
                primary = mc.primary
                m_ok, m_txt = True, f"{G}OK{X} (primary {primary[0] + ':' + str(primary[1]) if primary else '?'})"
            except PyMongoError as e:
                m_ok, m_txt = False, f"{R}GAGAL{X} {type(e).__name__}"
            took_m = time.time() - t_m
            t_r = time.time()
            try:
                db.redis_client().incr("heartbeat:{failover}")
                r_ok, r_txt = True, f"{G}OK{X}"
            except Exception as e:  # noqa: BLE001
                r_ok, r_txt = False, f"{R}GAGAL{X} {type(e).__name__}"
                db.reset_redis()
            took_r = time.time() - t_r
            print(f"{stamp}  mongo {m_txt:<40} redis {r_txt}")
            mark("mongo", m_ok, t_m, took_m)
            mark("redis", r_ok, t_r, took_r)
            time.sleep(0.5)
    except KeyboardInterrupt:
        print(f"\nTotal gangguan tulis: MongoDB {total['mongo']:.1f} dtk, Redis {total['redis']:.1f} dtk")


# ------------------------------------------------------------------ skenario
def auto():
    """Skenario lengkap tanpa input, dipakai untuk demo cepat atau pengujian."""
    status_cluster()
    for mrn, c in [("RSA-000001", "demam"), ("RSA-000002", "sakit kepala"),
                   ("RSA-000003", "nyeri dada"), ("RSA-000004", "batuk")]:
        do_register(mrn, c)
    do_register("RSA-000001", "daftar dua kali")          # harus ditolak
    show_queue()
    do_triage("RSA-000003", "darurat", ["170", "100", "37.0", "110"])
    do_triage("RSA-000001", "normal", ["120", "80", "38.2", "92"])
    do_cancel("RSA-000004")
    show_queue()
    do_call()
    time.sleep(1.5)                                        # beri waktu worker prefetch
    do_open_ehr(CURRENT["mrn"])
    do_finish(CURRENT["mrn"], ["nyeri dada kiri 1 jam", "I20.9", "Angina pektoris", "ISDN 5 mg", "sublingual", 1])
    do_call()
    time.sleep(1.5)
    do_finish(CURRENT["mrn"], ["demam 2 hari", "J06.9", "ISPA akut", "Paracetamol 500 mg", "3x1", 5])
    time.sleep(1.5)
    show_queue()
    show_history()
    show_hash_tag()


MENU = [
    ("Status cluster (MongoDB & Redis)", status_cluster),
    ("Daftar antrean [pasien]", do_register),
    ("Lihat antrean poli", show_queue),
    ("Input tanda vital & triage [staf]", do_triage),
    ("Batalkan antrean [pasien]", do_cancel),
    ("Panggil pasien berikutnya [dokter]", do_call),
    ("Buka rekam medis [dokter/pasien]", do_open_ehr),
    ("Selesaikan konsultasi [dokter]", do_finish),
    ("Riwayat & laporan antrean", show_history),
    ("Bukti hash tag Redis", show_hash_tag),
    ("Lihat dokumen pasien (JSON)", show_document),
    ("Uji kinerja", benchmark),
    ("Uji failover", failover_test),
    ("Ganti faskes/poli", None),
]


def main():
    global TENANT, POLI
    if "--auto" in sys.argv:
        auto()
        return
    if "--bench" in sys.argv:
        benchmark()
        return
    while True:
        print(f"\n{B}EHR & Antrean Real-Time{X}  faskes={TENANT} poli={POLI}")
        for i, (label, _) in enumerate(MENU, 1):
            print(f" {i:>2}. {label}")
        print("  0. Keluar")
        choice = input("> ").strip()
        if choice == "0":
            break
        if not choice.isdigit() or not 1 <= int(choice) <= len(MENU):
            continue
        label, fn = MENU[int(choice) - 1]
        if fn is None:
            TENANT = ask("Faskes (rsa/klb)", TENANT)
            POLI = ask("Poli (rsa: umum/gigi/anak, klb: umum/kia)", POLI)
            continue
        try:
            fn()
        except Exception as e:  # noqa: BLE001
            print(f"{R}Error: {type(e).__name__}: {e}{X}")


if __name__ == "__main__":
    main()
