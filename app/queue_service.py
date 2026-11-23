"""Queue Service: antrean poli real-time di Redis Cluster.

Pola key (bagian {...} adalah HASH TAG, yang menentukan slot):
  queue:{rsa:umum}:2026-11-19          ZSET   urutan antrean (score = prioritas + waktu daftar)
  pasien:{rsa:umum}:2026-11-19:MRN     HASH   status & data pasien di antrean hari itu
  counter:{rsa:umum}:2026-11-19        STRING nomor tiket terakhir (INCR)
  quota:{rsa:umum}                     STRING kuota harian poli
  events:{rsa:umum}                    STREAM event untuk worker (PatientCalled, dst.)

Karena semua key satu poli memakai hash tag yang sama, semuanya ada di slot
dan node yang sama. Itu syarat agar script Lua (operasi atomik multi-key) boleh
jalan di Redis Cluster. Poli lain punya tag lain, jadi tersebar ke node lain.
"""
import time
from datetime import datetime, timedelta, timezone

from db import redis_client

WIB = timezone(timedelta(hours=7))
TTL_SECONDS = 86400          # data antrean harian otomatis hilang setelah 24 jam
AVG_SERVICE_MIN = 10         # asumsi rata-rata lama layanan per pasien
GROUP = "ehr-worker"         # consumer group stream
PRIORITY_LEVEL = {"darurat": 0, "normal": 1}


def today():
    return datetime.now(WIB).strftime("%Y-%m-%d")


def now_iso():
    return datetime.now(WIB).isoformat(timespec="seconds")


def k_queue(t, p, d):
    return f"queue:{{{t}:{p}}}:{d}"


def k_patient(t, p, d, mrn):
    return f"pasien:{{{t}:{p}}}:{d}:{mrn}"


def k_counter(t, p, d):
    return f"counter:{{{t}:{p}}}:{d}"


def k_quota(t, p):
    return f"quota:{{{t}:{p}}}"


def k_events(t, p):
    return f"events:{{{t}:{p}}}"


def k_cache(t, mrn):
    # sengaja TANPA hash tag: cache rekam medis tersebar merata ke semua node
    return f"ehrcache:{t}:{mrn}"


def score_for(level, registered_ms):
    return level * 10**13 + registered_ms


# ---------------------------------------------------------------- Lua scripts
LUA_REGISTER = """
local st = redis.call('HGET', KEYS[3], 'status')
if st == 'menunggu' or st == 'dipanggil' then return {-2, -1} end
local quota = tonumber(redis.call('GET', KEYS[4]) or '0')
local used = tonumber(redis.call('GET', KEYS[1]) or '0')
if quota > 0 and used >= quota then return {-1, -1} end
local n = redis.call('INCR', KEYS[1])
redis.call('ZADD', KEYS[2], ARGV[1], ARGV[2])
redis.call('DEL', KEYS[3])
redis.call('HSET', KEYS[3], 'mrn', ARGV[2], 'name', ARGV[6], 'ticket', n,
           'status', 'menunggu', 'priority', ARGV[3], 'complaint', ARGV[4],
           'registered_at', ARGV[5], 'registered_ms', ARGV[8])
redis.call('EXPIRE', KEYS[1], ARGV[7], 'NX')
redis.call('EXPIRE', KEYS[2], ARGV[7], 'NX')
redis.call('EXPIRE', KEYS[3], ARGV[7])
return {n, redis.call('ZRANK', KEYS[2], ARGV[2])}
"""

LUA_TRIAGE = """
if redis.call('HGET', KEYS[2], 'status') ~= 'menunggu' then return -1 end
redis.call('ZADD', KEYS[1], 'XX', ARGV[3], ARGV[1])
redis.call('HSET', KEYS[2], 'priority', ARGV[2], 'systolic', ARGV[4], 'diastolic', ARGV[5],
           'temperature', ARGV[6], 'pulse', ARGV[7], 'triaged_at', ARGV[8])
return redis.call('ZRANK', KEYS[1], ARGV[1])
"""

LUA_CANCEL = """
if redis.call('HGET', KEYS[2], 'status') ~= 'menunggu' then return 0 end
redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('HSET', KEYS[2], 'status', 'batal', 'finished_at', ARGV[2])
redis.call('XADD', KEYS[3], 'MAXLEN', '~', '1000', '*', 'type', 'QueueCancelled',
           'tenant', ARGV[3], 'poli', ARGV[4], 'date', ARGV[5], 'mrn', ARGV[1])
return 1
"""

LUA_CALL = """
if not redis.call('ZSCORE', KEYS[1], ARGV[1]) then return 0 end
redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('HSET', KEYS[2], 'status', 'dipanggil', 'called_at', ARGV[2], 'doctor', ARGV[6])
redis.call('XADD', KEYS[3], 'MAXLEN', '~', '1000', '*', 'type', 'PatientCalled',
           'tenant', ARGV[3], 'poli', ARGV[4], 'date', ARGV[5], 'mrn', ARGV[1])
return 1
"""

LUA_FINISH = """
if redis.call('HGET', KEYS[1], 'status') ~= 'dipanggil' then return 0 end
redis.call('HSET', KEYS[1], 'status', 'selesai', 'finished_at', ARGV[1])
redis.call('XADD', KEYS[2], 'MAXLEN', '~', '1000', '*', 'type', 'ConsultationFinished',
           'tenant', ARGV[2], 'poli', ARGV[3], 'date', ARGV[4], 'mrn', ARGV[5])
return 1
"""


_scripts = {}


def _script(src):
    if src not in _scripts:
        _scripts[src] = redis_client().register_script(src)
    return _scripts[src]


# ------------------------------------------------------------------ operations
def register(tenant, poli, mrn, name, complaint, priority="normal"):
    """FR-01: pasien mendaftar antrean. Return (nomor_tiket, posisi) atau error."""
    d = today()
    reg_ms = int(time.time() * 1000)
    score = score_for(PRIORITY_LEVEL[priority], reg_ms)
    res = _script(LUA_REGISTER)(
        keys=[k_counter(tenant, poli, d), k_queue(tenant, poli, d),
              k_patient(tenant, poli, d, mrn), k_quota(tenant, poli), k_events(tenant, poli)],
        args=[score, mrn, priority, complaint, now_iso(), name, TTL_SECONDS, reg_ms],
    )
    ticket, rank = int(res[0]), int(res[1])
    if ticket == -1:
        raise ValueError("Kuota poli hari ini sudah penuh")
    if ticket == -2:
        raise ValueError("Pasien sudah ada di antrean poli ini")
    return ticket, rank + 1


def position(tenant, poli, mrn):
    """FR-02: posisi antrean & estimasi waktu tunggu. O(log N)."""
    rank = redis_client().zrank(k_queue(tenant, poli, today()), mrn)
    if rank is None:
        return None
    return {"position": rank + 1, "people_ahead": rank,
            "estimated_wait_min": rank * AVG_SERVICE_MIN}


def list_queue(tenant, poli):
    r = redis_client()
    d = today()
    mrns = r.zrange(k_queue(tenant, poli, d), 0, -1)
    if not mrns:
        return []
    pipe = r.pipeline()
    for m in mrns:
        pipe.hgetall(k_patient(tenant, poli, d, m))
    rows = pipe.execute()
    return [dict(row, position=i + 1) for i, row in enumerate(rows)]


def get_entry(tenant, poli, mrn):
    return redis_client().hgetall(k_patient(tenant, poli, today(), mrn))


def triage(tenant, poli, mrn, priority, systolic, diastolic, temperature, pulse):
    """FR-06/07: staf input tanda vital dan menentukan prioritas (triage)."""
    d = today()
    entry = get_entry(tenant, poli, mrn)
    if not entry:
        raise ValueError("Pasien tidak ada di antrean")
    score = score_for(PRIORITY_LEVEL[priority], int(entry["registered_ms"]))
    rank = _script(LUA_TRIAGE)(
        keys=[k_queue(tenant, poli, d), k_patient(tenant, poli, d, mrn)],
        args=[mrn, priority, score, systolic, diastolic, temperature, pulse, now_iso()],
    )
    if rank == -1:
        raise ValueError("Pasien sudah tidak berstatus menunggu")
    return rank + 1


def cancel(tenant, poli, mrn):
    """FR-03: pasien membatalkan antrean."""
    d = today()
    ok = _script(LUA_CANCEL)(
        keys=[k_queue(tenant, poli, d), k_patient(tenant, poli, d, mrn), k_events(tenant, poli)],
        args=[mrn, now_iso(), tenant, poli, d],
    )
    return bool(ok)


def call_next(tenant, poli, doctor="dr. Andi"):
    """FR-09: dokter memanggil pasien berikutnya. Aman walau 2 dokter memanggil bersamaan."""
    r = redis_client()
    d = today()
    for _ in range(5):
        head = r.zrange(k_queue(tenant, poli, d), 0, 0)
        if not head:
            return None
        mrn = head[0]
        ok = _script(LUA_CALL)(
            keys=[k_queue(tenant, poli, d), k_patient(tenant, poli, d, mrn), k_events(tenant, poli)],
            args=[mrn, now_iso(), tenant, poli, d, doctor],
        )
        if ok:
            return mrn
    return None


def finish(tenant, poli, mrn):
    """Menandai konsultasi selesai dan menerbitkan event ConsultationFinished."""
    d = today()
    ok = _script(LUA_FINISH)(
        keys=[k_patient(tenant, poli, d, mrn), k_events(tenant, poli)],
        args=[now_iso(), tenant, poli, d, mrn],
    )
    return bool(ok)
