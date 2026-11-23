"""Koneksi ke MongoDB replica set dan Redis Cluster.

Semua alamat diambil dari environment variable supaya kode yang sama
bisa jalan di Docker (hostname mongo1, redis-1, ...) maupun di luar Docker.
"""
import os
import time

from pymongo import MongoClient, ReadPreference
from pymongo.read_concern import ReadConcern
from pymongo.write_concern import WriteConcern
from redis.cluster import RedisCluster, ClusterNode

MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb://mongo1:27017,mongo2:27017,mongo3:27017/?replicaSet=rs0",
)
REDIS_NODES = os.getenv(
    "REDIS_NODES",
    "10.250.0.21:6379,10.250.0.22:6379,10.250.0.23:6379",
)

_mongo = None
_redis = None


def mongo_client(timeout_ms=5000):
    global _mongo
    if _mongo is None:
        _mongo = MongoClient(
            MONGO_URI,
            serverSelectionTimeoutMS=timeout_ms,
            retryWrites=True,
        )
    return _mongo


def ehr_db():
    """Database EHR dengan jaminan konsistensi kuat (CP).

    w=majority     : tulis dianggap sukses kalau sudah ada di >= 2 dari 3 node
    readConcern    : baca hanya data yang sudah di-commit mayoritas
    readPreference : baca dari primary, jadi dokter selalu lihat data terbaru
    """
    return mongo_client().get_database(
        "ehr",
        write_concern=WriteConcern(w="majority", wtimeout=5000),
        read_concern=ReadConcern("majority"),
        read_preference=ReadPreference.PRIMARY,
    )


def ehr_db_analytics():
    """Untuk query laporan/riwayat yang berat: dilempar ke secondary."""
    return mongo_client().get_database(
        "ehr", read_preference=ReadPreference.SECONDARY_PREFERRED
    )


def redis_client():
    global _redis
    if _redis is None:
        nodes = []
        for item in REDIS_NODES.split(","):
            host, port = item.strip().rsplit(":", 1)
            nodes.append(ClusterNode(host, int(port)))
        _redis = RedisCluster(
            startup_nodes=nodes,
            decode_responses=True,
            socket_timeout=2,
            socket_connect_timeout=2,
        )
    return _redis


def reset_redis():
    """Buang koneksi lama supaya peta slot dibaca ulang (dipakai setelah failover)."""
    global _redis
    try:
        if _redis is not None:
            _redis.close()
    except Exception:  # noqa: BLE001
        pass
    _redis = None


def wait_until_ready(max_wait=120):
    """Tunggu sampai replica set punya primary dan Redis Cluster status ok."""
    start = time.time()
    last_err = None
    while time.time() - start < max_wait:
        try:
            hello = mongo_client().admin.command("hello")
            if not hello.get("isWritablePrimary") and not hello.get("primary"):
                raise RuntimeError("replica set belum punya primary")
            r = redis_client()
            info = r.cluster_info()
            if info.get("cluster_state") != "ok":
                raise RuntimeError("redis cluster belum ok")
            return True
        except Exception as e:  # noqa: BLE001
            last_err = e
            reset_redis()
            time.sleep(2)
    raise RuntimeError(f"Database belum siap: {last_err}")
