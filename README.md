# EHR & Antrean Real-Time (Polyglot NoSQL)

MongoDB replica set (3 node) untuk rekam medis + Redis Cluster (6 node) untuk antrean poli.

## Yang dibutuhkan

- Docker Desktop (Windows/Mac/Linux), sudah dalam keadaan **running**
- RAM kosong kira-kira 2 GB
- Internet saat pertama kali jalan (download image sekitar 1 GB)

## Menjalankan

Buka terminal (PowerShell) di folder ini, lalu:

```
docker compose up -d --build
```

Tunggu 1 sampai 2 menit, lalu cek:

```
docker compose ps
```

Harus ada 11 container berstatus `running`/`healthy` (mongo1-3, redis-1..6, ehr-app, ehr-worker).
`mongo-init` dan `redis-init` berstatus `exited (0)`, dan itu normal karena keduanya cuma jalan sekali.

## Demo

Terminal 1, menu demo:

```
docker compose exec app python demo.py
```

Terminal 2, log worker (supaya event terlihat jalan):

```
docker compose logs -f worker
```

Skenario lengkap otomatis (kalau mau cepat): `docker compose exec app python demo.py --auto`

### Urutan demo yang disarankan

1. Menu 1, status cluster (3 node Mongo, 6 node Redis)
2. Menu 2, daftarkan 3 pasien, lalu menu 3 untuk lihat antrean
3. Menu 4, triage salah satu pasien jadi `darurat`, lalu menu 3 lagi (posisinya naik ke 1)
4. Menu 6, panggil pasien (lihat terminal 2: rekam medis di-prefetch)
5. Menu 7, buka rekam medis (sumbernya "cache Redis")
6. Menu 8, selesaikan konsultasi, lalu menu 9 (riwayat masuk MongoDB)
7. Menu 10, bukti hash tag
8. Menu 12, uji kinerja
9. Menu 13, uji failover. Di terminal lain jalankan `docker kill mongo1`, tunggu sampai muncul "pulih", lalu `docker kill redis-1`. Tekan Ctrl+C untuk melihat total gangguan.
10. Hidupkan lagi node yang dimatikan: `docker start mongo1 redis-1`, lalu menu 1 (mongo1 kembali jadi PRIMARY karena priority 2, redis-1 kembali sebagai REPLICA)

`docker kill` = simulasi server crash (failover sekitar 10 detik).
`docker stop` = mematikan dengan rapi; MongoDB menyerahkan primary duluan, jadi hampir tanpa gangguan.

## Melihat data dengan MongoDB Compass (opsional)

Connection string: `mongodb://localhost:27017/?directConnection=true`
(27018 dan 27019 untuk mongo2 dan mongo3). Database: `ehr`.

## Melihat Redis langsung (opsional)

```
docker exec -it redis-1 redis-cli -c
cluster info
keys *
```

## Reset total

```
docker compose down -v
```

## Masalah yang mungkin muncul

| Gejala | Solusi |
|---|---|
| `port 27017 is already allocated` | Ada MongoDB lain di laptop. Matikan service-nya, atau ganti `27017:27017` jadi `27117:27017` di docker-compose.yml |
| `Pool overlaps with other one on this address space` | Subnet 10.250.0.0/24 bentrok. Ganti semua `10.250.0.` di docker-compose.yml ke angka lain, misal `10.251.0.` |
| app tidak kunjung healthy | `docker compose logs app`, biasanya karena init belum selesai. Tunggu, atau `docker compose restart app` |

## Struktur file

```
docker-compose.yml   definisi 11 container + skrip inisialisasi cluster
app/db.py            koneksi (write/read concern majority, Redis Cluster client)
app/setup_db.py      koleksi, validator, index, data dummy 80 pasien
app/queue_service.py antrean di Redis (ZSET, HASH, Lua script, Stream)
app/ehr_service.py   rekam medis di MongoDB
app/worker.py        konsumen event: prefetch & arsip
app/demo.py          menu demo, uji kinerja, uji failover
```
