# JdbcSMTReplaceField

Kumpulan **Single Message Transform (SMT)** untuk Kafka Connect:

1. `ReplaceNullWithValue`: mengganti nilai `NULL` pada field tertentu dengan nilai yang ditentukan.
2. `ReplaceValue`: mengganti nilai tertentu pada sebuah field dengan nilai lain (misalnya `###` menjadi `####`).
3. `FilterRecord`: membuang record (tidak ikut ke sink) berdasarkan nilai sebuah field (misalnya `MB_NO` bernilai `822`, `503`, `564`).

Ketiganya dibuat untuk dipakai bersama **JDBC Sink Connector** pada Confluent Platform 7.9.x (Apache Kafka 3.9.x).

Contoh: pada topic, `MB_NO = NULL` diubah menjadi `MB_NO = '###'` sebelum ditulis ke database.

Artefak hasil build: `JdbcSMTReplaceField-1.0.jar`

---

## 1. Cara kerja

```
Topic Kafka --> Converter (Avro) --> SMT ReplaceNullWithValue --> JDBC Sink --> Database
```

- SMT berjalan di Connect worker, sebelum record sampai ke JDBC Sink. JDBC Sink tidak perlu dimodifikasi.
- Jika field yang terdaftar bernilai `null`, field diisi dengan nilai pengganti.
- Nilai pengganti dikonversi otomatis sesuai tipe field di schema:

| Tipe schema | Contoh nilai pengganti |
|---|---|
| STRING | `###`, `-`, `N/A` |
| INT8 / INT16 / INT32 / INT64 | `0`, `-1` |
| FLOAT32 / FLOAT64 | `0.0` |
| BOOLEAN | `false` |
| DECIMAL (logical type) | `0` (otomatis disesuaikan dengan scale schema) |
| BYTES | teks UTF-8 |

- Logical type Date, Time, dan Timestamp **belum didukung** (akan melempar `DataException`).
- Tombstone (value `null`) dibiarkan apa adanya, sehingga `delete.enabled` pada JDBC Sink tetap berfungsi.
- Untuk data tanpa schema (Map), nilai pengganti dipasang sebagai string.

## 2. Class yang tersedia

| Class | Dipakai untuk |
|---|---|
| `com.yokke.kafka.connect.smt.ReplaceNullWithValue$Value` | Field di **value** record (kasus umum) |
| `com.yokke.kafka.connect.smt.ReplaceNullWithValue$Key` | Field di **key** record (misalnya `pk.mode=record_key`) |
| `com.yokke.kafka.connect.smt.ReplaceValue$Value` | Mengganti nilai lama menjadi nilai baru di **value** record |
| `com.yokke.kafka.connect.smt.ReplaceValue$Key` | Mengganti nilai lama menjadi nilai baru di **key** record |
| `com.yokke.kafka.connect.smt.FilterRecord$Key` | Membuang record berdasarkan nilai field di **key** |
| `com.yokke.kafka.connect.smt.FilterRecord$Value` | Membuang record berdasarkan nilai field di **value** |

## 3. Parameter

| Parameter | Tipe | Wajib | Keterangan |
|---|---|---|---|
| `replacements` | list | ya | Pasangan `FIELD:NILAI` dipisah koma. Contoh: `MB_NO:###,CUST_NM:-,AMOUNT:0` |

Aturan penulisan:
- Nama field **case-sensitive** dan harus sama persis dengan nama field di schema topic (misalnya `MB_NO`, bukan `mb_no`).
- Tulis nilai **tanpa tanda kutip**. `MB_NO:###` menghasilkan `###`. Jika ditulis `MB_NO:'###'`, tanda kutipnya ikut tersimpan.
- Nilai pengganti **tidak boleh mengandung koma** (koma adalah pemisah pasangan).
- Pemisah field dan nilai adalah tanda `:` pertama, jadi nilai boleh mengandung `:` (contoh `URL:http://x`).

### 3b. `ReplaceValue`: ganti nilai tertentu menjadi nilai lain

Dipakai kalau field sudah berisi nilai (bukan `NULL`) dan ingin diganti, misalnya `MB_NO = ###` menjadi `MB_NO = ####`.

| Parameter | Tipe | Wajib | Keterangan |
|---|---|---|---|
| `replacements` | list | ya | Aturan `FIELD:NILAI_LAMA=>NILAI_BARU` dipisah koma |

```properties
transforms=replaceMb
transforms.replaceMb.type=com.yokke.kafka.connect.smt.ReplaceValue$Value
transforms.replaceMb.replacements=MB_NO:###=>####
```

Beberapa aturan sekaligus (aturan pertama yang cocok dipakai):

```properties
transforms.replaceMb.replacements=MB_NO:###=>####,MB_NO:--=>####,CUST_NM:N/A=>-
```

Aturan:
- Nilai yang bernilai `NULL` **tidak** diubah oleh transform ini. Untuk `NULL` gunakan `ReplaceNullWithValue`.
- Pencocokan nilai lama bersifat persis (case-sensitive untuk string). Nilai dikonversi ke tipe field di schema sebelum dibandingkan, jadi `AMOUNT:0=>1` juga bisa dipakai untuk field numerik.
- Nama field case-sensitive, sama seperti `ReplaceNullWithValue`.
- Nilai tidak boleh mengandung koma. Pemisah lama dan baru adalah `=>`, sehingga nilai tidak boleh mengandung `=>`.
- Field yang nilainya tidak cocok dengan aturan manapun dibiarkan apa adanya.

Mengubah `NULL` **dan** `###` menjadi `####` sekaligus, pakai dua transform:

```properties
transforms=nullToDefault,replaceMb
transforms.nullToDefault.type=com.yokke.kafka.connect.smt.ReplaceNullWithValue$Value
transforms.nullToDefault.replacements=MB_NO:####
transforms.replaceMb.type=com.yokke.kafka.connect.smt.ReplaceValue$Value
transforms.replaceMb.replacements=MB_NO:###=>####
```

### 3c. Pakai `$Key` atau `$Value`?

Kedua SMT punya dua varian. Pilih sesuai dari mana JDBC Sink mengambil kolom yang bermasalah:

| Kolom yang bermasalah | Varian |
|---|---|
| Bagian primary key dan `pk.mode=record_key` (field ada di **key** record) | `$Key` |
| Kolom biasa (bukan primary key), atau `pk.mode=record_value` / `none` | `$Value` |

Penting: pada `pk.mode=record_key`, field yang ada di key diambil dari key, dan field dengan nama yang sama di value diabaikan oleh JDBC Sink. Jadi kalau `MB_NO` ada di key, mengubah `MB_NO` lewat `$Value` tidak berpengaruh. Pakai `$Key`.

Cara mengetahui field ada di key atau di value: cek schema di Schema Registry.

```bash
curl -s http://<schema-registry>:8081/subjects/<TOPIC>-key/versions/latest
curl -s http://<schema-registry>:8081/subjects/<TOPIC>-value/versions/latest
```

Contoh `$Key` (field primary key `MB_NO` bernilai `NULL` menjadi `####`):

```properties
transforms=nullToDefaultKey
transforms.nullToDefaultKey.type=com.yokke.kafka.connect.smt.ReplaceNullWithValue$Key
transforms.nullToDefaultKey.replacements=MB_NO:####
```

Contoh `$Key` untuk mengganti nilai (`###` menjadi `####` di key):

```properties
transforms=replaceMbKey
transforms.replaceMbKey.type=com.yokke.kafka.connect.smt.ReplaceValue$Key
transforms.replaceMbKey.replacements=MB_NO:###=>####
```

Contoh `$Value` (kolom biasa `REQ_DATE` bernilai `NULL` menjadi `-`):

```properties
transforms=nullToDefault
transforms.nullToDefault.type=com.yokke.kafka.connect.smt.ReplaceNullWithValue$Value
transforms.nullToDefault.replacements=REQ_DATE:-
```

Key dan value bisa dipakai bersamaan. Setiap transform butuh alias sendiri.

Catatan untuk primary key: nilai pengganti menjadi bagian identitas baris. Kalau di sumber `MB_NO` kelak diisi nilai asli untuk kombinasi key yang sama, sink menulis baris baru dan baris dengan nilai pengganti tetap ada.

### 3d. `FilterRecord`: record tidak ikut ke sink

Dipakai untuk membuang record tertentu sehingga tidak ditulis ke tabel tujuan. Data di topic tetap utuh, hanya connector ini yang melewatinya.

| Parameter | Tipe | Wajib | Default | Keterangan |
|---|---|---|---|---|
| `field` | string | ya | | Nama field yang diperiksa (case-sensitive) |
| `values` | list | ya | | Daftar nilai dipisah koma, contoh `822,503,564` |
| `action` | string | tidak | `drop` | `drop` = buang record yang cocok, `keep` = hanya loloskan record yang cocok |
| `trim` | boolean | tidak | `true` | Spasi di awal/akhir nilai pada record diabaikan saat membandingkan |

Contoh: record dengan `MB_NO` = `822 `, `503`, atau `564` tidak ikut ke tabel (field ada di **key**):

```properties
transforms=dropMb
transforms.dropMb.type=com.yokke.kafka.connect.smt.FilterRecord$Key
transforms.dropMb.field=MB_NO
transforms.dropMb.values=822,503,564
transforms.dropMb.action=drop
```

Aturan dan catatan:
- Karena `trim=true`, nilai `822 ` (dengan spasi di belakang, misalnya dari kolom CHAR) dan `822` sama-sama cocok dengan `822`. Kafka selalu memotong spasi di ujung item list pada konfigurasi, jadi nilai di `values` tidak bisa ditulis dengan spasi di ujung. Biarkan `trim=true`.
- Pencocokan bersifat persis (case-sensitive) setelah trim. `822` tidak cocok dengan `8220`.
- Record yang field-nya `NULL` atau tidak ada dianggap **tidak cocok**: dengan `drop` record tetap lolos, dengan `keep` record dibuang.
- Pakai `$Key` kalau field adalah bagian primary key pada `pk.mode=record_key`, pakai `$Value` untuk kolom biasa (lihat 3c).
- Letakkan `FilterRecord$Key` **paling awal** di daftar `transforms`. Letakkan `FilterRecord$Value` **tepat sesudah `unwrap`** kalau topic berformat envelope Debezium (lihat bagian 7b). Keduanya harus berada sebelum transform yang mengubah field yang sama (misalnya sebelum `nullToDefaultKey`).
- Record yang dibuang hilang diam-diam dari sink: tidak masuk dead letter queue dan tidak muncul di log error.
- Pada `$Key`, tombstone dengan key yang cocok ikut dibuang.

## 4. Prasyarat build

| Cara build | Kebutuhan |
|---|---|
| `build.sh` (tanpa Maven) | JDK 11 atau lebih baru (`javac`), instalasi Confluent di mesin build (untuk JAR `connect-api` dan `kafka-clients`) |
| Maven | JDK 11 atau lebih baru, Maven 3.6+, akses ke Maven Central atau repository internal |

Runtime Connect memakai Java 21. JAR dikompilasi dengan target Java 11 sehingga kompatibel.

## 5. Build

Masuk ke folder project terlebih dahulu:

```bash
cd replace-null-smt
```

### Opsi A: `build.sh` (disarankan, tanpa Maven dan tanpa internet)

```bash
./build.sh                                # default lib: /opt/confluent/share/java/kafka
./build.sh /path/ke/lib/kafka             # jika lokasi lib berbeda
```

Hasil: `build/JdbcSMTReplaceField-1.0.jar`

Script memakai `connect-api-*.jar` dan `kafka-clients-*.jar` dari folder lib Confluent, sehingga versinya otomatis sama dengan runtime Connect.

### Opsi B: Maven

```bash
mvn clean package
```

Hasil: `target/JdbcSMTReplaceField-1.0.jar`

`connect-api` berscope `provided` (sudah ada di runtime Connect), jadi JAR tidak membawa dependency lain.

Jika muncul error `Could not transfer artifact ... repo.maven.apache.org ... Connect timed out`, mesin build tidak bisa mengakses Maven Central. Pilihannya:

1. Gunakan **Opsi A**.
2. Build di laptop/PC yang punya internet, lalu salin JAR ke server.
3. Atur proxy atau mirror (Nexus/Artifactory) di `~/.m2/settings.xml`:

```xml
<settings>
  <mirrors>
    <mirror>
      <id>internal</id>
      <mirrorOf>central</mirrorOf>
      <url>https://nexus.perusahaan.local/repository/maven-public/</url>
    </mirror>
  </mirrors>
  <!-- atau lewat proxy: -->
  <proxies>
    <proxy>
      <id>corp-proxy</id>
      <active>true</active>
      <protocol>http</protocol>
      <host>proxy.perusahaan.local</host>
      <port>8080</port>
    </proxy>
  </proxies>
</settings>
```

## 6. Deploy ke Kafka Connect

Lakukan di **semua** Connect worker.

1. Buat folder khusus di dalam `plugin.path` (lihat `plugin.path` di `connect-distributed.properties`) dan salin JAR ke sana. Folder terpisah menjaga JAR tetap terisolasi dari plugin lain.
   ```bash
   mkdir -p /opt/confluent/confluent-hub-components/JdbcSMTReplaceField
   cp build/JdbcSMTReplaceField-1.0.jar /opt/confluent/confluent-hub-components/JdbcSMTReplaceField/
   # jika build dengan Maven: cp target/JdbcSMTReplaceField-1.0.jar ...
   ```
2. **Restart Connect worker.** Restart wajib, karena Connect hanya memindai `plugin.path` saat worker start. Lakukan rolling restart: restart satu worker, tunggu worker bergabung kembali ke cluster, baru lanjut ke worker berikutnya. Connector dan task dipindahkan otomatis oleh rebalance, jadi tidak perlu menghentikan semua connector.
3. Verifikasi plugin terdeteksi di **setiap** worker:
   ```bash
   curl -s http://<connect-host>:8083/connector-plugins | grep -i ReplaceNullWithValue
   ```
   Hasil yang benar menampilkan `ReplaceNullWithValue` dan `ReplaceValue` (masing-masing `$Value` dan `$Key`) dengan type `transformation`.

Catatan:
- Worker yang belum punya JAR akan membuat task yang dijadwalkan ke worker itu gagal (`Class ... could not be found`).
- Mengganti JAR dengan versi lain juga membutuhkan restart worker.
- Mengubah konfigurasi connector (misalnya `replacements`) lewat REST API **tidak** membutuhkan restart worker.

## 7. Konfigurasi di connector

### Properties

```properties
transforms=nullToDefault
transforms.nullToDefault.type=com.yokke.kafka.connect.smt.ReplaceNullWithValue$Value
transforms.nullToDefault.replacements=MB_NO:###
```

### JSON (REST API)

Contoh lengkap ada di `sink-connector-example.json`.

```json
{
  "name": "jdbc_sink_example",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
    "topics": "NAMA_TOPIC",
    "connection.url": "jdbc:oracle:thin:@//host:1521/SERVICE",
    "connection.user": "user",
    "connection.password": "password",
    "insert.mode": "upsert",
    "pk.mode": "record_key",
    "pk.fields": "ID",
    "auto.create": "false",

    "transforms": "dropMb,nullToDefaultKey,replaceMbKey,nullToDefault,replaceVal",

    "transforms.dropMb.type": "com.yokke.kafka.connect.smt.FilterRecord$Key",
    "transforms.dropMb.field": "MB_NO",
    "transforms.dropMb.values": "822,503,564",
    "transforms.dropMb.action": "drop",

    "transforms.nullToDefaultKey.type": "com.yokke.kafka.connect.smt.ReplaceNullWithValue$Key",
    "transforms.nullToDefaultKey.replacements": "MB_NO:####",

    "transforms.replaceMbKey.type": "com.yokke.kafka.connect.smt.ReplaceValue$Key",
    "transforms.replaceMbKey.replacements": "MB_NO:###=>####",

    "transforms.nullToDefault.type": "com.yokke.kafka.connect.smt.ReplaceNullWithValue$Value",
    "transforms.nullToDefault.replacements": "REQ_DATE:-,RECEIVED_DATE:-",

    "transforms.replaceVal.type": "com.yokke.kafka.connect.smt.ReplaceValue$Value",
    "transforms.replaceVal.replacements": "QR_VAL1:N/A=>-"
  }
}
```

Penjelasan contoh (`pk.mode=record_key`, `MB_NO` bagian dari primary key):
- `dropMb` (`FilterRecord$Key`): record dengan `MB_NO` `822`, `503`, atau `564` di **key** dibuang dan tidak ditulis ke tabel. Letaknya paling awal.
- `nullToDefaultKey` (`ReplaceNullWithValue$Key`): `MB_NO` di **key** bernilai `NULL` menjadi `####`.
- `replaceMbKey` (`ReplaceValue$Key`): `MB_NO` di **key** bernilai `###` menjadi `####`.
- `nullToDefault` (`ReplaceNullWithValue$Value`): kolom biasa di **value** (`REQ_DATE`, `RECEIVED_DATE`) bernilai `NULL` menjadi `-`.
- `replaceVal` (`ReplaceValue$Value`): `QR_VAL1` di **value** bernilai `N/A` menjadi `-`.

Hapus transform yang tidak dibutuhkan, dan pastikan setiap alias di `transforms` punya baris `type` pasangannya. Ganti `com.yokke.kafka.connect.smt` dengan package sesuai source Anda.

Buat atau perbarui connector:

```bash
# buat baru
curl -X POST -H "Content-Type: application/json" \
  --data @sink-connector-example.json http://<connect-host>:8083/connectors

# update config connector yang sudah ada (isi hanya bagian "config")
curl -X PUT -H "Content-Type: application/json" \
  --data '{ ...isi bagian "config" saja... }' \
  http://<connect-host>:8083/connectors/<nama-connector>/config
```

### Beberapa kolom sekaligus

```properties
transforms.nullToDefault.replacements=MB_NO:###,CUST_NM:-,AMOUNT:0
```

### Digabung dengan SMT lain

SMT dieksekusi sesuai urutan di `transforms`. Jika ada SMT yang mengubah nama field (misalnya `ReplaceField` dengan `renames`), taruh SMT ini **setelahnya** dan gunakan nama field hasil rename.

```properties
transforms=rename,nullToDefault
transforms.rename.type=org.apache.kafka.connect.transforms.ReplaceField$Value
transforms.rename.renames=mb_no:MB_NO
transforms.nullToDefault.type=com.yokke.kafka.connect.smt.ReplaceNullWithValue$Value
transforms.nullToDefault.replacements=MB_NO:###
```

## 7b. Debezium CDC, `unwrap`, dan urutan `transforms`

### Apa itu CDC dan envelope Debezium

CDC (Change Data Capture) membaca perubahan data (insert, update, delete) dari log database lalu mengirimnya ke Kafka. Debezium mengirim setiap perubahan sebagai event dengan **envelope** bersarang:

```json
{
  "before": { "MB_NO": "###", "MID": "71000269964" },
  "after":  { "MB_NO": "####", "MID": "71000269964" },
  "source": { "ts_ms": 1700000000000, "table": "TBMCQRMAIN" },
  "op": "u",
  "ts_ms": 1700000000123
}
```

| Field | Isi |
|---|---|
| `before` | Isi baris sebelum perubahan (`null` untuk insert) |
| `after` | Isi baris sesudah perubahan (`null` untuk delete) |
| `source` | Metadata sumber (tabel, posisi log, waktu di database, dll.) |
| `op` | Jenis operasi: `c` create, `u` update, `d` delete, `r` snapshot read |
| `ts_ms` | Waktu Debezium memproses event |

Key record berisi kolom primary key tabel sumber dan tidak dibungkus envelope.

JDBC Sink butuh record **datar** (satu field = satu kolom), bukan envelope. Karena itu dipakai `unwrap`.

### Apa yang dilakukan `unwrap`

`io.debezium.transforms.ExtractNewRecordState` mengganti envelope dengan isi `after`, sehingga value menjadi datar:

```json
{ "MB_NO": "####", "MID": "71000269964" }
```

Opsi yang umum dipakai:

| Opsi | Fungsi |
|---|---|
| `add.fields` | Menambah metadata event ke value, dengan prefix `__` secara default. Contoh `source.ts_ms,op` menghasilkan field `__source_ts_ms` dan `__op` |
| `drop.tombstones` | `true` (default) membuang tombstone (record dengan value `null`) |
| `delete.handling.mode` | `drop` (default) membuang event delete, `rewrite` mempertahankannya dengan tambahan field `__deleted`, `none` mempertahankan record dengan value `null` |
| `delete.tombstone.handling.mode` | Pengganti baru untuk dua opsi di atas pada Debezium versi baru (nilai: `drop`, `tombstone`, `rewrite`, `rewrite-with-tombstone`, `delete-to-tombstone`). Opsi lama berstatus deprecated |

Satu operasi DELETE di database menghasilkan dua record di Kafka: event delete (`op = d`, berisi `before`) dan tombstone (key yang sama, value `null`). Dengan pengaturan default, keduanya dibuang oleh `unwrap`, sehingga baris di tabel tujuan tidak ikut terhapus.

Key record tidak diubah oleh `unwrap`.

### Apakah topic Anda berformat envelope atau datar?

Cek schema value di Schema Registry:

```bash
curl -s http://<schema-registry>:8081/subjects/<TOPIC>-value/versions/latest
```

- Ada field `before`, `after`, `source`, `op`: topic berformat **envelope**. `unwrap` wajib dan urutan di bawah sangat penting.
- Field langsung berupa kolom (misalnya `MID`, `MB_NO`): topic sudah **datar** (misalnya source connector sudah memakai `unwrap`). `unwrap` tidak diperlukan, dan metadata `__op`/`__source_ts_ms` tidak akan ada, sehingga SMT yang me-rename atau mengonversi field tersebut tidak punya sasaran.

### Di mana `unwrap` diletakkan di `transforms`?

SMT dijalankan **berurutan**: output SMT sebelumnya menjadi input SMT berikutnya. Aturan utamanya:

> Semua SMT yang bekerja pada **field di value** harus diletakkan **sesudah `unwrap`**. SMT yang bekerja pada **key** boleh sebelum atau sesudah `unwrap`.

| SMT | Posisi | Alasan |
|---|---|---|
| `unwrap` | Pertama untuk SMT yang menyentuh value | Sebelum `unwrap`, kolom ada di dalam `after`, bukan di level atas |
| `ReplaceField` (rename `__source_ts_ms` dan `__op`) | Sesudah `unwrap` | Field `__...` baru dibuat oleh `unwrap` lewat `add.fields` |
| `InsertField` (metadata Kafka) | Sesudah `unwrap` | Field disisipkan ke value datar. Kalau sebelum `unwrap`, field masuk ke envelope lalu hilang saat `after` diekstrak |
| `TimestampConverter` pada `TS_SRC` | Sesudah rename | Field `TS_SRC` baru ada setelah rename |
| `FilterRecord$Value`, `ReplaceValue$Value`, `ReplaceNullWithValue$Value` | Sesudah `unwrap` | SMT ini hanya memeriksa field di level atas value |
| `FilterRecord$Key`, `ReplaceValue$Key`, `ReplaceNullWithValue$Key` | Bebas, `FilterRecord$Key` paling awal | `unwrap` tidak mengubah key. Filter di awal membuang record sebelum diproses SMT lain |

### Apa yang terjadi kalau urutannya salah

SMT value yang diletakkan **sebelum** `unwrap` melihat field `before`, `after`, `source`, `op`, bukan `MB_NO`. Akibatnya:

- `FilterRecord$Value`: field tidak ditemukan, record dianggap tidak cocok, dan dengan `action=drop` semua record lolos.
- `ReplaceNullWithValue$Value` dan `ReplaceValue$Value`: field tidak ditemukan, tidak ada yang diubah.

Tidak ada pesan error. Hasilnya hanya "tidak berefek", sehingga mudah terlewat.

### Contoh urutan yang disarankan

`MB_NO` ada di **key** (`pk.mode=record_key`):

```properties
transforms=dropMb,unwrap,renameFields,addKafkaMeta,ts_ms,tzShift,nullToDefaultKey
transforms.dropMb.type=com.yokke.kafka.connect.smt.FilterRecord$Key
transforms.dropMb.field=MB_NO
transforms.dropMb.values=822,503,564
transforms.nullToDefaultKey.type=com.yokke.kafka.connect.smt.ReplaceNullWithValue$Key
transforms.nullToDefaultKey.replacements=MB_NO:####
```

`MB_NO` ada di **value**:

```properties
transforms=unwrap,dropMb,renameFields,addKafkaMeta,ts_ms,tzShift,nullToDefault
transforms.dropMb.type=com.yokke.kafka.connect.smt.FilterRecord$Value
transforms.dropMb.field=MB_NO
transforms.dropMb.values=822,503,564
transforms.nullToDefault.type=com.yokke.kafka.connect.smt.ReplaceNullWithValue$Value
transforms.nullToDefault.replacements=MB_NO:####
```

Aturan urutan secara ringkas: filter key, lalu `unwrap`, lalu filter value, lalu rename dan metadata, lalu konversi tipe/timezone, lalu penggantian `NULL` dan nilai. Kalau sebuah SMT memakai nama field hasil rename, letakkan SMT itu sesudah rename.

## 8. Pengujian

1. Kirim satu record uji ke topic dengan `MB_NO = null`.
2. Cek di tabel tujuan: kolom `MB_NO` harus berisi `###`.
3. Cek status connector:
   ```bash
   curl -s http://<connect-host>:8083/connectors/<nama-connector>/status
   ```

## 9. Troubleshooting

| Gejala | Penyebab | Solusi |
|---|---|---|
| `Could not transfer artifact ... Connect timed out` (saat build) | Mesin build tidak bisa mengakses Maven Central | Gunakan `build.sh`, build di mesin lain, atau atur proxy/mirror (bagian 5) |
| `javac tidak ditemukan` (saat `build.sh`) | Hanya terpasang JRE | Install JDK |
| `Class ... ReplaceNullWithValue$Value could not be found` | JAR belum terbaca worker | Pastikan JAR ada di folder di bawah `plugin.path` pada semua worker, lalu restart worker |
| `ConfigException: Missing required configuration "replacements"` | Parameter belum diisi | Tambahkan `transforms.<alias>.replacements` |
| `ConfigException ... Format harus FIELD:NILAI` | Format pasangan salah | Gunakan `FIELD:NILAI`, pisahkan beberapa pasangan dengan koma |
| `DataException: Nilai pengganti ... tidak cocok dengan tipe ...` | Nilai pengganti tidak bisa dikonversi ke tipe field | Sesuaikan nilai (misalnya field INT harus diisi angka) |
| `DataException: Logical type ... belum didukung` | Field bertipe Date/Time/Timestamp | Tidak didukung di versi ini |
| Nilai tetap `NULL` di database | Nama field tidak cocok (case-sensitive) atau field sudah di-rename SMT lain | Cek nama field di schema (Schema Registry) dan urutan `transforms` |
| Error NOT NULL / PK di database | Kolom lain yang null belum didaftarkan | Tambahkan kolom tersebut ke `replacements` |
| `cannot insert NULL into ... MB_NO` padahal `$Value` sudah dipasang | Field ada di **key** dan `pk.mode=record_key` | Pakai varian `$Key` (bagian 3c) |
| `FilterRecord`: record yang seharusnya dibuang tetap masuk tabel | Field ada di key tapi memakai `$Value` (atau sebaliknya), nama field salah, atau nilai tidak persis sama | Cek schema `-key` dan `-value`, pakai varian yang tepat, dan pastikan `trim=true` |
| SMT `$Value` tidak berefek pada topic Debezium | Dipasang sebelum `unwrap`, field masih berada di dalam `after` | Pindahkan sesudah `unwrap` (bagian 7b) |
| Tanda kutip `'` ikut masuk ke database | Nilai ditulis dengan kutip | Tulis tanpa kutip: `MB_NO:###` |
| `ReplaceValue`: nilai `###` tidak berubah | Nama field tidak cocok, nilai lama tidak persis sama (spasi, huruf besar/kecil), atau nilainya `NULL` | Cek nama field dan isi record; untuk `NULL` pakai `ReplaceNullWithValue` |
| `ConfigException ... Format harus FIELD:NILAI_LAMA=>NILAI_BARU` | Format aturan `ReplaceValue` salah | Gunakan `MB_NO:###=>####` |

## 10. Membersihkan hasil build

Salin JAR ke folder plugin terlebih dahulu, baru bersihkan:

```bash
# build Maven
mvn clean            # atau: rm -rf target

# build.sh
rm -rf build
```

`mvn clean package` dan `./build.sh` otomatis membersihkan hasil build sebelumnya, jadi langkah ini hanya untuk merapikan folder project.

## 11. Struktur project

```
replace-null-smt/
├── pom.xml
├── build.sh
├── README.md
├── sink-connector-example.json
└── src/main/java/com/yokke/kafka/connect/smt/
    ├── FilterRecord.java
    ├── ReplaceNullWithValue.java
    └── ReplaceValue.java
```

Folder `target/` (Maven) dan `build/` (`build.sh`) dibuat saat proses build.

## 12. Mengubah package atau nama class

Jika package `com.yokke.kafka.connect.smt` ingin diganti:
1. Ubah deklarasi `package` di `ReplaceNullWithValue.java`.
2. Pindahkan file ke struktur folder yang sesuai.
3. Ubah `groupId` di `pom.xml` bila perlu.
4. Ubah path `SRC` di `build.sh`.
5. Sesuaikan nilai `transforms.<alias>.type` di config connector.