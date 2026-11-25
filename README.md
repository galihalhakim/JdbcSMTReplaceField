# jdbc-JdbcSMTReplaceField

Kumpulan **Single Message Transform (SMT)** untuk Kafka Connect:

1. `ReplaceNullWithValue`: mengganti nilai `NULL` pada field tertentu dengan nilai yang ditentukan.
2. `ReplaceValue`: mengganti nilai tertentu pada sebuah field dengan nilai lain (misalnya `###` menjadi `####`).

Keduanya dibuat untuk dipakai bersama **JDBC Sink Connector** pada Confluent Platform 7.9.x (Apache Kafka 3.9.x).

Contoh: pada topic, `MB_NO = NULL` diubah menjadi `MB_NO = '###'` sebelum ditulis ke database.

Artefak hasil build: `jdbc-JdbcSMTReplaceField-1.0.jar`

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

## 4. Prasyarat build

| Cara build | Kebutuhan |
|---|---|
| `build.sh` (tanpa Maven) | JDK 11 atau lebih baru (`javac`), instalasi Confluent di mesin build (untuk JAR `connect-api` dan `kafka-clients`) |
| Maven | JDK 11 atau lebih baru, Maven 3.6+, akses ke Maven Central atau repository internal |

Runtime Connect memakai Java 21. JAR dikompilasi dengan target Java 11 sehingga kompatibel.

## 5. Build

Masuk ke folder project terlebih dahulu:

```bash
cd JdbcSMTReplaceField
```

### Opsi A: `build.sh` (disarankan, tanpa Maven dan tanpa internet)

```bash
./build.sh                                # default lib: /opt/confluent/share/java/kafka
./build.sh /path/ke/lib/kafka             # jika lokasi lib berbeda
```

Hasil: `build/jdbc-JdbcSMTReplaceField-1.0.jar`

Script memakai `connect-api-*.jar` dan `kafka-clients-*.jar` dari folder lib Confluent, sehingga versinya otomatis sama dengan runtime Connect.

### Opsi B: Maven

```bash
mvn clean package
```

Hasil: `target/jdbc-JdbcSMTReplaceField-1.0.jar`

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
   mkdir -p /opt/confluent/confluent-hub-components/jdbc-JdbcSMTReplaceField
   cp build/jdbc-JdbcSMTReplaceField-1.0.jar /opt/confluent/confluent-hub-components/jdbc-JdbcSMTReplaceField/
   # jika build dengan Maven: cp target/jdbc-JdbcSMTReplaceField-1.0.jar ...
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

    "transforms": "nullToDefaultKey,replaceMbKey,nullToDefault,replaceVal",

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
JdbcSMTReplaceField/
├── pom.xml
├── build.sh
├── README.md
├── sink-connector-example.json
└── src/main/java/com/yokke/kafka/connect/smt/
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