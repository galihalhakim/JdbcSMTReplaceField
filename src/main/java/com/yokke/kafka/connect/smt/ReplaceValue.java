package com.yokke.kafka.connect.smt;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * SMT untuk mengganti NILAI LAMA tertentu pada sebuah field dengan NILAI BARU.
 * (Nilai NULL tidak diubah oleh transform ini; untuk NULL pakai ReplaceNullWithValue.)
 *
 * Contoh konfigurasi:
 *   transforms=replaceMb
 *   transforms.replaceMb.type=com.yokke.kafka.connect.smt.ReplaceValue$Value
 *   transforms.replaceMb.replacements=MB_NO:###=>####
 *
 * Format replacements: FIELD:NILAI_LAMA=>NILAI_BARU, dipisah koma untuk beberapa aturan.
 * Satu field boleh punya beberapa aturan (aturan pertama yang cocok yang dipakai):
 *   MB_NO:###=>####,MB_NO:--=>####
 */
public abstract class ReplaceValue<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String REPLACEMENTS_CONFIG = "replacements";
    private static final String ARROW = "=>";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(REPLACEMENTS_CONFIG,
                    ConfigDef.Type.LIST,
                    ConfigDef.NO_DEFAULT_VALUE,
                    ConfigDef.Importance.HIGH,
                    "Daftar aturan FIELD:NILAI_LAMA=>NILAI_BARU yang dipisah koma. "
                            + "Contoh: MB_NO:###=>####");

    /** field -> (nilai lama -> nilai baru) */
    private Map<String, Map<String, String>> rules;

    @Override
    public void configure(Map<String, ?> props) {
        AbstractConfig config = new AbstractConfig(CONFIG_DEF, props, false);
        List<String> pairs = config.getList(REPLACEMENTS_CONFIG);
        Map<String, Map<String, String>> parsed = new LinkedHashMap<>();
        for (String pair : pairs) {
            int colon = pair.indexOf(':');
            int arrow = colon > 0 ? pair.indexOf(ARROW, colon + 1) : -1;
            if (colon <= 0 || arrow < 0 || arrow == colon + 1) {
                throw new ConfigException(REPLACEMENTS_CONFIG, pair,
                        "Format harus FIELD:NILAI_LAMA=>NILAI_BARU, contoh MB_NO:###=>####");
            }
            String field = pair.substring(0, colon).trim();
            String from = pair.substring(colon + 1, arrow);
            String to = pair.substring(arrow + ARROW.length());
            parsed.computeIfAbsent(field, k -> new LinkedHashMap<>()).put(from, to);
        }
        if (parsed.isEmpty()) {
            throw new ConfigException(REPLACEMENTS_CONFIG, pairs, "Minimal satu aturan FIELD:LAMA=>BARU");
        }
        this.rules = parsed;
    }

    @Override
    @SuppressWarnings("unchecked")
    public R apply(R record) {
        Object value = operatingValue(record);
        if (value == null) {
            return record; // tombstone: biarkan apa adanya
        }

        Object updated;
        if (value instanceof Struct) {
            updated = replaceInStruct((Struct) value);
        } else if (value instanceof Map) {
            updated = replaceInMap((Map<String, Object>) value);
        } else {
            return record;
        }
        return newRecord(record, updated);
    }

    private Struct replaceInStruct(Struct original) {
        Schema schema = original.schema();
        Struct result = new Struct(schema);
        for (Field field : schema.fields()) {
            Object val = original.get(field);
            Map<String, String> fieldRules = rules.get(field.name());
            if (val != null && fieldRules != null) {
                val = mapValue(field, val, fieldRules);
            }
            result.put(field, val);
        }
        return result;
    }

    private Map<String, Object> replaceInMap(Map<String, Object> original) {
        Map<String, Object> result = new HashMap<>(original);
        for (Map.Entry<String, Map<String, String>> e : rules.entrySet()) {
            Object val = result.get(e.getKey());
            if (val == null) {
                continue;
            }
            String to = e.getValue().get(String.valueOf(val));
            if (to != null) {
                result.put(e.getKey(), to);
            }
        }
        return result;
    }

    /** Cari aturan yang cocok dengan nilai saat ini; kembalikan nilai baru atau nilai asli. */
    private static Object mapValue(Field field, Object val, Map<String, String> fieldRules) {
        for (Map.Entry<String, String> rule : fieldRules.entrySet()) {
            Object from;
            try {
                from = ReplaceNullWithValue.convert(field.schema(), rule.getKey(), field.name());
            } catch (DataException e) {
                continue; // nilai lama tidak bisa dikonversi ke tipe field, berarti tidak mungkin cocok
            }
            if (sameValue(val, from)) {
                return ReplaceNullWithValue.convert(field.schema(), rule.getValue(), field.name());
            }
        }
        return val;
    }

    private static boolean sameValue(Object a, Object b) {
        if (a instanceof BigDecimal && b instanceof BigDecimal) {
            return ((BigDecimal) a).compareTo((BigDecimal) b) == 0;
        }
        if (a instanceof byte[]) {
            a = ByteBuffer.wrap((byte[]) a);
        }
        return a.equals(b);
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
        // tidak ada resource yang perlu ditutup
    }

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Object updated);

    /** Transform untuk value record. */
    public static class Value<R extends ConnectRecord<R>> extends ReplaceValue<R> {
        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Object updated) {
            return record.newRecord(record.topic(), record.kafkaPartition(),
                    record.keySchema(), record.key(),
                    record.valueSchema(), updated, record.timestamp());
        }
    }

    /** Transform untuk key record. */
    public static class Key<R extends ConnectRecord<R>> extends ReplaceValue<R> {
        @Override
        protected Object operatingValue(R record) {
            return record.key();
        }

        @Override
        protected R newRecord(R record, Object updated) {
            return record.newRecord(record.topic(), record.kafkaPartition(),
                    record.keySchema(), updated,
                    record.valueSchema(), record.value(), record.timestamp());
        }
    }
}