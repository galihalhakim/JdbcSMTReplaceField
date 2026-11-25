package com.yokke.kafka.connect.smt;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * SMT untuk mengganti nilai NULL pada field tertentu dengan nilai yang diinginkan.
 *
 * Contoh konfigurasi:
 *   transforms=nullToDefault
 *   transforms.nullToDefault.type=com.yokke.kafka.connect.smt.ReplaceNullWithValue$Value
 *   transforms.nullToDefault.replacements=MB_NO:###,CUST_NM:-
 *
 * Format replacements: FIELD:NILAI,FIELD2:NILAI2 (dipisah koma; nilai tidak boleh mengandung koma).
 * Nilai otomatis dikonversi sesuai tipe field di schema (string, int, long, double, boolean, decimal).
 */
public abstract class ReplaceNullWithValue<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String REPLACEMENTS_CONFIG = "replacements";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(REPLACEMENTS_CONFIG,
                    ConfigDef.Type.LIST,
                    ConfigDef.NO_DEFAULT_VALUE,
                    ConfigDef.Importance.HIGH,
                    "Daftar pasangan FIELD:NILAI yang dipisah koma. Jika FIELD bernilai null, "
                            + "diganti dengan NILAI. Contoh: MB_NO:###,CUST_NM:-");

    private Map<String, String> replacements;

    @Override
    public void configure(Map<String, ?> props) {
        AbstractConfig config = new AbstractConfig(CONFIG_DEF, props, false);
        List<String> pairs = config.getList(REPLACEMENTS_CONFIG);
        Map<String, String> parsed = new LinkedHashMap<>();
        for (String pair : pairs) {
            int idx = pair.indexOf(':');
            if (idx <= 0) {
                throw new ConfigException(REPLACEMENTS_CONFIG, pair,
                        "Format harus FIELD:NILAI, contoh MB_NO:###");
            }
            parsed.put(pair.substring(0, idx).trim(), pair.substring(idx + 1));
        }
        if (parsed.isEmpty()) {
            throw new ConfigException(REPLACEMENTS_CONFIG, pairs, "Minimal satu pasangan FIELD:NILAI");
        }
        this.replacements = parsed;
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
            return record; // bukan struct/map, tidak ada yang diubah
        }
        return newRecord(record, updated);
    }

    private Struct replaceInStruct(Struct original) {
        Schema schema = original.schema();
        Struct result = new Struct(schema);
        for (Field field : schema.fields()) {
            Object val = original.get(field);
            if (val == null && replacements.containsKey(field.name())) {
                val = convert(field.schema(), replacements.get(field.name()), field.name());
            }
            result.put(field, val);
        }
        return result;
    }

    private Map<String, Object> replaceInMap(Map<String, Object> original) {
        Map<String, Object> result = new HashMap<>(original);
        for (Map.Entry<String, String> e : replacements.entrySet()) {
            if (result.get(e.getKey()) == null) {
                result.put(e.getKey(), e.getValue());
            }
        }
        return result;
    }

    /** Konversi string replacement ke tipe Java sesuai schema field. */
    static Object convert(Schema schema, String raw, String fieldName) {
        try {
            if (Decimal.LOGICAL_NAME.equals(schema.name())) {
                int scale = Integer.parseInt(schema.parameters().get(Decimal.SCALE_FIELD));
                return new BigDecimal(raw).setScale(scale);
            }
            if (schema.name() != null) {
                throw new DataException("Logical type " + schema.name() + " pada field "
                        + fieldName + " belum didukung oleh ReplaceNullWithValue");
            }
            switch (schema.type()) {
                case STRING:
                    return raw;
                case INT8:
                    return Byte.parseByte(raw);
                case INT16:
                    return Short.parseShort(raw);
                case INT32:
                    return Integer.parseInt(raw);
                case INT64:
                    return Long.parseLong(raw);
                case FLOAT32:
                    return Float.parseFloat(raw);
                case FLOAT64:
                    return Double.parseDouble(raw);
                case BOOLEAN:
                    return Boolean.parseBoolean(raw);
                case BYTES:
                    return ByteBuffer.wrap(raw.getBytes(StandardCharsets.UTF_8));
                default:
                    throw new DataException("Tipe " + schema.type() + " pada field " + fieldName
                            + " tidak didukung oleh ReplaceNullWithValue");
            }
        } catch (NumberFormatException | ArithmeticException e) {
            throw new DataException("Nilai pengganti '" + raw + "' tidak cocok dengan tipe "
                    + schema.type() + " pada field " + fieldName, e);
        }
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

    /** Transform untuk value record (yang dipakai JDBC sink untuk kolom biasa). */
    public static class Value<R extends ConnectRecord<R>> extends ReplaceNullWithValue<R> {
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

    /** Transform untuk key record (jika kolom ada di key / pk.mode=record_key). */
    public static class Key<R extends ConnectRecord<R>> extends ReplaceNullWithValue<R> {
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
