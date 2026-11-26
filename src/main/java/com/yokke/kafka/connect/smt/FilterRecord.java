package com.yokke.kafka.connect.smt;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;

import java.math.BigDecimal;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * SMT untuk membuang (atau hanya meloloskan) record berdasarkan nilai sebuah field.
 * Record yang dibuang tidak akan sampai ke sink connector.
 *
 * Contoh: buang record yang MB_NO-nya 822, 503, atau 564 (field ada di key):
 *   transforms=dropMb
 *   transforms.dropMb.type=com.yokke.kafka.connect.smt.FilterRecord$Key
 *   transforms.dropMb.field=MB_NO
 *   transforms.dropMb.values=822,503,564
 *   transforms.dropMb.action=drop
 *
 * Parameter:
 *   field   : nama field (case-sensitive)
 *   values  : daftar nilai dipisah koma
 *   action  : drop (default) = buang record yang cocok; keep = hanya loloskan record yang cocok
 *   trim    : true (default) = spasi di awal/akhir nilai di record diabaikan saat membandingkan,
 *             sehingga '822 ' dan '822' dianggap sama
 *
 * Catatan: Kafka selalu memotong spasi pada item daftar di konfigurasi, jadi nilai di
 * "values" tidak bisa mengandung spasi di ujung. Untuk data CHAR berpadding, biarkan trim=true.
 */
public abstract class FilterRecord<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String FIELD_CONFIG = "field";
    public static final String VALUES_CONFIG = "values";
    public static final String ACTION_CONFIG = "action";
    public static final String TRIM_CONFIG = "trim";

    public static final String ACTION_DROP = "drop";
    public static final String ACTION_KEEP = "keep";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(FIELD_CONFIG,
                    ConfigDef.Type.STRING,
                    ConfigDef.NO_DEFAULT_VALUE,
                    new ConfigDef.NonEmptyString(),
                    ConfigDef.Importance.HIGH,
                    "Nama field yang diperiksa, contoh: MB_NO")
            .define(VALUES_CONFIG,
                    ConfigDef.Type.LIST,
                    ConfigDef.NO_DEFAULT_VALUE,
                    ConfigDef.Importance.HIGH,
                    "Daftar nilai yang dicocokkan, dipisah koma. Contoh: 822,503,564")
            .define(ACTION_CONFIG,
                    ConfigDef.Type.STRING,
                    ACTION_DROP,
                    ConfigDef.ValidString.in(ACTION_DROP, ACTION_KEEP),
                    ConfigDef.Importance.MEDIUM,
                    "drop = buang record yang cocok, keep = hanya loloskan record yang cocok")
            .define(TRIM_CONFIG,
                    ConfigDef.Type.BOOLEAN,
                    true,
                    ConfigDef.Importance.MEDIUM,
                    "Jika true, spasi di awal/akhir nilai pada record diabaikan saat membandingkan");

    private String fieldName;
    private Set<String> values;
    private boolean dropMatching;
    private boolean trim;

    @Override
    public void configure(Map<String, ?> props) {
        AbstractConfig config = new AbstractConfig(CONFIG_DEF, props, false);
        this.fieldName = config.getString(FIELD_CONFIG);
        this.trim = config.getBoolean(TRIM_CONFIG);
        this.dropMatching = ACTION_DROP.equals(config.getString(ACTION_CONFIG));

        List<String> list = config.getList(VALUES_CONFIG);
        Set<String> parsed = new HashSet<>();
        for (String v : list) {
            parsed.add(trim ? v.trim() : v);
        }
        if (parsed.isEmpty()) {
            throw new ConfigException(VALUES_CONFIG, list, "Minimal satu nilai");
        }
        this.values = parsed;
    }

    @Override
    public R apply(R record) {
        Object target = operatingValue(record);
        if (target == null) {
            return record; // key/value null: tidak ada yang bisa diperiksa
        }

        Object fieldValue;
        if (target instanceof Struct) {
            Struct struct = (Struct) target;
            Field f = struct.schema().field(fieldName);
            fieldValue = (f == null) ? null : struct.get(f);
        } else if (target instanceof Map) {
            fieldValue = ((Map<?, ?>) target).get(fieldName);
        } else {
            return record; // bukan struct/map, tidak ada field yang bisa diperiksa
        }

        boolean matches = false;
        if (fieldValue != null) {
            String s = asString(fieldValue);
            matches = values.contains(trim ? s.trim() : s);
        }

        boolean drop = dropMatching ? matches : !matches;
        return drop ? null : record;
    }

    private static String asString(Object v) {
        if (v instanceof BigDecimal) {
            return ((BigDecimal) v).toPlainString();
        }
        return String.valueOf(v);
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

    /** Memeriksa field di value record. */
    public static class Value<R extends ConnectRecord<R>> extends FilterRecord<R> {
        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }
    }

    /** Memeriksa field di key record (pakai ini jika field adalah bagian primary key / pk.mode=record_key). */
    public static class Key<R extends ConnectRecord<R>> extends FilterRecord<R> {
        @Override
        protected Object operatingValue(R record) {
            return record.key();
        }
    }
}