package nl.myndocs.database.migrator.definition;

import java.util.Objects;

/**
 * Created by albert on 13-8-2017.
 */
public class Column {
    public enum TYPE {
        SMALL_INTEGER,
        INTEGER,
        BIG_INTEGER,
        CHAR,
        VARCHAR,
        TEXT,
        UUID,
        DATE,
        TIME,
        TIMESTAMP,
        TIMESTAMPTZ,
        BOOLEAN,
        CLOB,
        BLOB,
        UDT
    }

    private final TYPE type;
    private final String columnName;
    private final Boolean primary;
    private final Boolean autoIncrement;
    private final Boolean isNotNull;
    private final Boolean isNull;
    private final Integer size;
    private final String defaultValue;
    private final String rename;
    private final String udt;

    private Column(Builder builder) {

        Objects.requireNonNull(builder.columnName, "columnName must not be null");
        Objects.requireNonNull(builder.type, "New column types should have a type");

        columnName = builder.columnName;
        primary = builder.primary;
        autoIncrement = builder.autoIncrement;
        isNotNull = builder.notNull;
        isNull = builder.isNull;
        type = builder.type;
        size = builder.size;
        defaultValue = builder.defaultValue;
        rename = builder.rename;
        udt = builder.udt;
    }

    public String getColumnName() {
        return columnName;
    }

    public Boolean getPrimary() {
        return primary;
    }

    public String getRename() {
        return rename;
    }

    public Boolean getAutoIncrement() {
        return autoIncrement;
    }

    public Boolean getIsNotNull() {
        return isNotNull;
    }

    public Boolean getIsNull() {
        return isNull;
    }

    public TYPE getType() {
        return type;
    }

    public Integer getSize() {
        return size;
    }

    public String getDefaultValue() {
        return defaultValue;
    }
    /**
     * @return the udt
     */
    public String getUDT() {
        return udt;
    }

    public static class Builder {
        private String columnName;
        private Boolean primary;
        private Boolean autoIncrement;
        private TYPE type;
        private Boolean notNull;
        private Boolean isNull;
        private Integer size;
        private String defaultValue;
        private String rename;
        private String udt;

        public Builder(String columnName, Column.TYPE type) {
            this.columnName = columnName;
            this.type = type;
        }

        public Builder(String columnName) {
            this.columnName = columnName;
        }

        public Builder rename(String name) {
            rename = name;
            return this;
        }

        public Builder type(Column.TYPE type) {
            this.type = type;
            return this;
        }

        public Builder udt(String udt) {
            this.udt = udt;
            return this;
        }

        public Builder primary(Boolean primary) {
            this.primary = primary;
            return this;
        }

        public Builder autoIncrement(Boolean autoIncrement) {
            this.autoIncrement = autoIncrement;
            return this;
        }

        public Builder defaultValue(String value) {
            this.defaultValue = value;
            return this;
        }

        public Builder size(Integer size) {
            this.size = size;
            return this;
        }

        public Builder notNull(Boolean notNull) {
            this.notNull = notNull;
            return this;
        }

        public Builder isNull(Boolean isNull) {
            this.isNull = isNull;
            return this;
        }

        public Column build() {
            return new Column(this);
        }
    }
}
