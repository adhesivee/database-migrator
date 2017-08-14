package nl.myndocs.database.migrator.definition;

/**
 * Created by albert on 13-8-2017.
 */
public class Column {
    public enum TYPE {
        INTEGER, CHAR, VARCHAR, UUID;
    }

    private String columnName;
    private boolean primary = false;
    private boolean autoIncrement = false;
    private boolean isNotNull = false;
    private TYPE type;
    private Integer size;
    private String defaultValue;

    private Column(Builder builder) {
        columnName = builder.getColumnName();
        primary = builder.isPrimary();
        autoIncrement = builder.isAutoIncrement();
        isNotNull = builder.isNotNull();
        type = builder.getType();
        size = builder.getSize();
        defaultValue = builder.getDefaultValue();
    }

    public String getColumnName() {
        return columnName;
    }

    public boolean isPrimary() {
        return primary;
    }

    public boolean isAutoIncrement() {
        return autoIncrement;
    }

    public TYPE getType() {
        return type;
    }

    public boolean isNotNull() {
        return isNotNull;
    }

    public Integer getSize() {
        return size;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public static class Builder {
        private String columnName;
        private boolean primary = false;
        private boolean autoIncrement = false;
        private TYPE type;
        private boolean notNull;
        private Integer size = null;
        private String defaultValue;

        public Builder(String columnName, Column.TYPE type) {
            this.columnName = columnName;
            this.type = type;
        }

        public Builder primary(boolean primary) {
            this.primary = primary;

            return this;
        }

        public Builder autoIncrement(boolean autoIncrement) {
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

        public Builder notNull(boolean notNull) {
            this.notNull = notNull;

            return this;
        }
        public String getColumnName() {
            return columnName;
        }

        public boolean isPrimary() {
            return primary;
        }

        public boolean isAutoIncrement() {
            return autoIncrement;
        }

        public boolean isNotNull() {
            return notNull;
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

        public Column build() {
            return new Column(this);
        }
    }
}
