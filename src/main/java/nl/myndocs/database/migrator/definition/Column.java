package nl.myndocs.database.migrator.definition;

import java.util.Optional;

/**
 * Created by albert on 13-8-2017.
 */
public class Column {
    public enum TYPE {
        INTEGER, CHAR, VARCHAR, UUID, DATE, TIME, TIMESTAMP
    }

    private String columnName;
    private Optional<Boolean> primary;
    private Optional<Boolean> autoIncrement;
    private Optional<Boolean> isNotNull;
    private Optional<TYPE> type;
    private Optional<Integer> size;
    private Optional<String> defaultValue;
    private Optional<String> rename;

    private Column(Builder builder) {
        columnName = builder.getColumnName();
        primary = builder.getPrimary();
        autoIncrement = builder.getAutoIncrement();
        isNotNull = builder.getNotNull();
        type = builder.getType();
        size = builder.getSize();
        defaultValue = builder.getDefaultValue();
        rename = builder.getRename();
    }

    public String getColumnName() {
        return columnName;
    }

    public Optional<Boolean> getPrimary() {
        return primary;
    }

    public Optional<String> getRename() {
        return rename;
    }

    public Optional<Boolean> getAutoIncrement() {
        return autoIncrement;
    }

    public Optional<Boolean> getIsNotNull() {
        return isNotNull;
    }

    public Optional<TYPE> getType() {
        return type;
    }

    public Optional<Integer> getSize() {
        return size;
    }

    public Optional<String> getDefaultValue() {
        return defaultValue;
    }

    public static class Builder {
        private String columnName;
        private Optional<Boolean> primary = Optional.empty();
        private Optional<Boolean> autoIncrement = Optional.empty();
        private Optional<TYPE> type = Optional.empty();
        private Optional<Boolean> notNull = Optional.empty();
        private Optional<Integer> size = Optional.empty();
        private Optional<String> defaultValue = Optional.empty();
        private Optional<String> rename = Optional.empty();

        public Builder(String columnName, Column.TYPE type) {
            this.columnName = columnName;
            this.type = Optional.of(type);
        }

        public Builder(String columnName) {
            this.columnName = columnName;
        }

        public Builder rename(String name) {
            rename = Optional.ofNullable(name);

            return this;
        }

        public Builder type(Column.TYPE type) {
            this.type = Optional.ofNullable(type);

            return this;
        }

        public Builder primary(Boolean primary) {
            this.primary = Optional.ofNullable(primary);

            return this;
        }

        public Builder autoIncrement(Boolean autoIncrement) {
            this.autoIncrement = Optional.ofNullable(autoIncrement);

            return this;
        }

        public Builder defaultValue(String value) {
            this.defaultValue = Optional.ofNullable(value);

            return this;
        }

        public Builder size(Integer size) {
            this.size = Optional.ofNullable(size);

            return this;
        }

        public Builder notNull(Boolean notNull) {
            this.notNull = Optional.ofNullable(notNull);

            return this;
        }

        public String getColumnName() {
            return columnName;
        }

        public Optional<String> getRename() {
            return rename;
        }

        public Optional<Boolean> getPrimary() {
            return primary;
        }

        public Optional<Boolean> getAutoIncrement() {
            return autoIncrement;
        }

        public Optional<TYPE> getType() {
            return type;
        }

        public Optional<Boolean> getNotNull() {
            return notNull;
        }

        public Optional<Integer> getSize() {
            return size;
        }

        public Optional<String> getDefaultValue() {
            return defaultValue;
        }

        public Column build() {
            return new Column(this);
        }
    }
}
