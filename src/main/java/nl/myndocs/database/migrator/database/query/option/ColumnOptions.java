package nl.myndocs.database.migrator.database.query.option;

import nl.myndocs.database.migrator.definition.Column;
import nl.myndocs.database.migrator.util.Assert;

import java.util.Optional;

/**
 * Created by albert on 20-8-2017.
 */
public class ColumnOptions {
    private final String columnName;
    private final Column.TYPE columnType;
    private final Boolean autoIncrement;
    private final Integer columnSize;
    private final String defaultValue;
    private final Boolean isNotNull;
    private final Boolean isPrimary;

    private ColumnOptions(Builder builder) {
        columnName = builder.getColumnName();
        columnType = builder.getColumnType();
        autoIncrement = builder.getAutoIncrement();
        columnSize = builder.getColumnSize();
        defaultValue = builder.getDefaultValue();
        isNotNull = builder.getNotNull();
        isPrimary = builder.getPrimary();
    }

    public String getColumnName() {
        return columnName;
    }

    public Column.TYPE getColumnType() {
        return columnType;
    }

    public Optional<Boolean> getAutoIncrement() {
        return Optional.ofNullable(autoIncrement);
    }

    public Optional<Integer> getColumnSize() {
        return Optional.ofNullable(columnSize);
    }

    public Optional<String> getDefaultValue() {
        return Optional.ofNullable(defaultValue);
    }

    public Optional<Boolean> getIsNotNull() {
        return Optional.ofNullable(isNotNull);
    }

    public Optional<Boolean> getIsPrimary() {
        return Optional.ofNullable(isPrimary);
    }

    public static class Builder {
        private final String columnName;
        private final Column.TYPE columnType;
        private Boolean autoIncrement;
        private Integer columnSize;
        private String defaultValue;
        private Boolean isNotNull;
        private Boolean isPrimary;

        public Builder(String columnName, Column.TYPE columnType) {
            Assert.notNull(columnName, "columnName must not be null");
            Assert.notNull(columnType, "columnType must not be null");

            this.columnName = columnName;
            this.columnType = columnType;
        }

        public String getColumnName() {
            return columnName;
        }

        public Column.TYPE getColumnType() {
            return columnType;
        }

        public Boolean getAutoIncrement() {
            return autoIncrement;
        }

        public Builder setAutoIncrement(Boolean autoIncrement) {
            this.autoIncrement = autoIncrement;

            return this;
        }

        public Integer getColumnSize() {
            return columnSize;
        }

        public Builder setColumnSize(Integer columnSize) {
            this.columnSize = columnSize;

            return this;
        }

        public String getDefaultValue() {
            return defaultValue;
        }

        public Builder setDefaultValue(String defaultValue) {
            this.defaultValue = defaultValue;

            return this;
        }

        public Boolean getNotNull() {
            return isNotNull;
        }

        public Builder setNotNull(Boolean notNull) {
            isNotNull = notNull;

            return this;
        }

        public Boolean getPrimary() {
            return isPrimary;
        }

        public Builder setPrimary(Boolean primary) {
            isPrimary = primary;

            return this;
        }

        public ColumnOptions build() {
            return new ColumnOptions(this);
        }
    }
}
