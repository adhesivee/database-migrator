package nl.myndocs.database.migrator.database.query.option;

import nl.myndocs.database.migrator.definition.Column;

import java.util.Optional;

/**
 * Created by albert on 20-8-2017.
 */
public class ColumnOptions {
    private final String columnName;
    private final Column.TYPE columnType;
    private final Optional<Boolean> autoIncrement;
    private final Optional<Integer> columnSize;
    private final Optional<String> defaultValue;
    private final Optional<Boolean> isNotNull;
    private final Optional<Boolean> isPrimary;

    public ColumnOptions(String columnName, Column.TYPE columnType, Optional<Boolean> autoIncrement, Optional<Integer> columnSize, Optional<String> defaultValue, Optional<Boolean> isNotNull, Optional<Boolean> isPrimary) {
        this.columnName = columnName;
        this.columnType = columnType;
        this.autoIncrement = autoIncrement;
        this.columnSize = columnSize;
        this.defaultValue = defaultValue;
        this.isNotNull = isNotNull;
        this.isPrimary = isPrimary;
    }

    public String getColumnName() {
        return columnName;
    }

    public Column.TYPE getColumnType() {
        return columnType;
    }

    public Optional<Boolean> getAutoIncrement() {
        return autoIncrement;
    }

    public Optional<Integer> getColumnSize() {
        return columnSize;
    }

    public Optional<String> getDefaultValue() {
        return defaultValue;
    }

    public Optional<Boolean> getIsNotNull() {
        return isNotNull;
    }

    public Optional<Boolean> getIsPrimary() {
        return isPrimary;
    }
}
