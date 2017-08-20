package nl.myndocs.database.migrator.database.query.option;

import java.util.Optional;

/**
 * Created by albert on 20-8-2017.
 */
public class AlterColumnOptions {
    private final Optional<Boolean> autoIncrement;
    private final Optional<Integer> columnSize;

    public AlterColumnOptions(Boolean autoIncrement, Integer columnSize) {
        this.autoIncrement = Optional.ofNullable(autoIncrement);
        this.columnSize = Optional.ofNullable(columnSize);
    }

    public AlterColumnOptions(Optional<Boolean> autoIncrement, Optional<Integer> columnSize) {
        if (autoIncrement == null) {
            throw new IllegalArgumentException("autoIncrement should not be null");
        }
        if (columnSize == null) {
            throw new IllegalArgumentException("columnSize should not be null");
        }

        this.autoIncrement = autoIncrement;
        this.columnSize = columnSize;
    }

    public Optional<Boolean> getAutoIncrement() {
        return autoIncrement;
    }

    public Optional<Integer> getColumnSize() {
        return columnSize;
    }

    public static AlterColumnOptions ofSize(int size) {
        return new AlterColumnOptions(null, size);
    }

    public static AlterColumnOptions empty() {
        return new AlterColumnOptions((Boolean) null, null);
    }
}
