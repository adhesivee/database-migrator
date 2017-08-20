package nl.myndocs.database.migrator.database.query.option;

import java.util.Optional;

/**
 * Created by albert on 20-8-2017.
 */
public class ChangeTypeOptions {
    private final Optional<Boolean> autoIncrement;
    private final Optional<Integer> columnSize;

    public ChangeTypeOptions(Boolean autoIncrement, Integer columnSize) {
        this.autoIncrement = Optional.ofNullable(autoIncrement);
        this.columnSize = Optional.ofNullable(columnSize);
    }

    public ChangeTypeOptions(Optional<Boolean> autoIncrement, Optional<Integer> columnSize) {
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

    public static ChangeTypeOptions ofSize(int size) {
        return new ChangeTypeOptions(null, size);
    }

    public static ChangeTypeOptions empty() {
        return new ChangeTypeOptions((Boolean) null, null);
    }
}
