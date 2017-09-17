package nl.myndocs.database.migrator.database.query.option;

import java.util.Optional;

/**
 * Created by albert on 20-8-2017.
 */
public class ChangeTypeOptions {
    private final Boolean autoIncrement;
    private final Integer columnSize;

    public ChangeTypeOptions(Boolean autoIncrement, Integer columnSize) {
        this.autoIncrement = autoIncrement;
        this.columnSize = columnSize;
    }

    public ChangeTypeOptions() {
        this(null, null);
    }

    public Optional<Boolean> getAutoIncrement() {
        return Optional.ofNullable(autoIncrement);
    }

    public Optional<Integer> getColumnSize() {
        return Optional.ofNullable(columnSize);
    }

    public static ChangeTypeOptions ofSize(int size) {
        return new ChangeTypeOptions(null, size);
    }
}
