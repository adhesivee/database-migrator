package nl.myndocs.database.migrator.database.query.option;

import nl.myndocs.database.migrator.definition.ForeignKey;

import java.util.Optional;

/**
 * Created by albert on 21-8-2017.
 */
public class ForeignKeyOptions {
    private final Optional<ForeignKey.CASCADE> onDelete;
    private final Optional<ForeignKey.CASCADE> onUpdate;

    public ForeignKeyOptions(Optional<ForeignKey.CASCADE> onDelete, Optional<ForeignKey.CASCADE> onUpdate) {
        if (onDelete == null) {
            throw new IllegalArgumentException("onDelete should not be null");
        }
        if (onUpdate == null) {
            throw new IllegalArgumentException("onUpdate should not be null");
        }

        this.onDelete = onDelete;
        this.onUpdate = onUpdate;
    }

    public Optional<ForeignKey.CASCADE> getOnDelete() {
        return onDelete;
    }

    public Optional<ForeignKey.CASCADE> getOnUpdate() {
        return onUpdate;
    }
}
