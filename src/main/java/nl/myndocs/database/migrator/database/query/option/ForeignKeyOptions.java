package nl.myndocs.database.migrator.database.query.option;

import nl.myndocs.database.migrator.definition.ForeignKey;

import java.util.Optional;

/**
 * Created by albert on 21-8-2017.
 */
public class ForeignKeyOptions {
    private final ForeignKey.CASCADE onDelete;
    private final ForeignKey.CASCADE onUpdate;

    public ForeignKeyOptions(ForeignKey.CASCADE onDelete, ForeignKey.CASCADE onUpdate) {

        this.onDelete = onDelete;
        this.onUpdate = onUpdate;
    }

    public Optional<ForeignKey.CASCADE> getOnDelete() {
        return Optional.ofNullable(onDelete);
    }

    public Optional<ForeignKey.CASCADE> getOnUpdate() {
        return Optional.ofNullable(onUpdate);
    }
}
