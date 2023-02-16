package nl.myndocs.database.migrator.definition;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;

import nl.myndocs.database.migrator.database.exception.InvalidSpecException;

/**
 * Created by albert on 13-8-2017.
 */
public class ForeignKey {
    /**
     * @author Mikhail Mikhailov
     * Foreign key cascade action type.
     */
    public enum CASCADE {
        CASCADE,
        RESTRICT,
        SET_DEFAULT,
        SET_NULL,
        NO_ACTION
    }

    private String foreignTable;
    private Collection<String> foreignKeys;
    private CASCADE updateCascade;
    private CASCADE deleteCascade;

    private ForeignKey(Builder builder) {

        Objects.requireNonNull(builder.foreignTable, "foreignTable must not be null");
        Objects.requireNonNull(builder.foreignKeys, "foreignKeys must not be null");

        if (builder.foreignKeys.isEmpty()) {
            throw new InvalidSpecException("foreignKeys must not be empty");
        }

        foreignTable = builder.foreignTable;
        foreignKeys = builder.foreignKeys;
        updateCascade = builder.updateCascade;
        deleteCascade = builder.deleteCascade;
    }

    public String getForeignTable() {
        return foreignTable;
    }

    public Collection<String> getForeignKeys() {
        return foreignKeys;
    }

    public CASCADE getUpdateCascade() {
        return updateCascade;
    }

    public CASCADE getDeleteCascade() {
        return deleteCascade;
    }

    public static class Builder {
        private String foreignTable;
        private Collection<String> foreignKeys = new ArrayList<>();
        private CASCADE deleteCascade;
        private CASCADE updateCascade;

        public Builder(String foreignTable, Collection<String> foreignKeys) {
            this.foreignTable = foreignTable;
            this.foreignKeys = foreignKeys;
        }

        public Builder(String foreignTable, String... foreignKeys) {
            this.foreignTable = foreignTable;
            this.foreignKeys.addAll(Arrays.asList(foreignKeys));
        }

        public Builder() {
            super();
        }

        public Builder foreignTable(String foreignTable) {
            this.foreignTable = foreignTable;
            return this;
        }

        public Builder foreignKeys(String... foreignKeys) {
            this.foreignKeys.addAll(Arrays.asList(foreignKeys));
            return this;
        }

        public Builder cascadeDelete(CASCADE cascade) {
            deleteCascade = cascade;
            return this;
        }

        public Builder cascadeUpdate(CASCADE cascade) {
            updateCascade = cascade;
            return this;
        }

        public ForeignKey build() {
            return new ForeignKey(this);
        }
    }
}
