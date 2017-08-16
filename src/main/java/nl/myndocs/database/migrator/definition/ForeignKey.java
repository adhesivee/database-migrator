package nl.myndocs.database.migrator.definition;

import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;

/**
 * Created by albert on 13-8-2017.
 */
public class ForeignKey {
    public enum CASCADE {
        CASCADE, RESTRICT, SET_DEFAULT, SET_NULL, NO_ACTION
    }

    private String constraintName;
    private String foreignTable;
    private Collection<String> localKeys;
    private Collection<String> foreignKeys;
    private Optional<CASCADE> updateCascade = Optional.empty();
    private Optional<CASCADE> deleteCascade = Optional.empty();

    private ForeignKey(Builder builder) {
        if (builder.getLocalKeys().size() != builder.getForeignKeys().size()) {
            throw new RuntimeException("Foreign and local keys size should match");
        }

        constraintName = builder.getConstraintName();
        foreignTable = builder.getForeignTable();
        localKeys = builder.getLocalKeys();
        foreignKeys = builder.getForeignKeys();
        updateCascade = builder.getUpdateCascade();
        deleteCascade = builder.getDeleteCascade();
    }

    public String getForeignTable() {
        return foreignTable;
    }

    public Collection<String> getLocalKeys() {
        return localKeys;
    }

    public Collection<String> getForeignKeys() {
        return foreignKeys;
    }

    public Optional<CASCADE> getUpdateCascade() {
        return updateCascade;
    }

    public Optional<CASCADE> getDeleteCascade() {
        return deleteCascade;
    }

    public String getConstraintName() {
        return constraintName;
    }

    public static class Builder {
        private String constraintName;
        private String foreignTable;
        private Collection<String> localKeys;
        private Collection<String> foreignKeys;
        private Optional<CASCADE> deleteCascade;
        private Optional<CASCADE> updateCascade;

        public Builder(String constraintName, String foreignTable, Collection<String> localKeys, Collection<String> foreignKeys) {
            this.constraintName = constraintName;
            this.foreignTable = foreignTable;
            this.localKeys = localKeys;
            this.foreignKeys = foreignKeys;
        }

        public Builder(String constraintName, String foreignTable, String localKey, String foreignKey) {
            this(constraintName, foreignTable, Arrays.asList(localKey), Arrays.asList(foreignKey));
        }

        public Builder cascadeDelete(CASCADE cascade) {
            deleteCascade = Optional.ofNullable(cascade);

            return this;
        }

        public Builder cascadeUpdate(CASCADE cascade) {
            updateCascade = Optional.ofNullable(cascade);

            return this;
        }

        public String getForeignTable() {
            return foreignTable;
        }

        public Collection<String> getLocalKeys() {
            return localKeys;
        }

        public Collection<String> getForeignKeys() {
            return foreignKeys;
        }

        public Optional<CASCADE> getDeleteCascade() {
            return deleteCascade;
        }

        public Optional<CASCADE> getUpdateCascade() {
            return updateCascade;
        }

        public ForeignKey build() {
            return new ForeignKey(this);
        }

        public String getConstraintName() {
            return constraintName;
        }
    }
}
