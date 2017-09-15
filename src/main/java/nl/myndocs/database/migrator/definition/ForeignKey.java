package nl.myndocs.database.migrator.definition;

import nl.myndocs.database.migrator.util.Assert;

import java.util.Arrays;
import java.util.Collection;

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
    private CASCADE updateCascade;
    private CASCADE deleteCascade;

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

    public CASCADE getUpdateCascade() {
        return updateCascade;
    }

    public CASCADE getDeleteCascade() {
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
        private CASCADE deleteCascade;
        private CASCADE updateCascade;

        public Builder(String constraintName, String foreignTable, Collection<String> localKeys, Collection<String> foreignKeys) {
            Assert.notNull(constraintName, "constraintName must not be null");
            Assert.notNull(foreignTable, "foreignTable must not be null");
            Assert.notNull(localKeys, "localKeys must not be null");
            Assert.notNull(foreignKeys, "foreignKeys must not be null");

            this.constraintName = constraintName;
            this.foreignTable = foreignTable;
            this.localKeys = localKeys;
            this.foreignKeys = foreignKeys;
        }

        public Builder(String constraintName, String foreignTable, String localKey, String foreignKey) {
            this(constraintName, foreignTable, Arrays.asList(localKey), Arrays.asList(foreignKey));
        }

        public Builder cascadeDelete(CASCADE cascade) {
            deleteCascade = cascade;

            return this;
        }

        public Builder cascadeUpdate(CASCADE cascade) {
            updateCascade = cascade;

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

        public CASCADE getDeleteCascade() {
            return deleteCascade;
        }

        public CASCADE getUpdateCascade() {
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
