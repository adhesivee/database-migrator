package nl.myndocs.database.migrator.definition;

import nl.myndocs.database.migrator.util.Assert;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;

public class Constraint {
    public enum TYPE {
        INDEX, UNIQUE
    }

    private String constraintName;
    private TYPE type;
    private Collection<String> columnNames = new ArrayList<>();

    private Constraint(Builder builder) {
        constraintName = builder.getConstraintName();
        type = builder.getType();
        columnNames = builder.getColumnNames();
    }

    public String getConstraintName() {
        return constraintName;
    }

    public Optional<TYPE> getType() {
        return Optional.ofNullable(type);
    }

    public Collection<String> getColumnNames() {
        return columnNames;
    }

    public static class Builder {
        private String constraintName;
        private TYPE type;
        private Collection<String> columnNames = new ArrayList<>();

        public Builder(String constraintName, TYPE type, Collection<String> columnNames) {
            Assert.notNull(constraintName, "constraintName must not be null");
            Assert.notNull(type, "type must not be null");
            Assert.notNull(columnNames, "columnNames must not be null");

            this.constraintName = constraintName;
            this.type = type;
            this.columnNames = columnNames;
        }

        public Builder columns(String... columnNames) {
            this.columnNames = Arrays.asList(columnNames);

            return this;
        }

        public String getConstraintName() {
            return constraintName;
        }

        public TYPE getType() {
            return type;
        }

        public Collection<String> getColumnNames() {
            return columnNames;
        }

        public Constraint build() {
            return new Constraint(this);
        }
    }
}
