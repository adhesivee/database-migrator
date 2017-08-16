package nl.myndocs.database.migrator.definition;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;

public class Constraint {
    public enum TYPE {
        INDEX, UNIQUE
    }

    private String constraintName;
    private Optional<TYPE> type;
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
        return type;
    }

    public Collection<String> getColumnNames() {
        return columnNames;
    }

    public static class Builder {
        private String constraintName;
        private Optional<TYPE> type;
        private Collection<String> columnNames = new ArrayList<>();

        public Builder(String constraintName) {
            this.constraintName = constraintName;
        }

        public Builder(String constraintName, TYPE type) {
            this.constraintName = constraintName;
            this.type = Optional.ofNullable(type);
        }

        public Builder columns(String... columnNames) {
            this.columnNames = Arrays.asList(columnNames);

            return this;
        }

        public String getConstraintName() {
            return constraintName;
        }

        public Optional<TYPE> getType() {
            return type;
        }

        public Collection<String> getColumnNames() {
            return columnNames;
        }
    }
}
