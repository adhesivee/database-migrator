package nl.myndocs.database.migrator.definition;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import java.util.function.Consumer;

import nl.myndocs.database.migrator.database.exception.InvalidSpecException;

/**
 * {@link Index}
 */
public class Constraint {
    /**
     * The constraint type.
     */
    public enum TYPE {
        PRIMARY_KEY,
        FOREIGN_KEY,
        UNIQUE,
        CHECK
    }

    private String constraintName;
    private TYPE type;
    private Collection<String> columnNames;
    private Collection<String> includeNames;
    private ForeignKey foreignKey;
    private String checkExpression;

    private Constraint(Builder builder) {

        Objects.requireNonNull(builder.constraintName, "constraintName must not be null");
        Objects.requireNonNull(builder.type, "type must not be null");
        Objects.requireNonNull(builder.columnNames, "columnNames must not be null");

        if (builder.columnNames.isEmpty()) {
            throw new InvalidSpecException("columnNames must not be empty");
        }

        constraintName = builder.constraintName;
        type = builder.type;
        columnNames = builder.columnNames;
        includeNames = builder.includeNames;

        if (builder.type == TYPE.FOREIGN_KEY) {

            Objects.requireNonNull(builder.foreignKey, "foreignKey must not be null for foreign key type constraint");

            foreignKey = builder.foreignKey.build();
            if (columnNames.size() != foreignKey.getForeignKeys().size()) {
                throw new InvalidSpecException("Foreign and local keys size should match");
            }
        } else if (builder.type == TYPE.CHECK) {

            Objects.requireNonNull(builder.checkExpression, "checkExpression must not be null for check type constraint");
            this.checkExpression = builder.checkExpression;
        }
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

    /**
     * @return the includeNames
     */
    public Collection<String> getIncludeNames() {
        return includeNames;
    }

    public ForeignKey getForeignKey() {
        return foreignKey;
    }

    /**
     * @return the checkExpression
     */
    public String getCheckExpression() {
        return checkExpression;
    }

    public static class Builder {
        private String constraintName;
        private TYPE type;
        private Collection<String> columnNames = new ArrayList<>();
        private Collection<String> includeNames = new ArrayList<>();
        private ForeignKey.Builder foreignKey;
        private String checkExpression;

        public Builder(String constraintName, TYPE type, String... columnNames) {
            this.constraintName = constraintName;
            this.type = type;
            this.columnNames.addAll(Arrays.asList(columnNames));
        }

        public Builder(String constraintName, TYPE type, Collection<String> columnNames) {
            this.constraintName = constraintName;
            this.type = type;
            this.columnNames.addAll(columnNames);
        }

        public Builder(String constraintName, TYPE type) {
            this.constraintName = constraintName;
            this.type = type;
        }

        public Builder columns(String... columnNames) {
            this.columnNames.addAll(Arrays.asList(columnNames));
            return this;
        }

        public Builder columns(Collection<String> columnNames) {
            this.columnNames.addAll(columnNames);
            return this;
        }

        public Builder include(String... includeNames) {
            this.includeNames.addAll(Arrays.asList(includeNames));
            return this;
        }

        public Builder include(Collection<String> includeNames) {
            this.includeNames.addAll(includeNames);
            return this;
        }

        public Builder foreignKey(Consumer<ForeignKey.Builder> fkBuilder) {
            foreignKey = new ForeignKey.Builder();
            fkBuilder.accept(foreignKey);
            return this;
        }

        public Builder checkExpression(String checkExpression) {
            this.checkExpression = checkExpression;
            return this;
        }

        public Constraint build() {
            return new Constraint(this);
        }
    }
}
