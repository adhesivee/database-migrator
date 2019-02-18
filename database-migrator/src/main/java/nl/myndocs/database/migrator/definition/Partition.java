package nl.myndocs.database.migrator.definition;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * @author Mikhail Mikhailov
 * Simple partition class.
 */
public class Partition {

    private final String partitionName;
    private final boolean foreign;
    private final PartitionSpec partitionSpec;
    private final Collection<Constraint> newConstraints;
    private final Collection<Index> newIndexes;
    private final Collection<String> dropConstraints;
    private final Collection<String> dropIndexes;
    /**
     * Constructor.
     */
    public Partition(Builder builder) {

        super();

        Objects.requireNonNull(builder.partitionName, "partitionName must not be null");
        Objects.requireNonNull(builder.partitionSpec, "partitionExpr must not be null");

        this.partitionName = builder.partitionName;
        this.foreign = builder.foreign;
        this.partitionSpec = builder.partitionSpec;
        this.newConstraints = new ArrayList<>(builder.newConstraints.size());
        this.newIndexes = new ArrayList<>(builder.newIndexes.size());

        builder.newConstraints.forEach(constraint -> this.newConstraints.add(constraint.build()));
        builder.newIndexes.forEach(index -> this.newIndexes.add(index.build()));

        this.dropConstraints = builder.dropConstraints;
        this.dropIndexes = builder.dropIndexes;
    }
    /**
     * @return the partitionName
     */
    public String getPartitionName() {
        return partitionName;
    }
    /**
     * @return the foreign
     */
    public boolean isForeign() {
        return foreign;
    }
    /**
     * @return the partitionExpr
     */
    public PartitionSpec getPartitionSpec() {
        return partitionSpec;
    }

    public Collection<Constraint> getNewConstraints() {
        return newConstraints;
    }

    public Collection<Index> getNewIndexes() {
        return newIndexes;
    }

    public Collection<String> getDropConstraints() {
        return dropConstraints;
    }

    public Collection<String> getDropIndexes() {
        return dropIndexes;
    }
    /**
     * @author Mikhail Mikhailov
     * The builder type.
     */
    public static class Builder {
        private String partitionName;
        private boolean foreign;
        private PartitionSpec partitionSpec;
        private Collection<Constraint.Builder> newConstraints = new ArrayList<>();
        private Collection<Index.Builder> newIndexes = new ArrayList<>();
        private Collection<String> dropConstraints = new ArrayList<>();
        private Collection<String> dropIndexes = new ArrayList<>();

        public Builder() {
            super();
        }

        public Builder(String name, boolean foreign) {
            super();
            this.partitionName = name;
            this.foreign = foreign;
        }

        public Builder setPartitionName(String partitionName) {
            this.partitionName = partitionName;
            return this;
        }

        public Builder setForeign(boolean foreign) {
            this.foreign = foreign;
            return this;
        }

        public Builder setPartitionSpec(PartitionSpec partitionSpec) {
            this.partitionSpec = partitionSpec;
            return this;
        }

        /**
         * {@link Builder#addIndex(String, Index.TYPE, String)}
         */
        public Builder addConstraint(String constraintName, Constraint.TYPE type, String columnName) {
            return addConstraint(constraintName, type, Arrays.asList(columnName));
        }
        /**
         * {@link Builder#addIndex(String, Index.TYPE, String)}
         */
        public Builder addConstraint(String constraintName, Constraint.TYPE type, String... columnNames) {
            return addConstraint(constraintName, type, Arrays.asList(columnNames));
        }

        /**
         * {@link Builder#addIndex(String, Index.TYPE, Collection)}
         */
        public Builder addConstraint(String constraintName, Constraint.TYPE type, Collection<String> columnNames) {
            newConstraints.add(new Constraint.Builder(constraintName, type, columnNames));
            return this;
        }
        /**
         * {@link Builder#addIndex(String, Index.TYPE, String)}
         */
        public Builder addConstraint(String constraintName, Constraint.TYPE type, Consumer<Constraint.Builder> consumer) {
            Constraint.Builder constraintBuilder = new Constraint.Builder(constraintName, type);
            consumer.accept(constraintBuilder);
            newConstraints.add(constraintBuilder);
            return this;
        }
        /**
         * {@link Builder#dropIndex(String)}
         */
        public Builder dropConstraint(String constraintName) {
            dropConstraints.add(constraintName);
            return this;
        }

        public Builder addIndex(String indexName, Index.TYPE type, String columnName) {
            return addIndex(indexName, type, Arrays.asList(columnName));
        }

        public Builder addIndex(String indexName, Index.TYPE type, String... columnName) {
            return addIndex(indexName, type, Arrays.asList(columnName));
        }

        public Builder addIndex(String indexName, Index.TYPE type, Collection<String> columnNames) {
            newIndexes.add(new Index.Builder(indexName, type, columnNames));
            return this;
        }

        /**
         * {@link Builder#addIndex(String, Index.TYPE, String)}
         */
        public Builder addIndex(String indexName, Index.TYPE type, Consumer<Index.Builder> consumer) {
            Index.Builder constraintBuilder = new Index.Builder(indexName, type);
            consumer.accept(constraintBuilder);
            newIndexes.add(constraintBuilder);
            return this;
        }

        public Builder dropIndex(String indexName) {
            dropIndexes.add(indexName);
            return this;
        }

        public Partition build() {
            return new Partition(this);
        }
    }
}
