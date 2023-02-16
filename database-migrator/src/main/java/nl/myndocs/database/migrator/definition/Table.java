package nl.myndocs.database.migrator.definition;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * Created by albert on 13-8-2017.
 */
public class Table {
    private String tableName;
    private List<Column> newColumns = new ArrayList<>();
    private Collection<Constraint> newConstraints = new ArrayList<>();
    private Collection<Index> newIndexes = new ArrayList<>();
    private Collection<Column> changeColumns = new ArrayList<>();
    private Collection<String> dropColumns;
    private Collection<String> dropConstraints;
    private Collection<String> dropIndexes;
    private Collection<String> rawSQL;
    private PartitionSet partitions;

    private Table(Builder tableBuilder) {

        tableName = tableBuilder.tableName;

        tableBuilder.newColumnBuilders
                .forEach(column -> newColumns.add(column.build()));

        tableBuilder.changeColumns
                .forEach(column -> changeColumns.add(column.build()));

        tableBuilder.newConstraints
                .forEach(constraint -> newConstraints.add(constraint.build()));

        tableBuilder.newIndexes
                .forEach(index -> newIndexes.add(index.build()));

        dropColumns = tableBuilder.dropColumns;
        dropConstraints = tableBuilder.dropConstraints;
        dropIndexes = tableBuilder.dropIndexes;

        rawSQL = tableBuilder.rawSQL;

        partitions = tableBuilder.partitions != null ? tableBuilder.partitions.build() : null;
    }

    public String getTableName() {
        return tableName;
    }

    public List<Column> getNewColumns() {
        return newColumns;
    }

    public Collection<Column> getChangeColumns() {
        return changeColumns;
    }

    public Collection<String> getDropColumns() {
        return dropColumns;
    }

    public Collection<String> getDropConstraints() {
        return dropConstraints;
    }

    public Collection<Constraint> getNewConstraints() {
        return newConstraints;
    }

    public Collection<Index> getNewIndexes() {
        return newIndexes;
    }

    public Collection<String> getDropIndexes() {
        return dropIndexes;
    }

    /**
     * @return the rawSQL
     */
    public Collection<String> getRawSQL() {
        return rawSQL;
    }

    /**
     * @return the partitions
     */
    public PartitionSet getPartitions() {
        return partitions;
    }

    public Stream<Partition> getPartitionStream() {
        return isPartitioned() ? partitions.getPartitions().stream() : Stream.empty();
    }

    public boolean isPartitioned() {
        return partitions != null && !partitions.isEmpty();
    }

    public boolean isSharded() {
        return isPartitioned() && partitions.getPartitions().stream().anyMatch(Partition::isForeign);
    }

    public static class Builder {
        private String tableName;
        private final Consumer<Table> tableConsumer;
        private PartitionSet.Builder partitions;
        private List<Column.Builder> newColumnBuilders = new ArrayList<>();
        private List<Column.Builder> changeColumns = new ArrayList<>();
        private Collection<Constraint.Builder> newConstraints = new ArrayList<>();
        private Collection<Index.Builder> newIndexes = new ArrayList<>();
        private Collection<String> dropColumns = new ArrayList<>();
        private Collection<String> dropConstraints = new ArrayList<>();
        private Collection<String> dropIndexes = new ArrayList<>();
        private Collection<String> rawSQL = new ArrayList<>();

        public Builder(String tableName, Consumer<Table> tableConsumer) {
            Objects.requireNonNull(tableName, "tableName must not be null");
            Objects.requireNonNull(tableConsumer, "tableConsumer must not be null");

            this.tableName = tableName;
            this.tableConsumer = tableConsumer;
        }

        public Table.Builder addPartitions(PartitionSet.TYPE type, Collection<Partition> partitions) {
            PartitionSet.Builder builder = new PartitionSet.Builder(type);
            builder.partitions(() -> partitions);
            this.partitions = builder;
            return this;
        }

        public Table.Builder addPartitions(PartitionSet.TYPE type, Consumer<PartitionSet.Builder> c) {
            PartitionSet.Builder builder = new PartitionSet.Builder(type);
            c.accept(builder);
            this.partitions = builder;
            return this;
        }

        public Table.Builder addColumn(String columnName, Column.TYPE type) {
            Column.Builder builder = new Column.Builder(columnName, type);
            newColumnBuilders.add(builder);
            return this;
        }

        public Table.Builder addColumn(String columnName, Column.TYPE type, Consumer<Column.Builder> column) {
            Column.Builder columnBuilder = new Column.Builder(columnName, type);
            column.accept(columnBuilder);
            newColumnBuilders.add(columnBuilder);
            return this;
        }

        public Table.Builder changeColumn(String columnName) {
            Column.Builder builder = new Column.Builder(columnName);
            changeColumns.add(builder);
            return this;
        }

        public Table.Builder changeColumn(String columnName, Consumer<Column.Builder> column) {
            Column.Builder columnBuilder = new Column.Builder(columnName);
            column.accept(columnBuilder);
            changeColumns.add(columnBuilder);
            return this;
        }

        public Builder dropColumn(String columnName) {
            this.dropColumns.add(columnName);
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

        public Builder addRawSQL(String sql) {
            this.rawSQL.add(sql);
            return this;
        }

        public Table build() {
            return new Table(this);
        }

        public void save() {
            tableConsumer.accept(build());
        }
    }
}
