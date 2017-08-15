package nl.myndocs.database.migrator.definition;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

/**
 * Created by albert on 13-8-2017.
 */
public class Table {
    private String tableName;
    private List<Column> newColumns = new ArrayList<>();
    private Collection<ForeignKey> foreignKeys = new ArrayList<>();
    private Collection<Column> changeColumns = new ArrayList<>();

    private Table(Builder tableBuilder) {
        tableName = tableBuilder.getTableName();

        long emptyTypeCount = tableBuilder.getNewColumns()
                .stream()
                .filter(column -> column.getType() == null)
                .count();

        if (emptyTypeCount > 0) {
            throw new RuntimeException("New column types should have a type");
        }

        tableBuilder.getNewColumns()
                .forEach(column -> newColumns.add(column.build()));

        tableBuilder.getForeignColumnKeys()
                .forEach(foreignColumnKey -> foreignKeys.add(foreignColumnKey.build()));

        tableBuilder.getChangeColumns()
                .forEach(column -> changeColumns.add(column.build()));
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

    public Collection<ForeignKey> getForeignKeys() {
        return foreignKeys;
    }

    public static class Builder {
        private String tableName;
        private List<Column.Builder> newColumnBuilders = new ArrayList<>();
        private List<Column.Builder> changeColumns = new ArrayList<>();
        private Collection<ForeignKey.Builder> foreignColumnKeys = new ArrayList<>();

        public Builder(String tableName) {
            this.tableName = tableName;
        }

        private Column.Builder addNewColumn(String columnName, Column.TYPE type) {
            Column.Builder builder = new Column.Builder(columnName, type);

            newColumnBuilders.add(builder);
            return builder;
        }

        public Table.Builder addColumn(String columnName, Column.TYPE type) {
            addNewColumn(columnName, type);

            return this;
        }

        public Table.Builder addColumn(String columnName, Column.TYPE type, Consumer<Column.Builder> column) {
            Column.Builder columnBuilder = addNewColumn(columnName, type);
            column.accept(columnBuilder);

            return this;
        }

        private Column.Builder addChangeColumn(String columnName) {
            Column.Builder builder = new Column.Builder(columnName);

            changeColumns.add(builder);
            return builder;
        }

        public Table.Builder changeColumn(String columnName) {
            addChangeColumn(columnName);

            return this;
        }

        public Table.Builder changeColumn(String columnName, Consumer<Column.Builder> column) {
            Column.Builder columnBuilder = addChangeColumn(columnName);
            column.accept(columnBuilder);

            return this;
        }

        private ForeignKey.Builder createNewForeignKey(String foreignTable, Collection<String> localKeys, Collection<String> foreignKeys) {
            ForeignKey.Builder builder = new ForeignKey.Builder(foreignTable, localKeys, foreignKeys);
            foreignColumnKeys.add(
                    builder
            );

            return builder;
        }


        public Builder foreignKey(String foreignTable, Collection<String> localKeys, Collection<String> foreignKeys) {
            createNewForeignKey(foreignTable, localKeys, foreignKeys);

            return this;
        }

        public Builder foreignKey(String foreignTable, String localKey, String foreignKey) {
            return foreignKey(foreignTable, Arrays.asList(localKey), Arrays.asList(foreignKey));
        }

        public Builder foreignKey(String foreignTable, Collection<String> localKeys, Collection<String> foreignKeys, Consumer<ForeignKey.Builder> foreignKeyConsumer) {
            foreignKeyConsumer.accept(
                    createNewForeignKey(foreignTable, localKeys, foreignKeys)
            );
            return this;
        }

        public Builder foreignKey(String foreignTable, String localKey, String foreignKey, Consumer<ForeignKey.Builder> foreignKeyConsumer) {
            foreignKeyConsumer.accept(
                    createNewForeignKey(foreignTable, Arrays.asList(localKey), Arrays.asList(foreignKey))
            );

            return this;
        }


        public Collection<ForeignKey.Builder> getForeignColumnKeys() {
            return foreignColumnKeys;
        }

        public String getTableName() {
            return tableName;
        }

        public List<Column.Builder> getNewColumns() {
            return newColumnBuilders;
        }

        public List<Column.Builder> getChangeColumns() {
            return changeColumns;
        }

        public Table build() {
            return new Table(this);
        }
    }
}
