package nl.myndocs.database.migrator.definition;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by albert on 13-8-2017.
 */
public class Migration {
    private List<Table> newTables = new ArrayList<>();

    private Migration(Builder builder) {
        builder.getNewTables()
                .forEach(table -> newTables.add(table.build()));
    }

    public List<Table> getNewTables() {
        return newTables;
    }

    public static class Builder {
        private List<Table.Builder> newTables = new ArrayList<>();

        public Table.Builder addTable(String tableName) {
            Table.Builder builder = new Table.Builder(tableName);
            newTables.add(builder);
            return builder;
        }

        public List<Table.Builder> getNewTables() {
            return newTables;
        }

        public Migration build() {
            return new Migration(this);
        }

    }
}
