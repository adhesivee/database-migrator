package nl.myndocs.database.migrator.definition;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by albert on 13-8-2017.
 */
public class Migration {
    private String migrationId;
    private List<Table> tables = new ArrayList<>();

    private Migration(Builder builder) {
        builder.getTables()
                .forEach(table -> tables.add(table.build()));

        migrationId = builder.getMigrationId();
    }

    public String getMigrationId() {
        return migrationId;
    }

    public List<Table> getTables() {
        return tables;
    }

    public static class Builder {
        private final String migrationId;
        private List<Table.Builder> tables = new ArrayList<>();

        public Builder(String migrationId) {
            this.migrationId = migrationId;
        }

        public Table.Builder table(String tableName) {
            Table.Builder builder = new Table.Builder(tableName);
            tables.add(builder);
            return builder;
        }

        public List<Table.Builder> getTables() {
            return tables;
        }

        public String getMigrationId() {
            return migrationId;
        }

        public Migration build() {
            return new Migration(this);
        }
    }
}
