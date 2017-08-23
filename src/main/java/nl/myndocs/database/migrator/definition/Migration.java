package nl.myndocs.database.migrator.definition;

import nl.myndocs.database.migrator.database.query.Database;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * Created by albert on 13-8-2017.
 */
public class Migration {
    private final String migrationId;
    private final Database database;
    private final Consumer<Table> tableConsumer;

    public Migration(
            String migrationId,
            Database database,
            Consumer<Table> tableConsumer
    ) {
        this.migrationId = migrationId;
        this.database = database;
        this.tableConsumer = tableConsumer;
    }

    public Table.Builder table(String tableName) {
        return new Table.Builder(tableName, tableConsumer);
    }

    public String getMigrationId() {
        return migrationId;
    }

    public Database getDatabase() {
        return database;
    }
}
