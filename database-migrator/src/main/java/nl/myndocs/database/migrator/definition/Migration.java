package nl.myndocs.database.migrator.definition;

import nl.myndocs.database.migrator.database.query.Database;
import nl.myndocs.database.migrator.util.Assert;

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
        Assert.notNull(migrationId, "migrationId must not be null");
        Assert.notNull(database, "database must not be null");
        Assert.notNull(tableConsumer, "tableConsumer must not be null");

        this.migrationId = migrationId;
        this.database = database;
        this.tableConsumer = tableConsumer;
    }

    public Table.Builder table(String tableName) {
        Assert.notNull(tableName, "tableName must not be null");
        return new Table.Builder(tableName, tableConsumer);
    }

    public String getMigrationId() {
        return migrationId;
    }

    public Database getDatabase() {
        return database;
    }
}
