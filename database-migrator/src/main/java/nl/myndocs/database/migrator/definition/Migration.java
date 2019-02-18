package nl.myndocs.database.migrator.definition;

import java.util.Objects;
import java.util.function.Consumer;

import nl.myndocs.database.migrator.database.query.Database;
import nl.myndocs.database.migrator.processor.MigrationContext;

/**
 * Created by albert on 13-8-2017.
 */
public class Migration {
    private final String migrationId;
    private final Database database;
    private final Consumer<Table> tableConsumer;
    private final Consumer<Raw> rawConsumer;
    private final MigrationContext context;

    public Migration(
            String migrationId,
            Database database,
            Consumer<Table> tableConsumer,
            Consumer<Raw> rawConsumer,
            MigrationContext context
    ) {
        Objects.requireNonNull(migrationId, "migrationId must not be null");
        Objects.requireNonNull(database, "database must not be null");
        Objects.requireNonNull(tableConsumer, "tableConsumer must not be null");

        this.migrationId = migrationId;
        this.database = database;
        this.tableConsumer = tableConsumer;
        this.rawConsumer = rawConsumer;
        this.context = context;
    }

    public Table.Builder table(String tableName) {
        Objects.requireNonNull(tableName, "tableName must not be null");
        return new Table.Builder(tableName, tableConsumer);
    }

    public Raw.Builder raw() {
        return new Raw.Builder(rawConsumer);
    }

    public String getMigrationId() {
        return migrationId;
    }

    public Database getDatabase() {
        return database;
    }

    /**
     * @return the context
     */
    public MigrationContext getContext() {
        return context;
    }
}
