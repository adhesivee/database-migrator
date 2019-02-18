package nl.myndocs.database.migrator.integration.tools;

import java.util.function.Consumer;

import nl.myndocs.database.migrator.MigrationScript;
import nl.myndocs.database.migrator.definition.Migration;

public class SimpleMigrationScript implements MigrationScript {
    private final String migrationId;
    private final Consumer<Migration> migrationConsumer;

    public SimpleMigrationScript(String migrationId, Consumer<Migration> migrationConsumer) {
        this.migrationId = migrationId;
        this.migrationConsumer = migrationConsumer;
    }

    @Override
    public String migrationId() {
        return migrationId;
    }

    @Override
    public void migrate(Migration migration) {
        migrationConsumer.accept(migration);
    }

    @Override
    public String author() {
        return "DEFAULT";
    }
}
