package nl.myndocs.database.migrator;

import nl.myndocs.database.migrator.definition.Migration;

/**
 * Created by albert on 19-8-2017.
 */
public interface MigrationScript {
    String migrationId();

    String author();

    void migrate(Migration migration);
}
