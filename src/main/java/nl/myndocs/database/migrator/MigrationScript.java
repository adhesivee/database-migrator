package nl.myndocs.database.migrator;

import nl.myndocs.database.migrator.definition.Migration;

import java.sql.Connection;

/**
 * Created by albert on 19-8-2017.
 */
public interface MigrationScript {
    String migrationId();

    void migrate(Migration migration);
}
