package nl.myndocs.database.migrator;

import nl.myndocs.database.migrator.definition.Migration;

import java.sql.Connection;

/**
 * Created by albert on 19-8-2017.
 */
@FunctionalInterface
public interface MigrationScript {
    Migration migrate(Connection connection);
}
