package nl.myndocs.database.migrator.processor;

import nl.myndocs.database.migrator.definition.Migration;

import java.sql.SQLException;

public interface Migrator {
    void migrate(Migration migration) throws SQLException;
}
