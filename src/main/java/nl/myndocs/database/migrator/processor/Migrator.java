package nl.myndocs.database.migrator.processor;

import nl.myndocs.database.migrator.definition.Migration;

public interface Migrator {
    void migrate(Migration migration);
}
