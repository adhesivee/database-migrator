package nl.myndocs.database.migrator.profile;

import nl.myndocs.database.migrator.definition.Migration;

public interface Profile {
    void createDatabase(Migration migration);
}
