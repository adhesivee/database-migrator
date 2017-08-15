package nl.myndocs.database.migrator.profile;

import nl.myndocs.database.migrator.definition.Migration;

import java.sql.Connection;

public interface Profile {
    void createDatabase(Connection connection, Migration migration);
}
