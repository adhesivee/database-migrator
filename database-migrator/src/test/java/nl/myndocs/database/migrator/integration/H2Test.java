package nl.myndocs.database.migrator.integration;

import nl.myndocs.database.migrator.database.H2Database;
import nl.myndocs.database.migrator.database.query.Database;

import java.sql.Connection;

/**
 * Created by albert on 14-8-2017.
 */
public class H2Test extends BaseIntegration {

    @Override
    protected Class<? extends Database> expectedDatabaseClass() {
        return H2Database.class;
    }

    @Override
    public Connection getConnection() throws ClassNotFoundException {
        Class.forName("org.h2.Driver");

        return acquireConnection(
                "jdbc:h2:mem:integration;DB_CLOSE_DELAY=-1",
                "",
                ""
        );
    }

    @Override
    protected boolean isConstraintViolationException(Exception exception) {
        exception.printStackTrace();
        return exception.getMessage().startsWith("Unique index or primary key violation");
    }
}
