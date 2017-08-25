package nl.myndocs.database.migrator.integration;

import nl.myndocs.database.migrator.database.HyperSQLDatabase;
import nl.myndocs.database.migrator.database.query.Database;

import java.sql.Connection;
import java.sql.SQLIntegrityConstraintViolationException;

/**
 * Created by albert on 15-8-2017.
 */
public class HypersqlTest extends BaseIntegration {

    @Override
    protected Class<? extends Database> expectedDatabaseClass() {
        return HyperSQLDatabase.class;
    }
    public Connection getConnection() throws ClassNotFoundException {
        Class.forName("org.hsqldb.jdbc.JDBCDriver");

        return acquireConnection(
                "jdbc:hsqldb:mem:integration",
                "SA",
                ""
        );

    }

    @Override
    protected boolean isConstraintViolationException(Exception exception) {
        return exception instanceof SQLIntegrityConstraintViolationException;
    }
}
