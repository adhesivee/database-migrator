package nl.myndocs.database.migrator.integration;

import nl.myndocs.database.migrator.database.DerbyDatabase;
import nl.myndocs.database.migrator.database.query.Database;
import org.apache.derby.shared.common.error.DerbySQLIntegrityConstraintViolationException;

import java.sql.Connection;

public class DerbyTest extends BaseIntegration {

    @Override
    protected Class<? extends Database> expectedDatabaseClass() {
        return DerbyDatabase.class;
    }

    public Connection getConnection() throws ClassNotFoundException {
        try {
            Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

        return acquireConnection(
                "jdbc:derby:memory:integration;create=true",
                "SA",
                ""
        );
    }

    @Override
    protected boolean isConstraintViolationException(Exception exception) {
        return exception instanceof DerbySQLIntegrityConstraintViolationException;
    }
}
