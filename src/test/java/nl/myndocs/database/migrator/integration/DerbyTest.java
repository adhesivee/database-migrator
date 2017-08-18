package nl.myndocs.database.migrator.integration;

import nl.myndocs.database.migrator.engine.Derby;
import nl.myndocs.database.migrator.engine.Engine;
import nl.myndocs.database.migrator.engine.HyperSQL;

import java.sql.Connection;

public class DerbyTest extends BaseIntegration {
    @Override
    protected Engine getEngine() {
        try {
            return new Derby(getConnection());
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
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
}
