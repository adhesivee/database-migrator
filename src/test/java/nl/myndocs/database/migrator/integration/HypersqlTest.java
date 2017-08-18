package nl.myndocs.database.migrator.integration;

import nl.myndocs.database.migrator.engine.Engine;
import nl.myndocs.database.migrator.engine.HyperSQL;

import java.sql.Connection;

/**
 * Created by albert on 15-8-2017.
 */
public class HypersqlTest extends BaseIntegration {

    @Override
    protected Engine getEngine() {
        try {
            return new HyperSQL(getConnection());
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public Connection getConnection() throws ClassNotFoundException {
        Class.forName("org.hsqldb.jdbc.JDBCDriver");

        return acquireConnection(
                "jdbc:hsqldb:mem:integration",
                "SA",
                ""
        );

    }

}
