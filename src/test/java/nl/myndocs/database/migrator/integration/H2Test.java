package nl.myndocs.database.migrator.integration;

import nl.myndocs.database.migrator.engine.Engine;
import nl.myndocs.database.migrator.engine.H2;

import java.sql.Connection;

/**
 * Created by albert on 14-8-2017.
 */
public class H2Test extends BaseIntegration {

    @Override
    protected Engine getEngine() {
        return new H2();
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
}
