package nl.myndocs.database.migrator.integration;

import nl.myndocs.database.migrator.profile.HyperSQL;
import nl.myndocs.database.migrator.profile.Profile;

import java.sql.Connection;

/**
 * Created by albert on 15-8-2017.
 */
public class HypersqlTest extends BaseIntegration {

    @Override
    protected Profile getProfile() {
        return new HyperSQL();
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
