package nl.myndocs.database.migrator.integration;

import nl.myndocs.database.migrator.profile.H2;
import nl.myndocs.database.migrator.profile.Profile;

import java.sql.Connection;

/**
 * Created by albert on 14-8-2017.
 */
public class H2Test extends BaseIntegration {

    @Override
    protected Profile getProfile() {
        try {
            return new H2(getConnection());
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
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
