package nl.myndocs.database.migrator.integration;

import nl.myndocs.database.migrator.profile.H2;
import nl.myndocs.database.migrator.profile.HyperSQL;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Created by albert on 14-8-2017.
 */
public class H2Test extends BaseIntegration {
    @Test
    public void testConnection() throws SQLException, ClassNotFoundException, InterruptedException {
        Class.forName("org.h2.Driver");

        Connection connection = acquireConnection(
                "jdbc:h2:mem:integration;DB_CLOSE_DELAY=-1",
                "",
                ""
        );

        new H2().createDatabase(
                connection,
                buildMigration()
        );

        performIntegration(connection);
    }
}
