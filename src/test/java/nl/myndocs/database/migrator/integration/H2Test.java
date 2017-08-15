package nl.myndocs.database.migrator.integration;

import nl.myndocs.database.migrator.profile.H2;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Created by albert on 14-8-2017.
 */
public class H2Test extends BaseIntegration {
    @Test
    public void testConnection() throws SQLException, ClassNotFoundException, InterruptedException {
        Connection connection = getConnection();

        new H2().createDatabase(
                connection,
                buildMigration()
        );

        performIntegration(connection);
    }

    @Test
    public void testRenamingWithDefaults() throws ClassNotFoundException, SQLException {
        super.testRenamingWithDefaults(getConnection(), new H2());
    }

    public Connection getConnection() throws ClassNotFoundException {
        Class.forName("org.h2.Driver");

        return acquireConnection(
                "jdbc:h2:mem:integration;DB_CLOSE_DELAY=-1",
                "",
                ""
        );
    }
}
