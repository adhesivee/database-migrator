package nl.myndocs.database.migrator.integration;

import nl.myndocs.database.migrator.profile.HyperSQL;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Created by albert on 15-8-2017.
 */
public class HypersqlTest extends BaseIntegration {

    @Test
    public void testConnection() throws SQLException, ClassNotFoundException, InterruptedException {
        Connection connection = getConnection();
        new HyperSQL().createDatabase(
                connection,
                buildMigration()
        );

        performIntegration(connection);
    }

    @Test
    public void testRenamingWithDefaults() throws ClassNotFoundException, SQLException {
        super.testRenamingWithDefaults(getConnection(), new HyperSQL());
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
