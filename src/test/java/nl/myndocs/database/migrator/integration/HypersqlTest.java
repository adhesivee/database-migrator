package nl.myndocs.database.migrator.integration;

import nl.myndocs.database.migrator.profile.HyperSQL;
import nl.myndocs.database.migrator.profile.MySQL;
import org.arquillian.cube.docker.impl.client.containerobject.dsl.Container;
import org.arquillian.cube.docker.impl.client.containerobject.dsl.DockerContainer;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Created by albert on 15-8-2017.
 */
@RunWith(Arquillian.class)
public class HypersqlTest extends BaseIntegration {

    @Test
    public void testConnection() throws SQLException, ClassNotFoundException, InterruptedException {
        Class.forName("org.hsqldb.jdbc.JDBCDriver");

        Connection connection = acquireConnection(
                "jdbc:hsqldb:mem:integration",
                "SA",
                ""
        );

        new HyperSQL().createDatabase(
                connection,
                buildMigration()
        );

        performIntegration(connection);
    }
}
