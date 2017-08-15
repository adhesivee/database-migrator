package nl.myndocs.database.migrator.integration;

import nl.myndocs.database.migrator.integration.tools.ResultSetPrinter;
import nl.myndocs.database.migrator.profile.MySQL;
import org.arquillian.cube.docker.impl.client.containerobject.dsl.Container;
import org.arquillian.cube.docker.impl.client.containerobject.dsl.DockerContainer;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by albert on 14-8-2017.
 */
@RunWith(Arquillian.class)
public class MysqlTest extends BaseIntegration {
    @DockerContainer
    Container mysqlContainer = Container.withContainerName("mysql-test")
            .fromImage("mysql")
            .withEnvironment("MYSQL_ROOT_PASSWORD", "root")
            .withEnvironment("MYSQL_DATABASE", "integration")
            .withPortBinding(3306)
            .build();

    @Test
    public void testConnection() throws SQLException, ClassNotFoundException, InterruptedException {

        Connection connection = getConnection();
        new MySQL().createDatabase(
                connection,
                buildMigration()
        );
        performIntegration(connection);
    }


    @Test
    public void testRenamingWithDefaults() throws ClassNotFoundException, SQLException {
        try {
            super.testRenamingWithDefaults(getConnection(), new MySQL());

        } catch (Exception e) {
        }
        Statement statement = getConnection().createStatement();
        statement.execute("DESCRIBE test_rename_table");

        ResultSet resultSet = statement.getResultSet();

        new ResultSetPrinter().print(resultSet);

    }

    public Connection getConnection() throws ClassNotFoundException {
        Class.forName("com.mysql.cj.jdbc.Driver");

        String containerHost = mysqlContainer.getIpAddress();
        int containerPort = mysqlContainer.getBindPort(3306);

        System.out.println("Connecting to database... " + containerHost + ":" + containerPort);

        return acquireConnection(
                "jdbc:mysql://" + containerHost + ":" + containerPort + "/integration",
                "root",
                "root"
        );
    }
}