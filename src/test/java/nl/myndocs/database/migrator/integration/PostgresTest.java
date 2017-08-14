package nl.myndocs.database.migrator.integration;

import nl.myndocs.database.migrator.definition.Column;
import nl.myndocs.database.migrator.definition.ForeignKey;
import nl.myndocs.database.migrator.definition.Migration;
import nl.myndocs.database.migrator.profile.Postgres;
import org.arquillian.cube.docker.impl.client.containerobject.dsl.Container;
import org.arquillian.cube.docker.impl.client.containerobject.dsl.DockerContainer;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.sql.*;

/**
 * Created by albert on 13-8-2017.
 */
@RunWith(Arquillian.class)
public class PostgresTest extends BaseIntegration {
    @DockerContainer
    Container postgresContainer = Container.withContainerName("postgres-test")
            .fromImage("postgres")
            .withPortBinding(5432)
            .build();


    @Test
    public void testConnection() throws SQLException, ClassNotFoundException, InterruptedException {
        Class.forName("org.postgresql.Driver");

        String containerHost = postgresContainer.getIpAddress();
        int containerPort = postgresContainer.getBindPort(5432);

        System.out.println("Connecting to database... " + containerHost + ":" + containerPort);
        Connection connection = acquireConnection(
                "jdbc:postgresql://" + containerHost + ":" + containerPort + "/postgres?loggerLevel=OFF",
                "postgres",
                "postgres"
        );

        new Postgres().createDatabase(
                connection,
                buildMigration()
        );

        performIntegration(connection);
    }

}
