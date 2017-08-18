package nl.myndocs.database.migrator.integration;

import nl.myndocs.database.migrator.database.query.PhraseTranslator;
import nl.myndocs.database.migrator.database.query.translator.PostgresTranslator;
import org.arquillian.cube.docker.impl.client.containerobject.dsl.Container;
import org.arquillian.cube.docker.impl.client.containerobject.dsl.DockerContainer;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.runner.RunWith;

import java.sql.Connection;

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


    @Override
    protected PhraseTranslator phraseTranslator() {
        return new PostgresTranslator();
    }

    @Override
    public Connection getConnection() throws ClassNotFoundException {
        Class.forName("org.postgresql.Driver");

        String containerHost = postgresContainer.getIpAddress();
        int containerPort = postgresContainer.getBindPort(5432);

        System.out.println("Connecting to database... " + containerHost + ":" + containerPort);
        return acquireConnection(
                "jdbc:postgresql://" + containerHost + ":" + containerPort + "/postgres?loggerLevel=OFF",
                "postgres",
                "postgres"
        );

    }
}
