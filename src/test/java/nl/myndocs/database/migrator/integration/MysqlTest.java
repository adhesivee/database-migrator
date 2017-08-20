package nl.myndocs.database.migrator.integration;

import nl.myndocs.database.migrator.database.query.Database;
import nl.myndocs.database.migrator.database.query.PhraseTranslator;
import nl.myndocs.database.migrator.database.query.translator.MySQLPhraseTranslator;
import org.arquillian.cube.docker.impl.client.containerobject.dsl.Container;
import org.arquillian.cube.docker.impl.client.containerobject.dsl.DockerContainer;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.runner.RunWith;

import java.sql.Connection;

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


    @Override
    protected PhraseTranslator phraseTranslator() {
        try {
            return new MySQLPhraseTranslator(getConnection());
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected Database database() {
        try {
            return new MySQLPhraseTranslator(getConnection());
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
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