package nl.myndocs.database.migrator.integration;

import nl.myndocs.database.migrator.database.query.Database;
import nl.myndocs.database.migrator.database.query.translator.HyperSQLPhraseTranslator;

import java.sql.Connection;

/**
 * Created by albert on 15-8-2017.
 */
public class HypersqlTest extends BaseIntegration {

    @Override
    protected Database database() {
        try {
            return new HyperSQLPhraseTranslator(getConnection());
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
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
