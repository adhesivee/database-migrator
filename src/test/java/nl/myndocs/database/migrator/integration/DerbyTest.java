package nl.myndocs.database.migrator.integration;

import nl.myndocs.database.migrator.database.query.Database;
import nl.myndocs.database.migrator.database.query.PhraseTranslator;
import nl.myndocs.database.migrator.database.query.translator.DerbyPhraseTranslator;

import java.sql.Connection;

public class DerbyTest extends BaseIntegration {

    @Override
    protected PhraseTranslator phraseTranslator() {
        try {
            return new DerbyPhraseTranslator(getConnection());
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected Database database() {
        try {
            return new DerbyPhraseTranslator(getConnection());
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public Connection getConnection() throws ClassNotFoundException {
        try {
            Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

        return acquireConnection(
                "jdbc:derby:memory:integration;create=true",
                "SA",
                ""
        );

    }
}
