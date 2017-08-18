package nl.myndocs.database.migrator.integration;

import nl.myndocs.database.migrator.database.query.PhraseTranslator;
import nl.myndocs.database.migrator.database.query.translator.DerbyTranslator;

import java.sql.Connection;

public class DerbyTest extends BaseIntegration {

    @Override
    protected PhraseTranslator phraseTranslator() {
        return new DerbyTranslator();
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
