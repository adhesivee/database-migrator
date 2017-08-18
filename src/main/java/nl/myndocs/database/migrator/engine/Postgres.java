package nl.myndocs.database.migrator.engine;

import nl.myndocs.database.migrator.engine.query.PhraseTranslator;
import nl.myndocs.database.migrator.engine.query.translator.PostgresTranslator;

import java.sql.Connection;

/**
 * Created by albert on 13-8-2017.
 */
public class Postgres extends BaseEngine {

    public Postgres(Connection connection) {
        super(connection);
    }

    @Override
    protected PhraseTranslator phraseTranslator() {
        return new PostgresTranslator();
    }
}
