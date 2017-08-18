package nl.myndocs.database.migrator.engine;

import nl.myndocs.database.migrator.engine.query.PhraseTranslator;
import nl.myndocs.database.migrator.engine.query.translator.H2Translator;

import java.sql.Connection;

/**
 * Created by albert on 14-8-2017.
 */
public class H2 extends BaseEngine {

    public H2(Connection connection) {
        super(connection);
    }

    @Override
    protected PhraseTranslator phraseTranslator() {
        return new H2Translator();
    }
}
