package nl.myndocs.database.migrator.engine;

import nl.myndocs.database.migrator.engine.query.PhraseTranslator;
import nl.myndocs.database.migrator.engine.query.translator.DerbyTranslator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;

public class Derby extends BaseEngine {
    private static final Logger logger = LoggerFactory.getLogger(Derby.class);

    public Derby(Connection connection) {
        super(connection);
    }

    @Override
    protected PhraseTranslator phraseTranslator() {
        return new DerbyTranslator();
    }
}
