package nl.myndocs.database.migrator.engine;

import nl.myndocs.database.migrator.engine.query.PhraseTranslator;
import nl.myndocs.database.migrator.engine.query.translator.HyperSQLTranslator;

import java.sql.Connection;

/**
 * Created by albert on 15-8-2017.
 */
public class HyperSQL extends BaseEngine {

    public HyperSQL(Connection connection) {
        super(connection);
    }

    @Override
    protected PhraseTranslator phraseTranslator() {
        return new HyperSQLTranslator();
    }
}
