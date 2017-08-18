package nl.myndocs.database.migrator.engine;

import nl.myndocs.database.migrator.engine.query.PhraseTranslator;
import nl.myndocs.database.migrator.engine.query.translator.MySQLTranslator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;

/**
 * Created by albert on 13-8-2017.
 */
public class MySQL extends BaseEngine {
    private static Logger logger = LoggerFactory.getLogger(MySQL.class);
    private static final String ALTER_TABLE_FORMAT = "ALTER TABLE %s CHANGE %s %s %s %s %s";

    private final Connection connection;

    public MySQL(Connection connection) {
        super(connection);
        this.connection = connection;
    }

    @Override
    protected PhraseTranslator phraseTranslator() {
        return new MySQLTranslator(connection);
    }
}
