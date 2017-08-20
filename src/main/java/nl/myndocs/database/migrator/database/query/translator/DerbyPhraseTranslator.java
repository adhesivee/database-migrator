package nl.myndocs.database.migrator.database.query.translator;

import nl.myndocs.database.migrator.database.query.Phrase;
import nl.myndocs.database.migrator.database.query.Query;
import nl.myndocs.database.migrator.database.query.option.AlterColumnOptions;
import nl.myndocs.database.migrator.definition.Column;
import nl.myndocs.database.migrator.definition.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.function.Function;

/**
 * Created by albert on 18-8-2017.
 */
public class DerbyPhraseTranslator extends DefaultPhraseTranslator {
    private static final Logger logger = LoggerFactory.getLogger(DerbyPhraseTranslator.class);

    public DerbyPhraseTranslator(Connection connection) {
        super(connection);
    }

    @Override
    public void changeType(Column.TYPE type, AlterColumnOptions alterColumnOptions) {
        String[] alters = new String[]{
                "ALTER TABLE %1$s ADD COLUMN %2$s_newtype %3$s",
                "UPDATE %1$s SET %2$s_newtype = %2$s",
                "ALTER TABLE %1$s DROP COLUMN %2$s",
                "RENAME COLUMN %1$s.%2$s_newtype TO %2$s"
        };


        for (String alter : alters) {
            executeInStatement(String.format(
                    alter,
                    getAlterTableName(),
                    getAlterColumnName(),
                    getNativeColumnDefinition(type)
                    )
            );

        }
    }

    @Override
    public String[] translatePhrases(Query query, Phrase... phrases) {
        if (query.equals(Phrase.ALTER_TABLE, Phrase.ALTER_COLUMN, Phrase.RENAME)) {
            Table table = query.getTable();
            Column column = query.getColumn();

            return new String[]{
                    String.format(
                            "RENAME COLUMN %1$s.%2$s TO %3$s",
                            table.getTableName(),
                            column.getColumnName(),
                            column.getRename().get()
                    )
            };
        }

        return super.translatePhrases(query, phrases);
    }

    @Override
    protected Function<Query, String> translatePhrase(Phrase phrase) {
        if (phrase.equals(Phrase.TYPE)) {
            return query -> "SET DATA TYPE " + getNativeColumnDefinition(query.getColumn().getType().get());
        }

        return super.translatePhrase(phrase);
    }

    @Override
    public String getNativeColumnDefinition(Column.TYPE columnType) {
        switch (columnType) {
            case INTEGER:
            case UUID:
                return getNativeColumnDefinition(columnType, AlterColumnOptions.empty());
            case VARCHAR:
                return getNativeColumnDefinition(columnType, AlterColumnOptions.ofSize(255));
            case CHAR:
                return getNativeColumnDefinition(columnType, AlterColumnOptions.ofSize(254));
        }

        throw new RuntimeException("Unknown type");
    }

    @Override
    protected String getNativeColumnDefinition(Column.TYPE columnType, AlterColumnOptions alterColumnOptions) {
        switch (columnType) {
            case INTEGER:
                return "INTEGER " + (alterColumnOptions.getAutoIncrement().orElse(false) ? "GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1)" : "");
            case UUID:
                logger.warn("UUID not supported, creating CHAR(36) instead");
                return getNativeColumnDefinition(Column.TYPE.CHAR, AlterColumnOptions.ofSize(36));
        }

        return super.getNativeColumnDefinition(columnType, alterColumnOptions);
    }
}
