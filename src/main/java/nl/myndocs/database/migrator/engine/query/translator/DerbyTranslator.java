package nl.myndocs.database.migrator.engine.query.translator;

import nl.myndocs.database.migrator.definition.Column;
import nl.myndocs.database.migrator.definition.Table;
import nl.myndocs.database.migrator.engine.query.Phrase;
import nl.myndocs.database.migrator.engine.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Function;

/**
 * Created by albert on 18-8-2017.
 */
public class DerbyTranslator extends DefaultPhraseTranslator {
    private static final Logger logger = LoggerFactory.getLogger(DerbyTranslator.class);

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
        if (query.equals(Phrase.ALTER_TABLE, Phrase.ALTER_COLUMN, Phrase.TYPE)) {
            // @TODO This should be improved
            String[] alters = new String[]{
                    "ALTER TABLE %1$s ADD COLUMN %2$s_newtype %3$s",
                    "UPDATE %1$s SET %2$s_newtype = %2$s",
                    "ALTER TABLE %1$s DROP COLUMN %2$s",
                    "RENAME COLUMN %1$s.%2$s_newtype TO %2$s"
            };

            Table table = query.getTable();
            Column column = query.getColumn();

            String[] formattedStrings = new String[alters.length];

            int count = 0;
            for (String alter : alters) {
                formattedStrings[count] = String.format(
                        alter,
                        table.getTableName(),
                        column.getColumnName(),
                        getNativeColumnDefinition(column)
                );

                count++;
            }

            return formattedStrings;
        }
        return super.translatePhrases(query, phrases);
    }

    @Override
    protected Function<Query, String> translatePhrase(Phrase phrase) {
        if (phrase.equals(Phrase.TYPE)) {
            return query -> "SET DATA TYPE " + getNativeColumnDefinition(query.getColumn());
        }

        return super.translatePhrase(phrase);
    }

    @Override
    public String getNativeColumnDefinition(Column column) {
        Column.TYPE columnType = column.getType().get();
        switch (columnType) {
            case INTEGER:
                return "INTEGER " + (column.getAutoIncrement().orElse(false) ? "GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1)" : "");
            case VARCHAR:
                return "VARCHAR" + getWithSizeOrDefault(column, 255);
            case CHAR:
                return "CHAR" + getWithSizeOrDefault(column, 254);
            case UUID:
                logger.warn("UUID not supported, creating CHAR(36) instead");
                return "CHAR " + getWithSizeOrDefault(column, 36);
        }

        throw new RuntimeException("Unknown type");
    }
}
