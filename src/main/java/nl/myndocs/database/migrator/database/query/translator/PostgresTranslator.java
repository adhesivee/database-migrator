package nl.myndocs.database.migrator.database.query.translator;

import nl.myndocs.database.migrator.database.query.Phrase;
import nl.myndocs.database.migrator.database.query.Query;
import nl.myndocs.database.migrator.definition.Column;

import java.util.function.Function;

/**
 * Created by albert on 18-8-2017.
 */
public class PostgresTranslator extends DefaultPhraseTranslator {
    private static final Function<Query, String> ALTER_COLUMN_RENAME = query ->
            "RENAME " + query.getColumn().getColumnName() + " TO " + query.getColumn().getRename().get();

    @Override
    protected Function<Query, String> translatePhrase(Phrase phrase) {
        if (phrase.equals(Phrase.TYPE)) {
            return query -> "TYPE " + getNativeColumnDefinition(query.getColumn());
        }

        return super.translatePhrase(phrase);
    }

    @Override
    public String[] translatePhrases(Query query, Phrase... phrases) {
        if (query.equals(Phrase.ALTER_TABLE, Phrase.ALTER_COLUMN, Phrase.RENAME)) {
            return new String[]{translatePhrase(Phrase.ALTER_TABLE).apply(query) + " " + ALTER_COLUMN_RENAME.apply(query)};
        }

        return super.translatePhrases(query, phrases);
    }

    public String getNativeColumnDefinition(Column column) {
        Column.TYPE columnType = column.getType().get();
        switch (columnType) {
            case INTEGER:
                if (column.getAutoIncrement().orElse(false)) {
                    return "SERIAL";
                }
                return "INTEGER";
            case VARCHAR:
                return "VARCHAR " + getWithSizeIfPresent(column);
            case CHAR:
                return "CHAR " + getWithSizeIfPresent(column);
            case UUID:
                return "UUID";
        }

        throw new RuntimeException("Unknown type");
    }
}
