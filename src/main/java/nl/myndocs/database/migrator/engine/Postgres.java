package nl.myndocs.database.migrator.engine;

import nl.myndocs.database.migrator.definition.Column;
import nl.myndocs.database.migrator.engine.query.Phrase;
import nl.myndocs.database.migrator.engine.query.Query;

import java.sql.Connection;
import java.util.function.Function;

/**
 * Created by albert on 13-8-2017.
 */
public class Postgres extends BaseEngine {

    public Postgres(Connection connection) {
        super(connection);
    }

    private static final Function<Query, String> ALTER_COLUMN_RENAME = query ->
            "RENAME " + query.getColumn().getColumnName() + " TO " + query.getColumn().getRename().get();

    @Override
    protected String[] getQueries(Query query) {
        if (query.equals(Phrase.ALTER_TABLE, Phrase.ALTER_COLUMN, Phrase.RENAME)) {
            return new String[]{translatePhrase(Phrase.ALTER_TABLE).apply(query) + " " + ALTER_COLUMN_RENAME.apply(query)};
        }

        return super.getQueries(query);
    }

    @Override
    protected Function<Query, String> translatePhrase(Phrase phrase) {
        if (phrase.equals(Phrase.TYPE)) {
            return query -> "TYPE " + getNativeColumnDefinition(query.getColumn());
        }

        return super.translatePhrase(phrase);
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
                return "VARCHAR " + getWithSizeIfPossible(column);
            case CHAR:
                return "CHAR " + getWithSizeIfPossible(column);
            case UUID:
                return "UUID";
        }

        throw new RuntimeException("Unknown type");
    }
}
