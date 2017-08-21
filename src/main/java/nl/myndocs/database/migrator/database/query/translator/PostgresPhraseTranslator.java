package nl.myndocs.database.migrator.database.query.translator;

import nl.myndocs.database.migrator.database.query.Phrase;
import nl.myndocs.database.migrator.database.query.Query;
import nl.myndocs.database.migrator.database.query.option.ChangeTypeOptions;
import nl.myndocs.database.migrator.definition.Column;

import java.sql.Connection;
import java.util.function.Function;

/**
 * Created by albert on 18-8-2017.
 */
public class PostgresPhraseTranslator extends DefaultPhraseTranslator {
    private static final Function<Query, String> ALTER_COLUMN_RENAME = query ->
            "RENAME " + query.getColumn().getColumnName() + " TO " + query.getColumn().getRename().get();

    public PostgresPhraseTranslator(Connection connection) {
        super(connection);
    }

    @Override
    public void changeType(Column.TYPE type, ChangeTypeOptions changeTypeOptions) {
        String alterTypeFormat = "ALTER TABLE %s ALTER COLUMN %s TYPE %s";

        executeInStatement(
                String.format(
                        alterTypeFormat,
                        getAlterTableName(),
                        getAlterColumnName(),
                        getNativeColumnDefinition(type, ChangeTypeOptions.empty())
                )
        );
    }

    @Override
    public void rename(String rename) {
        executeInStatement(
                String.format(
                        "ALTER TABLE %s RENAME %s TO %s",
                        getAlterTableName(),
                        getAlterColumnName(),
                        rename
                )
        );
    }

    @Override
    protected Function<Query, String> translatePhrase(Phrase phrase) {
        if (phrase.equals(Phrase.TYPE)) {
            return query -> "TYPE " + getNativeColumnDefinition(query.getColumn().getType().get());
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

    @Override
    protected String getNativeColumnDefinition(Column.TYPE columnType) {
        switch (columnType) {
            case INTEGER:
            case UUID:
                getNativeColumnDefinition(columnType, ChangeTypeOptions.empty());
        }

        return super.getNativeColumnDefinition(columnType);
    }

    @Override
    protected String getNativeColumnDefinition(Column.TYPE columnType, ChangeTypeOptions changeTypeOptions) {
        switch (columnType) {
            case INTEGER:
                if (changeTypeOptions.getAutoIncrement().orElse(false)) {
                    return "SERIAL";
                }
                return "INTEGER";
            case UUID:
                return "UUID";
        }
        return super.getNativeColumnDefinition(columnType, changeTypeOptions);
    }
}
