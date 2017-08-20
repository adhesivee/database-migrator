package nl.myndocs.database.migrator.database.query.translator;

import nl.myndocs.database.migrator.database.query.option.AlterColumnOptions;
import nl.myndocs.database.migrator.definition.Column;

import java.sql.Connection;

/**
 * Created by albert on 18-8-2017.
 */
public class HyperSQLPhraseTranslator extends DefaultPhraseTranslator {

    public HyperSQLPhraseTranslator(Connection connection) {
        super(connection);
    }

    @Override
    protected String getNativeColumnDefinition(Column.TYPE columnType) {
        switch (columnType) {
            case INTEGER:
            case UUID:
                getNativeColumnDefinition(columnType, AlterColumnOptions.empty());
            case VARCHAR:
            case CHAR:
                return getNativeColumnDefinition(columnType, AlterColumnOptions.ofSize(255));
        }

        return super.getNativeColumnDefinition(columnType);
    }

    @Override
    protected String getNativeColumnDefinition(Column.TYPE columnType, AlterColumnOptions alterColumnOptions) {
        switch (columnType) {
            case INTEGER:
                return "INTEGER " + (alterColumnOptions.getAutoIncrement().orElse(false) ? "IDENTITY" : "");
            case UUID:
                return "UUID";
        }
        return super.getNativeColumnDefinition(columnType, alterColumnOptions);
    }
}
