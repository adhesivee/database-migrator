package nl.myndocs.database.migrator.database.query.translator;

import nl.myndocs.database.migrator.database.query.option.ChangeTypeOptions;
import nl.myndocs.database.migrator.definition.Column;

import java.sql.Connection;

/**
 * Created by albert on 18-8-2017.
 */
public class HyperSQLDatabase extends DefaultDatabase {

    public HyperSQLDatabase(Connection connection) {
        super(connection);
    }

    @Override
    protected String getNativeColumnDefinition(Column.TYPE columnType) {
        switch (columnType) {
            case INTEGER:
            case UUID:
                getNativeColumnDefinition(columnType, ChangeTypeOptions.empty());
            case VARCHAR:
            case CHAR:
                return getNativeColumnDefinition(columnType, ChangeTypeOptions.ofSize(255));
        }

        return super.getNativeColumnDefinition(columnType);
    }

    @Override
    protected String getNativeColumnDefinition(Column.TYPE columnType, ChangeTypeOptions changeTypeOptions) {
        switch (columnType) {
            case INTEGER:
                return "INTEGER " + (changeTypeOptions.getAutoIncrement().orElse(false) ? "IDENTITY" : "");
            case UUID:
                return "UUID";
        }
        return super.getNativeColumnDefinition(columnType, changeTypeOptions);
    }
}
