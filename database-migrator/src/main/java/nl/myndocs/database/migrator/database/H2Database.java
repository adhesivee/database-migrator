package nl.myndocs.database.migrator.database;

import nl.myndocs.database.migrator.database.query.option.ChangeTypeOptions;
import nl.myndocs.database.migrator.definition.Column;

import java.sql.Connection;

/**
 * Created by albert on 18-8-2017.
 */
public class H2Database extends DefaultDatabase {
    public H2Database(Connection connection) {
        super(connection);
    }

    @Override
    public String getNativeColumnDefinition(Column.TYPE columnType) {
        switch (columnType) {
            case BIG_INTEGER:
            case INTEGER:
            case SMALL_INTEGER:
            case UUID:
                return getNativeColumnDefinition(columnType, new ChangeTypeOptions());
            case VARCHAR:
            case CHAR:
                return getNativeColumnDefinition(columnType, ChangeTypeOptions.ofSize(255));
        }

        return super.getNativeColumnDefinition(columnType);
    }

    @Override
    protected String getNativeColumnDefinition(Column.TYPE columnType, ChangeTypeOptions changeTypeOptions) {
        switch (columnType) {
            case BIG_INTEGER:
            case SMALL_INTEGER:
            case INTEGER:
                return super.getNativeColumnDefinition(columnType) + " " + (changeTypeOptions.getAutoIncrement().orElse(false) ? "AUTO_INCREMENT" : "");
            case UUID:
                return "UUID";
        }
        return super.getNativeColumnDefinition(columnType, changeTypeOptions);
    }
}
