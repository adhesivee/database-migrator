package nl.myndocs.database.migrator.engine;

import nl.myndocs.database.migrator.definition.Column;

import java.sql.Connection;

/**
 * Created by albert on 15-8-2017.
 */
public class HyperSQL extends BaseEngine {

    public HyperSQL(Connection connection) {
        super(connection);
    }

    @Override
    public String getNativeColumnDefinition(Column column) {
        Column.TYPE columnType = column.getType().get();
        switch (columnType) {
            case INTEGER:
                return "INTEGER " + (column.getAutoIncrement().orElse(false) ? "IDENTITY" : "");
            case VARCHAR:
                return "VARCHAR " + getWithSizeOrDefault(column, 255);
            case CHAR:
                return "CHAR " + getWithSizeOrDefault(column, 255);
            case UUID:
                return "UUID";
        }

        throw new RuntimeException("Unknown type");
    }
}
