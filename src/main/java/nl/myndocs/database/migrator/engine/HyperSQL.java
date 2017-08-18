package nl.myndocs.database.migrator.engine;

import nl.myndocs.database.migrator.definition.Column;
import nl.myndocs.database.migrator.definition.Table;
import nl.myndocs.database.migrator.engine.exception.CouldNotProcessException;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by albert on 15-8-2017.
 */
public class HyperSQL extends BaseEngine {

    public HyperSQL(Connection connection) {
        super(connection);
    }

    @Override
    public void alterColumnName(Table table, Column column) {
        try {
            executeInStatement("ALTER TABLE " + table.getTableName() + " ALTER COLUMN " + column.getColumnName() + " RENAME TO " + column.getRename().get());
        } catch (SQLException e) {
            throw new CouldNotProcessException(e);
        }
    }

    @Override
    public String getNativeColumnDefinition(Column column) {
        Column.TYPE columnType = column.getType().get();
        switch (columnType) {
            case INTEGER:
                return "INTEGER " + (column.getAutoIncrement().orElse(false) ? "IDENTITY" : "");
            case VARCHAR:
                return "VARCHAR " + getWithSizeOrDefault(column, "255");
            case CHAR:
                return "CHAR " + getWithSizeOrDefault(column, "255");
            case UUID:
                return "UUID";
        }

        throw new RuntimeException("Unknown type");
    }
}
