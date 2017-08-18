package nl.myndocs.database.migrator.engine;

import nl.myndocs.database.migrator.definition.Column;
import nl.myndocs.database.migrator.definition.Table;
import nl.myndocs.database.migrator.engine.exception.CouldNotProcessException;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by albert on 13-8-2017.
 */
public class Postgres extends BaseEngine {

    public Postgres(Connection connection) {
        super(connection);
    }

    @Override
    public String getAlterTypeTerm() {
        return "TYPE";
    }


    @Override
    public void alterColumnName(Table table, Column column) {
        try {
            executeInStatement("ALTER TABLE " + table.getTableName() + " RENAME " + column.getColumnName() + " TO " + column.getRename().get());
        } catch (SQLException e) {
            throw new CouldNotProcessException(e);
        }
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
