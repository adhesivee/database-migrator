package nl.myndocs.database.migrator.profile;

import nl.myndocs.database.migrator.definition.Column;
import nl.myndocs.database.migrator.definition.Table;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by albert on 15-8-2017.
 */
public class HyperSQL extends BaseProfile  {

    @Override
    protected void changeColumnName(Connection connection, Table table, Column column) throws SQLException {
        Statement statement = connection.createStatement();
        statement.execute("ALTER TABLE " + table.getTableName() + " ALTER COLUMN " + column.getColumnName() + " RENAME TO " + column.getRename().get());
        statement.close();
    }

    protected String getNativeColumnDefinition(Column column) {
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
