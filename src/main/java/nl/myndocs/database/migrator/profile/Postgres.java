package nl.myndocs.database.migrator.profile;

import nl.myndocs.database.migrator.definition.Column;
import nl.myndocs.database.migrator.definition.Table;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by albert on 13-8-2017.
 */
public class Postgres extends BaseProfile {

    @Override
    protected String getAlterTypeKey() {
        return "TYPE";
    }


    @Override
    protected void changeColumnName(Connection connection, Table table, Column column) throws SQLException {
        Statement statement = connection.createStatement();
        statement.execute("ALTER TABLE " + table.getTableName() + " RENAME " + column.getColumnName() + " TO " + column.getRename().get());
        statement.close();
    }

    protected String getNativeColumnDefinition(Column column) {
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
