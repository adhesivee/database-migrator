package nl.myndocs.database.migrator.profile;

import nl.myndocs.database.migrator.definition.Column;
import nl.myndocs.database.migrator.definition.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by albert on 13-8-2017.
 */
public class MySQL extends BaseProfile {
    private static Logger logger = LoggerFactory.getLogger(MySQL.class);

    @Override
    protected String getAlterColumnKey() {
        return "MODIFY";
    }

    @Override
    protected void changeColumnName(Connection connection, Table table, Column column) throws SQLException {
        Statement statement = connection.createStatement();
        statement.execute("DESCRIBE test_rename_table");

        ResultSet resultSet = statement.getResultSet();

        String notNullValue = "";
        String columnType = "";
        String columnDefault = "";
        while (resultSet.next()) {
            if (resultSet.getString("Field").equals(column.getColumnName())) {
                if ("NO".equals(resultSet.getString("Null"))) {
                    notNullValue = "NOT NULL";
                }

                columnType = resultSet.getString("Type");
                columnDefault = resultSet.getString("Default");

            }
        }

        statement.execute(
                "ALTER TABLE " +
                        table.getTableName() +
                        " CHANGE " +
                        column.getColumnName() + " " +
                        column.getRename().get() + " " +
                        columnType + " " +
                        (columnDefault != null && !columnDefault.isEmpty() ? "DEFAULT '" + columnDefault + "'" : "") + " " +
                        notNullValue);
        statement.close();
    }


    protected String getNativeColumnDefinition(Column column) {
        Column.TYPE columnType = column.getType().get();
        switch (columnType) {
            case INTEGER:
                return "INTEGER " + (column.getAutoIncrement().orElse(false) ? "AUTO_INCREMENT" : "");
            case VARCHAR:
                return "VARCHAR " + getWithSizeOrDefault(column, "255");
            case CHAR:
                return "CHAR " + getWithSizeOrDefault(column, "255");
            case UUID:
                logger.warn("UUID not supported, creating CHAR(36) instead");
                return "CHAR " + getWithSizeOrDefault(column, "36");
        }

        throw new RuntimeException("Unknown type");
    }
}
