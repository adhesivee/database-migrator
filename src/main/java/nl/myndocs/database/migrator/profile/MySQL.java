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
    private static final String ALTER_TABLE_FORMAT = "ALTER TABLE %s CHANGE %s %s %s %s %s";

    public MySQL(Connection connection) {
        super(connection);
    }

    @Override
    protected String getAlterColumnTerm() {
        return "MODIFY";
    }

    @Override
    protected String getDropForeignKeyTerm() {
        return "FOREIGN KEY";
    }

    @Override
    protected String getDropConstraintTerm() {
        return "INDEX";
    }

    @Override
    protected void changeColumnName(Connection connection, Table table, Column column) throws SQLException {
        DatabaseColumn databaseColumn = loadDatabaseColumn(connection, table, column);

        Statement statement = connection.createStatement();

        String alterTableFormatted = String.format(
                ALTER_TABLE_FORMAT,
                table.getTableName(),
                column.getColumnName(),
                column.getRename().get(),
                databaseColumn.getColumnType(),
                (databaseColumn.getColumnDefault() != null && !databaseColumn.getColumnDefault().isEmpty() ? "DEFAULT '" + databaseColumn.getColumnDefault() + "'" : ""),
                databaseColumn.getNotNullValue()
        );

        statement.execute(alterTableFormatted);
        statement.close();
    }

    @Override
    protected void changeColumnDefault(Connection connection, Table table, Column column) throws SQLException {
        Statement statement = connection.createStatement();

        DatabaseColumn databaseColumn = loadDatabaseColumn(connection, table, column);

        String alterTableFormatted = String.format(
                ALTER_TABLE_FORMAT,
                table.getTableName(),
                column.getColumnName(),
                column.getColumnName(),
                databaseColumn.getColumnType(),
                (column.getDefaultValue().isPresent() ? "DEFAULT '" + column.getDefaultValue().get() + "'" : ""),
                databaseColumn.getNotNullValue()
        );
        statement.execute(alterTableFormatted);
        statement.close();
    }

    private DatabaseColumn loadDatabaseColumn(Connection connection, Table table, Column column) throws SQLException {
        Statement statement = connection.createStatement();
        statement.execute("DESCRIBE " + table.getTableName());

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

        statement.close();

        return new DatabaseColumn(notNullValue, columnType, columnDefault);
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

    private static class DatabaseColumn {
        private final String notNullValue;
        private final String columnType;
        private final String columnDefault;

        public DatabaseColumn(String notNullValue, String columnType, String columnDefault) {
            this.notNullValue = notNullValue;
            this.columnType = columnType;
            this.columnDefault = columnDefault;
        }

        public String getNotNullValue() {
            return notNullValue;
        }

        public String getColumnType() {
            return columnType;
        }

        public String getColumnDefault() {
            return columnDefault;
        }
    }
}
