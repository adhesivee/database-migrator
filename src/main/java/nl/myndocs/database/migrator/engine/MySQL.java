package nl.myndocs.database.migrator.engine;

import nl.myndocs.database.migrator.definition.Column;
import nl.myndocs.database.migrator.definition.Constraint;
import nl.myndocs.database.migrator.definition.Table;
import nl.myndocs.database.migrator.engine.exception.CouldNotProcessException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by albert on 13-8-2017.
 */
public class MySQL extends BaseEngine {
    private static Logger logger = LoggerFactory.getLogger(MySQL.class);
    private static final String ALTER_TABLE_FORMAT = "ALTER TABLE %s CHANGE %s %s %s %s %s";

    private final Connection connection;

    public MySQL(Connection connection) {
        super(connection);
        this.connection = connection;
    }

    @Override
    public String getAlterColumnTerm() {
        return "MODIFY";
    }

    @Override
    public String getDropForeignKeyTerm() {
        return "FOREIGN KEY";
    }

    @Override
    public String getDropConstraintTerm() {
        return "INDEX";
    }

    @Override
    public void alterColumnName(Table table, Column column) {
        DatabaseColumn databaseColumn = loadDatabaseColumn(connection, table, column);

        String alterTableFormatted = String.format(
                ALTER_TABLE_FORMAT,
                table.getTableName(),
                column.getColumnName(),
                column.getRename().get(),
                databaseColumn.getColumnType(),
                (databaseColumn.getColumnDefault() != null && !databaseColumn.getColumnDefault().isEmpty() ? "DEFAULT '" + databaseColumn.getColumnDefault() + "'" : ""),
                databaseColumn.getNotNullValue()
        );

        try {
            executeInStatement(alterTableFormatted);
        } catch (SQLException e) {
            throw new CouldNotProcessException(e);
        }
    }

    public void alterColumnDefault(Connection connection, Table table, Column column) {
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
        try {
            executeInStatement(alterTableFormatted);
        } catch (SQLException e) {
            throw new CouldNotProcessException(e);
        }
    }

    private DatabaseColumn loadDatabaseColumn(Connection connection, Table table, Column column) {
        try {
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
        } catch (SQLException sqlException) {
            throw new CouldNotProcessException(sqlException);
        }
    }

    @Override
    public String getNativeColumnDefinition(Column column) {
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
