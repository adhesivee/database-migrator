package nl.myndocs.database.migrator.database;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import nl.myndocs.database.migrator.database.exception.CouldNotProcessException;
import nl.myndocs.database.migrator.definition.Column;

/**
 * Created by albert on 18-8-2017.
 */
public class MySQLDatabase extends DefaultDatabase {
    private static final Logger logger = LoggerFactory.getLogger(MySQLDatabase.class);
    private final Connection connection;

    public MySQLDatabase(Connection connection) {
        super(connection);

        this.connection = connection;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected String dropConstraintSQL(String tableName, String constraintName) {
        // TODO Auto-generated method stub
        return super.dropConstraintSQL(tableName, constraintName);
    }
    // TODO query MySQL information schema to find out,
    // what kind of constraint is that due to idiotic MySQL dialect.
    /*
    @Override
    public void dropForeignKey(String constraintName) {
        String dropConstraintFormat = "ALTER TABLE %s DROP FOREIGN KEY %s";

        String dropConstraintQuery = String.format(
                dropConstraintFormat,
                getAlterTableName(),
                constraintName
        );

        executeInStatement(dropConstraintQuery);
    }
    */

    @Override
    protected String escapeString(String line) {
        String escapedLine = line.replaceAll("\\\\", "\\\\\\\\");

        return super.escapeString(escapedLine);
    }

    @Override
    public void dropConstraint(String constraintName) {
        String dropConstraintFormat = "ALTER TABLE %s DROP INDEX %s";

        String dropConstraintQuery = String.format(
                dropConstraintFormat,
                getAlterTableName(),
                constraintName
        );

        executeInStatement(dropConstraintQuery);
    }

    @Override
    public void changeType() {

        String alterTypeFormat = "ALTER TABLE %s MODIFY COLUMN %s %s";
        executeInStatement(
                String.format(
                        alterTypeFormat,
                        getAlterTableName(),
                        getAlterColumnName(),
                        getNativeColumnDefinition(getCurrentColumn())
                )
        );
    }

    @Override
    public void rename() {
        DatabaseColumn databaseColumn = loadDatabaseColumn(
                getAlterTableName(),
                getAlterColumnName()
        );

        executeInStatement(
                String.format(
                        "ALTER TABLE %s CHANGE %s %s %s %s %s",
                        getAlterTableName(),
                        getAlterColumnName(),
                        getCurrentColumn().getRename(),
                        databaseColumn.getColumnType(),
                        (databaseColumn.getColumnDefault() != null && !databaseColumn.getColumnDefault().isEmpty() ? "DEFAULT '" + databaseColumn.getColumnDefault() + "'" : ""),
                        databaseColumn.getNotNullValue()
                )
        );
    }

    private DatabaseColumn loadDatabaseColumn(String tableName, String columnName) {
        try (Statement statement = connection.createStatement()) {

            statement.execute("DESCRIBE " + tableName);

            String notNullValue = "";
            String columnType = "";
            String columnDefault = "";

            try (ResultSet resultSet = statement.getResultSet()) {

                while (resultSet.next()) {
                    if (resultSet.getString("Field").equals(columnName)) {
                        if ("NO".equals(resultSet.getString("Null"))) {
                            notNullValue = "NOT NULL";
                        }

                        columnType = resultSet.getString("Type");
                        columnDefault = resultSet.getString("Default");

                    }
                }
            }

            return new DatabaseColumn(notNullValue, columnType, columnDefault);
        } catch (SQLException sqlException) {
            throw new CouldNotProcessException(sqlException);
        }
    }

    @Override
    protected String getNativeColumnDefinition(Column column) {

        switch (column.getType()) {
            case BIG_INTEGER:
            case SMALL_INTEGER:
            case INTEGER:
                return super.getNativeColumnDefinition(column)
                        + " "
                        + (Objects.nonNull(column.getAutoIncrement()) && column.getAutoIncrement() ? "AUTO_INCREMENT" : "");
            case UUID:
                logger.warn("UUID not supported, creating CHAR(36) instead");
                return "CHAR(36)";
            default:
                break;
        }

        return super.getNativeColumnDefinition(column);
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
