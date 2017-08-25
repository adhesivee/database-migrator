package nl.myndocs.database.migrator.database;

import nl.myndocs.database.migrator.database.exception.CouldNotProcessException;
import nl.myndocs.database.migrator.database.query.option.ChangeTypeOptions;
import nl.myndocs.database.migrator.definition.Column;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

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
    public void changeType(Column.TYPE type, ChangeTypeOptions changeTypeOptions) {
        String alterTypeFormat = "ALTER TABLE %s MODIFY COLUMN %s %s";

        executeInStatement(
                String.format(
                        alterTypeFormat,
                        getAlterTableName(),
                        getAlterColumnName(),
                        getNativeColumnDefinition(type, ChangeTypeOptions.empty())
                )
        );
    }

    @Override
    public void rename(String rename) {
        DatabaseColumn databaseColumn = loadDatabaseColumn(
                getAlterTableName(),
                getAlterColumnName()
        );

        executeInStatement(
                String.format(
                        "ALTER TABLE %s CHANGE %s %s %s %s %s",
                        getAlterTableName(),
                        getAlterColumnName(),
                        rename,
                        databaseColumn.getColumnType(),
                        (databaseColumn.getColumnDefault() != null && !databaseColumn.getColumnDefault().isEmpty() ? "DEFAULT '" + databaseColumn.getColumnDefault() + "'" : ""),
                        databaseColumn.getNotNullValue()
                )
        );
    }

    private DatabaseColumn loadDatabaseColumn(String tableName, String columnName) {
        try {
            Statement statement = connection.createStatement();
            statement.execute("DESCRIBE " + tableName);

            ResultSet resultSet = statement.getResultSet();

            String notNullValue = "";
            String columnType = "";
            String columnDefault = "";

            while (resultSet.next()) {
                if (resultSet.getString("Field").equals(columnName)) {
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
    public String getNativeColumnDefinition(Column.TYPE columnType) {
        switch (columnType) {
            case INTEGER:
            case UUID:
                return getNativeColumnDefinition(columnType, ChangeTypeOptions.empty());
            case VARCHAR:
            case CHAR:
                return getNativeColumnDefinition(columnType, ChangeTypeOptions.ofSize(255));
        }


        return super.getNativeColumnDefinition(columnType);
    }

    @Override
    protected String getNativeColumnDefinition(Column.TYPE columnType, ChangeTypeOptions changeTypeOptions) {
        switch (columnType) {
            case INTEGER:
                return "INTEGER " + (changeTypeOptions.getAutoIncrement().orElse(false) ? "AUTO_INCREMENT" : "");
            case UUID:
                logger.warn("UUID not supported, creating CHAR(36) instead");
                return getNativeColumnDefinition(Column.TYPE.CHAR, ChangeTypeOptions.ofSize(36));
        }

        return super.getNativeColumnDefinition(columnType, changeTypeOptions);
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
