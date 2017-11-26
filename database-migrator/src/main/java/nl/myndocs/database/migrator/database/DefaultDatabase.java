package nl.myndocs.database.migrator.database;

import nl.myndocs.database.migrator.database.exception.CouldNotProcessException;
import nl.myndocs.database.migrator.database.query.AlterColumn;
import nl.myndocs.database.migrator.database.query.AlterTable;
import nl.myndocs.database.migrator.database.query.Database;
import nl.myndocs.database.migrator.database.query.option.ChangeTypeOptions;
import nl.myndocs.database.migrator.database.query.option.ColumnOptions;
import nl.myndocs.database.migrator.database.query.option.ForeignKeyOptions;
import nl.myndocs.database.migrator.definition.Column;
import nl.myndocs.database.migrator.definition.Constraint;
import nl.myndocs.database.migrator.definition.ForeignKey;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Created by albert on 18-8-2017.
 */
public class DefaultDatabase implements Database, AlterTable, AlterColumn {

    private final Connection connection;
    private String alterTableName;
    private String alterColumnName;

    protected String getAlterTableName() {
        return alterTableName;
    }

    protected String getAlterColumnName() {
        return alterColumnName;
    }

    @Override
    public AlterTable alterTable(String tableName) {
        alterTableName = tableName;
        return this;
    }

    @Override
    public AlterColumn alterColumn(String columnName) {
        alterColumnName = columnName;
        return this;
    }

    @Override
    public void setDefault(String defaultValue) {
        String queryFormat = "ALTER TABLE %s ALTER COLUMN %s SET DEFAULT '%s'";

        executeInStatement(
                String.format(
                        queryFormat,
                        getAlterTableName(),
                        getAlterColumnName(),
                        escapeString(defaultValue)
                )
        );
    }

    protected String escapeString(String line) {
        return line.replaceAll("'", "''");
    }

    @Override
    public void createTable(String tableName, Collection<ColumnOptions> columnOptions) {
        Collection<String> columnQueries = new ArrayList<>();
        for (ColumnOptions columnOption : columnOptions) {
            columnQueries.add(translateColumnOptions(columnOption));
        }


        executeInStatement(
                String.format(
                        "CREATE TABLE %s (%s)",
                        tableName,
                        String.join(",", columnQueries)
                )
        );
    }

    @Override
    public void addColumn(ColumnOptions columnOption) {
        executeInStatement(
                String.format(
                        "ALTER TABLE %s ADD COLUMN %s",
                        getAlterTableName(),
                        translateColumnOptions(columnOption)
                )
        );
    }

    private String translateColumnOptions(ColumnOptions columnOption) {
        return columnOption.getColumnName() + " " +
                getNativeColumnDefinition(
                        columnOption.getColumnType(),
                        new ChangeTypeOptions(
                                columnOption.getAutoIncrement().orElse(null),
                                columnOption.getColumnSize().orElse(null)
                        )
                ) + " " +
                (columnOption.getDefaultValue().isPresent() ? getDefaultValue(columnOption.getColumnType(), columnOption.getDefaultValue().get()) : "") + " " +
                (columnOption.getIsNotNull().orElse(false) ? "NOT NULL" : "") + " " +
                (columnOption.getIsPrimary().orElse(false) ? "PRIMARY KEY" : "") + " ";
    }

    @Override
    public void changeType(Column.TYPE type, ChangeTypeOptions changeTypeOptions) {
        String alterColumnFormat = "ALTER TABLE %s ALTER COLUMN %s %s";

        String alterQuery = String.format(
                alterColumnFormat,
                alterTableName,
                alterColumnName,
                getNativeColumnDefinition(type, changeTypeOptions)
        );

        executeInStatement(alterQuery);
    }

    @Override
    public void dropColumn(String columnName) {
        String dropColumnFormat = "ALTER TABLE %s DROP COLUMN %s";

        String dropColumnQuery = String.format(
                dropColumnFormat,
                alterTableName,
                columnName
        );

        executeInStatement(dropColumnQuery);
    }

    @Override
    public void dropForeignKey(String constraintName) {
        dropConstraint(constraintName);
    }

    @Override
    public void dropConstraint(String constraintName) {
        String dropConstraintFormat = "ALTER TABLE %s DROP CONSTRAINT %s";

        String dropConstraintQuery = String.format(
                dropConstraintFormat,
                alterTableName,
                constraintName
        );

        executeInStatement(dropConstraintQuery);
    }

    @Override
    public void addForeignKey(String constraintName, String foreignTable, Collection<String> localKeys, Collection<String> foreignKeys, ForeignKeyOptions foreignKeyOptions) {
        StringBuilder alterForeignKeyQueryBuilder = new StringBuilder();

        alterForeignKeyQueryBuilder.append(
                String.format(
                        "ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s)",
                        getAlterTableName(),
                        constraintName,
                        String.join(",", localKeys),
                        foreignTable,
                        String.join(",", foreignKeys)
                )
        );

        if (foreignKeyOptions.getOnDelete().isPresent()) {
            alterForeignKeyQueryBuilder.append(" ON DELETE " + getNativeCascadeType(foreignKeyOptions.getOnDelete().get()));
        }

        if (foreignKeyOptions.getOnUpdate().isPresent()) {
            alterForeignKeyQueryBuilder.append(" ON UPDATE " + getNativeCascadeType(foreignKeyOptions.getOnUpdate().get()));
        }

        executeInStatement(alterForeignKeyQueryBuilder.toString());
    }

    @Override
    public void addConstraint(String constraintName, Collection<String> columnNames, Constraint.TYPE type) {
        if (Constraint.TYPE.INDEX.equals(type)) {
            String addConstraint = String.format(
                    "CREATE INDEX %s ON  %s (%s)",
                    constraintName,
                    getAlterTableName(),
                    String.join(",", columnNames)
            );

            executeInStatement(addConstraint);
        } else {
            String addConstraint = String.format(
                    "ALTER TABLE %s ADD CONSTRAINT %s %s (%s)",
                    getAlterTableName(),
                    constraintName,
                    getNativeConstraintType(type),
                    String.join(",", columnNames)
            );

            executeInStatement(addConstraint);
        }
    }

    @Override
    public void rename(String rename) {
        executeInStatement(
                String.format(
                        "ALTER TABLE %s ALTER COLUMN %s RENAME TO %s",
                        getAlterTableName(),
                        getAlterColumnName(),
                        rename
                )
        );
    }

    @Override
    public boolean hasTable(String tableName) {
        try {
            DatabaseMetaData metaData = connection.getMetaData();
            ResultSet tables = metaData.getTables(null, null, "%", new String[]{"TABLE"});

            boolean tableExists = false;
            while (tables.next()) {
                if (tableName.equalsIgnoreCase(tables.getString("TABLE_NAME"))) {
                    tableExists = true;
                }
            }

            return tableExists;
        } catch (SQLException e) {
            throw new CouldNotProcessException(e);
        }
    }

    @Override
    public Connection getConnection() {
        return connection;
    }

    public DefaultDatabase(Connection connection) {
        this.connection = connection;
    }

    protected String getNativeCascadeType(ForeignKey.CASCADE cascade) {
        switch (cascade) {
            case RESTRICT:
            case SET_NULL:
            case SET_DEFAULT:
            case NO_ACTION:
            case CASCADE:
                return cascade.name().replace("_", " ");
        }
        throw new RuntimeException("Unknown type");
    }

    protected String getNativeConstraintType(Constraint.TYPE type) {
        switch (type) {
            case PRIMARY_KEY:
            case INDEX:
            case UNIQUE:
                return type.name().replaceAll("_", " ");
        }

        throw new RuntimeException("Could not process native constraint type");
    }

    protected String getNativeColumnDefinition(Column.TYPE columnType) {
        switch (columnType) {
            case SMALL_INTEGER:
                return "SMALLINT";
            case BIG_INTEGER:
                return "BIGINT";
        }
        return columnType.name();
    }

    protected String getNativeColumnDefinition(Column.TYPE columnType, ChangeTypeOptions changeTypeOptions) {
        if (!changeTypeOptions.getColumnSize().isPresent()) {
            return getNativeColumnDefinition(columnType);
        }

        return columnType.name() + "(" + changeTypeOptions.getColumnSize().get() + ")";
    }


    protected String getDefaultValue(Column.TYPE columnType, String defaultValue) {
        String quote = "";

        List<Column.TYPE> quotedTypes = Arrays.asList(
                Column.TYPE.CHAR,
                Column.TYPE.VARCHAR
        );
        if (quotedTypes.contains(columnType)) {
            quote = "'";
        }

        return "DEFAULT " + quote + defaultValue + quote;
    }

    protected void executeInStatement(String query) {
        executeInStatement(new String[]{query});
    }

    protected void executeInStatement(String[] queries) {
        Statement statement = null;
        try {
            statement = connection.createStatement();
            for (String query : queries) {
                statement.execute(query);
            }

        } catch (SQLException sqlException) {
            throw new CouldNotProcessException(sqlException);
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                }
            }
        }
    }
}
