package nl.myndocs.database.migrator.engine;

import nl.myndocs.database.migrator.definition.Column;
import nl.myndocs.database.migrator.definition.Constraint;
import nl.myndocs.database.migrator.definition.ForeignKey;
import nl.myndocs.database.migrator.definition.Table;
import nl.myndocs.database.migrator.engine.exception.CouldNotProcessException;
import nl.myndocs.database.migrator.validator.TableValidator;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

/**
 * Created by albert on 17-8-2017.
 */
public abstract class BaseEngine implements Engine {

    private static final String ALTER_TABLE_ALTER_DEFAULT = "ALTER TABLE %s ALTER COLUMN %s SET DEFAULT %s";

    private final Connection connection;

    public BaseEngine(Connection connection) {
        this.connection = connection;
    }

    protected String getWithSizeIfPossible(Column column) {
        if (column.getSize().isPresent()) {
            return "(" + column.getSize().get() + ")";
        }

        return "";
    }

    protected String getWithSizeOrDefault(Column column, String defaultValue) {
        if (column.getSize().isPresent()) {
            return "(" + column.getSize().get() + ")";
        }

        return "(" + defaultValue + ")";
    }

    @Override
    public void alterColumnDefault(Table table, Column column) {
        try {
            executeInStatement(
                    String.format(
                            ALTER_TABLE_ALTER_DEFAULT,
                            table.getTableName(),
                            column.getColumnName(),
                            "'" + column.getDefaultValue().get() + "'"

                    )
            );
        } catch (SQLException e) {
            throw new CouldNotProcessException(e);
        }
    }

    @Override
    public void alterColumnType(Table table, Column column) {
        String alterTableQuery = String.format(
                "ALTER TABLE %s %s COLUMN %s %s %s",
                table.getTableName(),
                getAlterColumnTerm(),
                column.getColumnName(),
                getAlterTypeTerm(),
                getNativeColumnDefinition(column)
        );
        System.out.println(alterTableQuery);

        try {
            executeInStatement(alterTableQuery);
        } catch (SQLException e) {
            throw new CouldNotProcessException(e);
        }
    }

    public String getDropForeignKeyTerm() {
        return "CONSTRAINT";
    }

    public String getAlterColumnTerm() {
        return "ALTER";
    }

    public String getAlterTypeTerm() {
        return "";
    }

    public String getDropConstraintTerm() {
        return "CONSTRAINT";
    }

    protected String getNativeConstraintType(Constraint.TYPE type) {
        switch (type) {
            case INDEX:
                return "INDEX";
            case UNIQUE:
                return "UNIQUE";
        }

        throw new RuntimeException("Could not process native constraint type");
    }

    protected void executeInStatement(String query) throws SQLException {
        Statement statement = connection.createStatement();
        statement.execute(query);
        statement.close();
    }

    @Override
    public void addColumnsWithCreateTable(Table table) {
        if (table.getNewColumns().size() > 0) {
            StringBuilder createTableQueryBuilder = new StringBuilder("CREATE TABLE " + table.getTableName() + " (\n");

            int count = 0;
            for (Column column : table.getNewColumns()) {
                if (count > 0) {
                    createTableQueryBuilder.append(",\n");
                }

                createTableQueryBuilder.append(
                        column.getColumnName() + " " +
                                getNativeColumnDefinition(column) + " " +
                                getDefaultValue(column) + " " +
                                (column.getIsNotNull().orElse(false) ? "NOT NULL" : "") + " " +
                                (column.getPrimary().orElse(false) ? "PRIMARY KEY" : "") + " "
                );

                count++;
            }


            createTableQueryBuilder.append(")");

            System.out.println(createTableQueryBuilder.toString());

            try {
                executeInStatement(createTableQueryBuilder.toString());
            } catch (SQLException e) {
                throw new CouldNotProcessException(e);
            }
        }
    }

    @Override
    public void addColumnsWithAlterTable(Table table) {
        for (Column column : table.getNewColumns()) {
            StringBuilder addColumnQueryBuilder = new StringBuilder();

            addColumnQueryBuilder.append(
                    "ALTER TABLE " + table.getTableName() + " " +
                            "ADD COLUMN " +
                            column.getColumnName() + " " +
                            getNativeColumnDefinition(column) + " " +
                            getDefaultValue(column) + " " +
                            (column.getIsNotNull().orElse(false) ? "NOT NULL" : "") + " " +
                            (column.getPrimary().orElse(false) ? "PRIMARY KEY" : "") + " "
            );

            try {
                executeInStatement(addColumnQueryBuilder.toString());
            } catch (SQLException e) {
                throw new CouldNotProcessException(e);
            }
        }
    }

    @Override
    public void addForeignKey(Table table, ForeignKey foreignKey) {
        StringBuilder alterForeignKeyQueryBuilder = new StringBuilder();

        alterForeignKeyQueryBuilder.append(
                String.format(
                        "ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s)",
                        table.getTableName(),
                        foreignKey.getConstraintName(),
                        String.join(",", foreignKey.getLocalKeys()),
                        foreignKey.getForeignTable(),
                        String.join(",", foreignKey.getForeignKeys())
                )
        );

        if (foreignKey.getDeleteCascade().isPresent()) {
            alterForeignKeyQueryBuilder.append(" ON DELETE " + getNativeCascadeType(foreignKey.getDeleteCascade().get()));
        }

        if (foreignKey.getUpdateCascade().isPresent()) {
            alterForeignKeyQueryBuilder.append(" ON UPDATE " + getNativeCascadeType(foreignKey.getUpdateCascade().get()));
        }

        try {
            executeInStatement(alterForeignKeyQueryBuilder.toString());
        } catch (SQLException e) {
            throw new CouldNotProcessException(e);
        }
    }

    @Override
    public void dropColumn(Table table, String columnName) {
        String dropColumnQuery = String.format(
                "ALTER TABLE %s DROP COLUMN %s ",
                table.getTableName(),
                columnName
        );

        try {
            executeInStatement(dropColumnQuery);
        } catch (SQLException e) {
            throw new CouldNotProcessException(e);
        }
    }

    @Override
    public void dropForeignKey(Table table, String constraintName) {
        String dropConstraintQuery = String.format(
                "ALTER TABLE %s DROP %s %s",
                table.getTableName(),
                getDropForeignKeyTerm(),
                constraintName
        );

        try {
            executeInStatement(dropConstraintQuery);
        } catch (SQLException e) {
            throw new CouldNotProcessException(e);
        }
    }

    @Override
    public void addConstraint(Table table, Constraint constraint) {
        StringBuilder alterForeignKeyQueryBuilder = new StringBuilder();

        alterForeignKeyQueryBuilder.append(
                String.format(
                        "ALTER TABLE %s ADD CONSTRAINT %s %s (%s)",
                        table.getTableName(),
                        constraint.getConstraintName(),
                        getNativeConstraintType(constraint.getType().get()),
                        String.join(",", constraint.getColumnNames())
                )
        );

        try {
            executeInStatement(alterForeignKeyQueryBuilder.toString());
        } catch (SQLException e) {
            throw new CouldNotProcessException(e);
        }
    }

    @Override
    public void dropConstraint(Table table, String constraintName) {
        String alterForeignKeyQuery = String.format(
                "ALTER TABLE %s DROP %s %s",
                table.getTableName(),
                getDropConstraintTerm(),
                constraintName
        );

        try {
            executeInStatement(alterForeignKeyQuery);
        } catch (SQLException e) {
            throw new CouldNotProcessException(e);
        }
    }

    protected String getNativeCascadeType(ForeignKey.CASCADE cascade) {
        switch (cascade) {
            case RESTRICT:
                return "RESTRICT";
            case SET_NULL:
                return "SET NULL";
            case SET_DEFAULT:
                return "SET DEFAULT";
            case NO_ACTION:
                return "NO ACTION";
            case CASCADE:
                return "CASCADE";
        }
        throw new RuntimeException("Unknown type");
    }

    protected String getDefaultValue(Column column) {
        String quote = "";

        List<Column.TYPE> quotedTypes = Arrays.asList(
                Column.TYPE.CHAR,
                Column.TYPE.VARCHAR
        );
        if (quotedTypes.contains(column.getType().get())) {
            quote = "'";
        }

        return (column.getDefaultValue().isPresent() ? "DEFAULT " + quote + column.getDefaultValue().get() + quote + "" : "");
    }

    protected abstract String getNativeColumnDefinition(Column column);

    @Override
    public TableValidator getTableValidator() {
        return new TableValidator(connection);
    }
}
