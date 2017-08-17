package nl.myndocs.database.migrator.profile;

import nl.myndocs.database.migrator.definition.*;
import nl.myndocs.database.migrator.profile.exception.CouldNotProcessException;

import java.sql.*;
import java.util.Arrays;
import java.util.List;

/**
 * Created by albert on 15-8-2017.
 */
public abstract class BaseProfile implements Profile {
    private static final String CREATE_FOREIGN_KEY_FORMAT = "FOREIGN KEY (%s) REFERENCES %s (%s)";
    private static final String ALTER_TABLE_ALTER_DEFAULT = "ALTER TABLE %s ALTER COLUMN %s SET DEFAULT %s";
    private final Connection connection;

    public BaseProfile(Connection connection) {
        this.connection = connection;
    }

    public void createDatabase(Migration migration) {
        try {
            for (Table table : migration.getTables()) {
                DatabaseMetaData metaData = connection.getMetaData();
                ResultSet tables = metaData.getTables(null, null, "%", new String[]{"TABLE"});

                boolean tableExists = false;
                while (tables.next()) {
                    if (table.getTableName().equalsIgnoreCase(tables.getString("TABLE_NAME"))) {
                        tableExists = true;
                    }
                }

                Statement statement = connection.createStatement();

                if (tableExists) {
                    processAddingColumnsWithAlter(table);
                } else {
                    processAddingColumnsWithCreate(table);
                }


                processDropColumns(table);
                processDropForeignKeys(table);
                processAddingConstraints(table);
                processDropConstraints(table);
                processAddingForeignKeys(table);

                statement.close();

                for (Column column : table.getChangeColumns()) {
                    if (column.getType().isPresent()) {
                        changeColumnType(connection, table, column);
                    }

                    if (column.getDefaultValue().isPresent()) {
                        changeColumnDefault(connection, table, column);
                    }
                }

                // Make sure renames always happens last
                // Otherwise changeColumnType and changeColumnDefault will break
                for (Column column : table.getChangeColumns()) {
                    if (column.getRename().isPresent()) {
                        changeColumnName(connection, table, column);
                    }
                }


            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    protected abstract String getNativeColumnDefinition(Column column);

    protected String getDropForeignKeyTerm() {
        return "CONSTRAINT";
    }

    protected String getAlterColumnTerm() {
        return "ALTER";
    }

    protected String getAlterTypeTerm() {
        return "";
    }

    protected String getDropConstraintTerm() {
        return "CONSTRAINT";
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

    protected String getWithSizeIfPossible(Column column) {
        if (column.getSize().isPresent()) {
            return "(" + column.getSize().get() + ")";
        }

        return "";
    }

    // @TODO: This should be solved more generic
    protected abstract void changeColumnName(Connection connection, Table table, Column column) throws SQLException;

    protected void changeColumnDefault(Connection connection, Table table, Column column) throws SQLException {
        Statement statement = connection.createStatement();
        statement.execute(
                String.format(
                        ALTER_TABLE_ALTER_DEFAULT,
                        table.getTableName(),
                        column.getColumnName(),
                        "'" + column.getDefaultValue().get() + "'"

                )
        );
        statement.close();
    }

    protected void changeColumnType(Connection connection, Table table, Column column) throws SQLException {
        Statement statement = connection.createStatement();
        StringBuilder alterTableQueryBuilder = new StringBuilder("ALTER TABLE " + table.getTableName() + " ");
        alterTableQueryBuilder.append(getAlterColumnTerm() + " COLUMN " + column.getColumnName() + " " + getAlterTypeTerm() + " " + getNativeColumnDefinition(column));

        System.out.println(alterTableQueryBuilder.toString());
        statement.execute(alterTableQueryBuilder.toString());

        statement.close();
    }

    protected String getWithSizeOrDefault(Column column, String defaultValue) {
        if (column.getSize().isPresent()) {
            return "(" + column.getSize().get() + ")";
        }

        return "(" + defaultValue + ")";
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

    private void processAddingColumnsWithCreate(Table table) {
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


            createTableQueryBuilder.append(");");

            System.out.println(createTableQueryBuilder.toString());

            try {
                executeInStatement(createTableQueryBuilder.toString());
            } catch (SQLException e) {
                throw new CouldNotProcessException(e);
            }
        }
    }

    private void processAddingColumnsWithAlter(Table table) {
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

    private void processAddingForeignKeys(Table table) {
        for (ForeignKey foreignKey : table.getNewForeignKeys()) {
            StringBuilder alterForeignKeyQueryBuilder = new StringBuilder("ALTER TABLE " + table.getTableName());
            alterForeignKeyQueryBuilder.append(" ADD CONSTRAINT " + foreignKey.getConstraintName());

            alterForeignKeyQueryBuilder.append(
                    String.format(
                            " FOREIGN KEY (%s) REFERENCES %s (%s)",
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
    }

    private void processDropColumns(Table table) {
        for (String dropColumn : table.getDropColumns()) {
            String dropColumnQuery = "ALTER TABLE " + table.getTableName() + " " +
                    "DROP COLUMN " +
                    dropColumn;

            try {
                executeInStatement(dropColumnQuery);
            } catch (SQLException e) {
                throw new CouldNotProcessException(e);
            }
        }
    }

    private void processDropForeignKeys(Table table) {
        for (String constraint : table.getDropForeignKeys()) {
            String dropConstraintQuery = "ALTER TABLE " + table.getTableName() + " " +
                    "DROP " + getDropForeignKeyTerm() + " " +
                    constraint;

            try {
                executeInStatement(dropConstraintQuery);
            } catch (SQLException e) {
                throw new CouldNotProcessException(e);
            }
        }

    }

    private void processAddingConstraints(Table table) {
        for (Constraint constraint : table.getNewConstraints()) {
            StringBuilder alterForeignKeyQueryBuilder = new StringBuilder("ALTER TABLE " + table.getTableName());
            alterForeignKeyQueryBuilder.append(" ADD CONSTRAINT " + constraint.getConstraintName());

            alterForeignKeyQueryBuilder.append(
                    String.format(
                            " %s (%s)",
                            (constraint.getType().get().equals(Constraint.TYPE.UNIQUE) ? "UNIQUE" : "INDEX"),
                            String.join(",", constraint.getColumnNames())
                    )
            );

            try {
                executeInStatement(alterForeignKeyQueryBuilder.toString());
            } catch (SQLException e) {
                throw new CouldNotProcessException(e);
            }
        }
    }

    private void processDropConstraints(Table table) {
        for (String constraintName : table.getDropConstraints()) {
            StringBuilder alterForeignKeyQueryBuilder = new StringBuilder("ALTER TABLE " + table.getTableName());
            alterForeignKeyQueryBuilder.append(" DROP " + getDropConstraintTerm() + " " + constraintName);

            try {
                executeInStatement(alterForeignKeyQueryBuilder.toString());
            } catch (SQLException e) {
                throw new CouldNotProcessException(e);
            }
        }
    }

    private void executeInStatement(String query) throws SQLException {
        Statement statement = connection.createStatement();
        statement.execute(query);
        statement.close();
    }
}
