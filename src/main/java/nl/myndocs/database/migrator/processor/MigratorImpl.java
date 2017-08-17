package nl.myndocs.database.migrator.processor;

import nl.myndocs.database.migrator.definition.*;
import nl.myndocs.database.migrator.engine.Engine;
import nl.myndocs.database.migrator.engine.exception.CouldNotProcessException;

import java.sql.*;
import java.util.Arrays;
import java.util.List;

/**
 * Created by albert on 15-8-2017.
 */
public class MigratorImpl implements Migrator {
    private static final String CREATE_FOREIGN_KEY_FORMAT = "FOREIGN KEY (%s) REFERENCES %s (%s)";
    private final Connection connection;
    private final Engine engine;

    public MigratorImpl(Connection connection, Engine engine) {
        this.connection = connection;
        this.engine = engine;
    }

    public void migrate(Migration migration) {
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
                        engine.changeColumnDefault(connection, table, column);
                    }
                }

                // Make sure renames always happens last
                // Otherwise changeColumnType and changeColumnDefault will break
                for (Column column : table.getChangeColumns()) {
                    if (column.getRename().isPresent()) {
                        engine.changeColumnName(connection, table, column);
                    }
                }


            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
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

    protected void changeColumnType(Connection connection, Table table, Column column) throws SQLException {
        Statement statement = connection.createStatement();

        String alterTableQuery = String.format(
                "ALTER TABLE %s %s COLUMN %s %s %s",
                table.getTableName(),
                engine.getAlterColumnTerm(),
                column.getColumnName(),
                engine.getAlterTypeTerm(),
                engine.getNativeColumnDefinition(column)
        );
        System.out.println(alterTableQuery);
        statement.execute(alterTableQuery);

        statement.close();
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
                                engine.getNativeColumnDefinition(column) + " " +
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
                            engine.getNativeColumnDefinition(column) + " " +
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
    }

    private void processDropColumns(Table table) {
        for (String dropColumn : table.getDropColumns()) {
            String dropColumnQuery = String.format(
                    "ALTER TABLE %s DROP COLUMN %s ",
                    table.getTableName(),
                    dropColumn
            );

            try {
                executeInStatement(dropColumnQuery);
            } catch (SQLException e) {
                throw new CouldNotProcessException(e);
            }
        }
    }

    private void processDropForeignKeys(Table table) {
        for (String constraint : table.getDropForeignKeys()) {
            String dropConstraintQuery = String.format(
                    "ALTER TABLE %s DROP %s %s",
                    table.getTableName(),
                    engine.getDropForeignKeyTerm(),
                    constraint
            );

            try {
                executeInStatement(dropConstraintQuery);
            } catch (SQLException e) {
                throw new CouldNotProcessException(e);
            }
        }

    }

    private void processAddingConstraints(Table table) {
        for (Constraint constraint : table.getNewConstraints()) {
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
    }

    private void processDropConstraints(Table table) {
        for (String constraintName : table.getDropConstraints()) {
            String alterForeignKeyQuery = String.format(
                    "ALTER TABLE %s DROP %s %s",
                    table.getTableName(),
                    engine.getDropConstraintTerm(),
                    constraintName
            );
            try {
                executeInStatement(alterForeignKeyQuery);
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

    private String getNativeConstraintType(Constraint.TYPE type) {
        switch (type) {
            case INDEX:
                return "INDEX";
            case UNIQUE:
                return "UNIQUE";
        }

        throw new RuntimeException("Could not process native constraint type");
    }
}
