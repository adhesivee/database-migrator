package nl.myndocs.database.migrator.profile;

import nl.myndocs.database.migrator.definition.Column;
import nl.myndocs.database.migrator.definition.ForeignKey;
import nl.myndocs.database.migrator.definition.Migration;
import nl.myndocs.database.migrator.definition.Table;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

/**
 * Created by albert on 15-8-2017.
 */
public abstract class BaseProfile implements Profile {
    private static final String CREATE_FOREIGN_KEY_FORMAT = "FOREIGN KEY (%s) REFERENCES %s (%s)";
    private static final String ALTER_TABLE_ALTER_DEFAULT = "ALTER TABLE %s ALTER COLUMN %s SET DEFAULT %s";

    public void createDatabase(Connection connection, Migration migration) {
        try {
            for (Table table : migration.getTables()) {
                Statement statement = connection.createStatement();
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

                    for (ForeignKey foreignKey : table.getNewForeignKeys()) {
                        createTableQueryBuilder.append(",\n ");

                        createTableQueryBuilder.append(
                                String.format(
                                        CREATE_FOREIGN_KEY_FORMAT,
                                        String.join(",", foreignKey.getLocalKeys()),
                                        foreignKey.getForeignTable(),
                                        String.join(",", foreignKey.getForeignKeys())
                                )
                        );

                        if (foreignKey.getDeleteCascade().isPresent()) {
                            createTableQueryBuilder.append(" ON DELETE " + getNativeCascadeType(foreignKey.getDeleteCascade().get()));
                        }

                        if (foreignKey.getUpdateCascade().isPresent()) {
                            createTableQueryBuilder.append(" ON UPDATE " + getNativeCascadeType(foreignKey.getUpdateCascade().get()));
                        }
                    }

                    createTableQueryBuilder.append(");");

                    System.out.println(createTableQueryBuilder.toString());

                    statement.execute(createTableQueryBuilder.toString());
                }

                statement.close();

                if (table.getChangeColumns().size() > 0) {

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

            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    protected abstract String getNativeColumnDefinition(Column column);

    protected String getAlterColumnKey() {
        return "ALTER";
    }

    protected String getAlterTypeKey() {
        return "";
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
        alterTableQueryBuilder.append(getAlterColumnKey() + " COLUMN " + column.getColumnName() + " " + getAlterTypeKey() + " " + getNativeColumnDefinition(column));

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
}
