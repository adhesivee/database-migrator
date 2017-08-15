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
                                        (column.isNotNull() != null && column.isNotNull() ? "NOT NULL" : "") + " " +
                                        (column.isPrimary() != null && column.isPrimary() ? "PRIMARY KEY" : "") + " "
                        );

                        count++;
                    }

                    for (ForeignKey foreignKey : table.getForeignKeys()) {
                        createTableQueryBuilder.append(",\n");

                        createTableQueryBuilder.append(" FOREIGN KEY (");
                        createTableQueryBuilder.append(String.join(",", foreignKey.getLocalKeys()));
                        createTableQueryBuilder.append(") REFERENCES " + foreignKey.getForeignTable() + " (");
                        createTableQueryBuilder.append(String.join(",", foreignKey.getForeignKeys()));
                        createTableQueryBuilder.append(")");

                        if (foreignKey.getDeleteCascade() != null) {
                            createTableQueryBuilder.append(" ON DELETE " + getNativeCascadeType(foreignKey.getDeleteCascade()));
                        }

                        if (foreignKey.getUpdateCascade() != null) {
                            createTableQueryBuilder.append(" ON UPDATE " + getNativeCascadeType(foreignKey.getUpdateCascade()));
                        }
                    }

                    createTableQueryBuilder.append(");");

                    System.out.println(createTableQueryBuilder.toString());

                    statement.execute(createTableQueryBuilder.toString());
                }

                if (table.getChangeColumns().size() > 0) {
                    StringBuilder alterTableQueryBuilder = new StringBuilder("ALTER TABLE " + table.getTableName() + " ");

                    for (Column column : table.getChangeColumns()) {
                        if (column.getType() != null) {
                            alterTableQueryBuilder.append(getAlterColumnKey() + " COLUMN " + column.getColumnName() + " " + getAlterType() + " " + getNativeColumnDefinition(column));
                        }
                    }

                    System.out.println(alterTableQueryBuilder.toString());
                    statement.execute(alterTableQueryBuilder.toString());
                }

                statement.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    protected abstract String getNativeColumnDefinition(Column column);

    protected String getAlterColumnKey() {
        return "ALTER";
    }

    protected String getAlterType() {
        return "";
    }
    protected String getDefaultValue(Column column) {
        String quote = "";

        List<Column.TYPE> quotedTypes = Arrays.asList(
                Column.TYPE.CHAR,
                Column.TYPE.VARCHAR
        );
        if (quotedTypes.contains(column.getType())) {
            quote = "'";
        }

        return (column.getDefaultValue() != null ? "DEFAULT " + quote + column.getDefaultValue() + quote + "" : "");
    }
    protected String getWithSizeIfPossible(Column column) {
        if (column.getSize() != null) {
            return "(" + column.getSize() + ")";
        }

        return "";
    }

    protected String getWithSizeOrDefault(Column column, String defaultValue) {
        if (column.getSize() != null) {
            return "(" + column.getSize() + ")";
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
