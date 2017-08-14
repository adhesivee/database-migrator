package nl.myndocs.database.migrator.profile;

import nl.myndocs.database.migrator.definition.Column;
import nl.myndocs.database.migrator.definition.ForeignKey;
import nl.myndocs.database.migrator.definition.Migration;
import nl.myndocs.database.migrator.definition.Table;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by albert on 15-8-2017.
 */
public abstract class BaseProfile implements Profile {
    public void createDatabase(Connection connection, Migration migration) {
        try {
            for (Table table : migration.getNewTables()) {
                Statement statement = connection.createStatement();
                StringBuilder stringBuilder = new StringBuilder("CREATE TABLE " + table.getTableName() + " (\n");

                int count = 0;
                for (Column column : table.getNewColumns()) {
                    if (count > 0) {
                        stringBuilder.append(",\n");
                    }

                    stringBuilder.append(
                            column.getColumnName() + " " +
                                    getNativeColumnDefinition(column) + " " +
                                    // @TODO: don't assume it is always a String / (VAR)CHAR value
                                    (column.getDefaultValue() != null ? "DEFAULT '" + column.getDefaultValue() + "'" : "") + " " +
                                    (column.isNotNull() ? "NOT NULL" : "") + " " +
                                    (column.isPrimary() ? "PRIMARY KEY" : "") + " "
                    );

                    count++;
                }

                for (ForeignKey foreignKey : table.getForeignKeys()) {
                    stringBuilder.append(",\n");

                    stringBuilder.append(" FOREIGN KEY (");
                    stringBuilder.append(String.join(",", foreignKey.getLocalKeys()));
                    stringBuilder.append(") REFERENCES " + foreignKey.getForeignTable() + " (");
                    stringBuilder.append(String.join(",", foreignKey.getForeignKeys()));
                    stringBuilder.append(")");

                    if (foreignKey.getDeleteCascade() != null) {
                        stringBuilder.append(" ON DELETE " + getNativeCascadeType(foreignKey.getDeleteCascade()));
                    }

                    if (foreignKey.getUpdateCascade() != null) {
                        stringBuilder.append(" ON UPDATE " + getNativeCascadeType(foreignKey.getUpdateCascade()));
                    }
                }

                stringBuilder.append(");");

                System.out.println(stringBuilder.toString());

                statement.execute(stringBuilder.toString());

                statement.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    protected abstract String getNativeColumnDefinition(Column column);

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
