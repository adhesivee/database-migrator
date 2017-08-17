package nl.myndocs.database.migrator.engine;

import nl.myndocs.database.migrator.definition.Column;
import nl.myndocs.database.migrator.definition.Table;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by albert on 17-8-2017.
 */
public abstract class BaseEngine implements Engine {

    private static final String ALTER_TABLE_ALTER_DEFAULT = "ALTER TABLE %s ALTER COLUMN %s SET DEFAULT %s";

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

    // @TODO: This should not be here, should be in the processor
    @Override
    public void changeColumnDefault(Connection connection, Table table, Column column) throws SQLException {
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

    @Override
    public String getDropForeignKeyTerm() {
        return "CONSTRAINT";
    }

    @Override
    public String getAlterColumnTerm() {
        return "ALTER";
    }

    @Override
    public String getAlterTypeTerm() {
        return "";
    }

    @Override
    public String getDropConstraintTerm() {
        return "CONSTRAINT";
    }
}
