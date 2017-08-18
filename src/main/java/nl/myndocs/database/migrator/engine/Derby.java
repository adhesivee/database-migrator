package nl.myndocs.database.migrator.engine;

import nl.myndocs.database.migrator.definition.Column;
import nl.myndocs.database.migrator.definition.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class Derby extends BaseEngine {
    private static final Logger logger = LoggerFactory.getLogger(Derby.class);

    @Override
    public void changeColumnName(Connection connection, Table table, Column column) throws SQLException {
        Statement statement = connection.createStatement();
        statement.execute(
                String.format(
                        "RENAME COLUMN %1$s.%2$s TO %3$s",
                        table.getTableName(),
                        column.getColumnName(),
                        column.getRename().get()
                )
        );
        statement.close();
    }

    @Override
    public void changeColumnType(Connection connection, Table table, Column column) throws SQLException {
        String[] alters = new String[] {
                "ALTER TABLE %1$s ADD COLUMN %2$s_newtype %3$s",
                "UPDATE %1$s SET %2$s_newtype = %2$s",
                "ALTER TABLE %1$s DROP COLUMN %2$s",
                "RENAME COLUMN %1$s.%2$s_newtype TO %2$s"
        };

        for (String alter : alters) {
            Statement statement = connection.createStatement();
            System.out.println(
                    String.format(
                            alter,
                            table.getTableName(),
                            column.getColumnName(),
                            getNativeColumnDefinition(column)
                    )
            );
            statement.execute(
                    String.format(
                            alter,
                            table.getTableName(),
                            column.getColumnName(),
                            getNativeColumnDefinition(column)
                    )
            );
            statement.close();
        }
    }

    @Override
    public String getAlterTypeTerm() {
        return "SET DATA TYPE";
    }

    @Override
    public String getNativeColumnDefinition(Column column) {
        Column.TYPE columnType = column.getType().get();
        switch (columnType) {
            case INTEGER:
                return "INTEGER " + (column.getAutoIncrement().orElse(false) ? "GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1)" : "");
            case VARCHAR:
                return "VARCHAR" + getWithSizeOrDefault(column, "255");
            case CHAR:
                return "CHAR" + getWithSizeOrDefault(column, "254");
            case UUID:
                logger.warn("UUID not supported, creating CHAR(36) instead");
                return "CHAR " + getWithSizeOrDefault(column, "36");
        }

        throw new RuntimeException("Unknown type");
    }
}
